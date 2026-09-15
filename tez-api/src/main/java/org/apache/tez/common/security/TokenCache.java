/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common.security;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;


/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.  
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class TokenCache {
  
  private static final Logger LOG = LoggerFactory.getLogger(TokenCache.class);

  private TokenCache() {}

  
  /**
   * auxiliary method to get user's secret keys..
   *
   * @return secret key from the storage
   */
  public static byte[] getSecretKey(Credentials credentials, Text alias) {
    if(credentials == null)
      return null;
    return credentials.getSecretKey(alias);
  }
  
  /**
   * Convenience method to obtain delegation tokens from namenodes 
   * corresponding to the paths passed.
   * @param credentials credentials
   * @param ps array of paths
   * @param conf configuration
   */
  public static void obtainTokensForFileSystems(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForFileSystemsInternal(credentials, ps, conf);
  }

  private static final int MAX_FS_OBJECTS = 10;
  static void obtainTokensForFileSystemsInternal(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    Set<FileSystem> fsSet = new HashSet<>();
    boolean limitExceeded = false;
    for(Path p: ps) {
      FileSystem fs = p.getFileSystem(conf);
      if (!limitExceeded && fsSet.size() == MAX_FS_OBJECTS) {
        LOG.warn("No of FileSystem objects exceeds {}, updating tokens for all paths. This can" +
            " happen when fs.<scheme>.impl.disable.cache is set to true.", MAX_FS_OBJECTS);
        limitExceeded = true;
      }
      if (limitExceeded) {
        // Too many fs objects are being created, most likely the cache is disabled. Prevent an
        // OOM and just directly invoke instead of adding to the set.
        obtainTokensForFileSystemsInternal(fs, credentials, conf);
      } else {
        fsSet.add(fs);
      }
    }
    for (FileSystem fs : fsSet) {
      obtainTokensForFileSystemsInternal(fs, credentials, conf);
    }
  }

  /**
   * Obtain delegation tokens from the credential providers of
   * {@code hadoop.security.credential.provider.path} that issue them, so tasks can read
   * credentials without Kerberos credentials of their own. Providers that cannot be created
   * or refuse a token are logged and skipped: the DAG may not need them. Providers listed in
   * {@link TezConfiguration#TEZ_JOB_CREDENTIAL_PROVIDERS_TOKEN_RENEWAL_EXCLUDE} get a token
   * with no renewer, which the RM leaves alone.
   *
   * @param credentials the credentials to add the tokens to
   * @param conf configuration naming the providers
   */
  public static void obtainTokensForCredentialProviders(Credentials credentials,
      Configuration conf) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForCredentialProvidersInternal(credentials, conf);
  }

  static void obtainTokensForCredentialProvidersInternal(Credentials credentials,
      Configuration conf) throws IOException {
    List<String> renewable = new ArrayList<>();
    List<String> excluded = new ArrayList<>();
    for (String path : conf.getStringCollection(
        CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH)) {
      String providerPath = path.trim();
      if (providerPath.isEmpty()) {
        continue;
      }
      if (isProviderTokenRenewalExcluded(providerPath, conf)) {
        excluded.add(providerPath);
      } else {
        renewable.add(providerPath);
      }
    }
    if (renewable.isEmpty() && excluded.isEmpty()) {
      return;
    }

    // RM skips renewing token with empty renewer
    int obtained = addCredentialProviderTokens(credentials, conf, excluded, "");
    if (!renewable.isEmpty()) {
      String delegTokenRenewer = getDelegationTokenRenewer(conf);
      if (delegTokenRenewer == null) {
        LOG.warn("Not obtaining delegation tokens from credential providers {}: {} is not set,"
            + " so there is no principal to use as renewer", renewable,
            YarnConfiguration.RM_PRINCIPAL);
      } else {
        obtained += addCredentialProviderTokens(credentials, conf, renewable, delegTokenRenewer);
      }
    }
    if (obtained == 0) {
      LOG.warn("Obtained no delegation token from credential providers {}; tasks relying on them"
          + " will have no credentials of their own", conf.get(
              CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH));
    }
  }

  /**
   * get delegation tokens from the credential providers of the given paths
   * @return the number of tokens obtained
   */
  private static int addCredentialProviderTokens(Credentials credentials, Configuration conf,
      List<String> providerPaths, String delegTokenRenewer) {
    if (providerPaths.isEmpty()) {
      return 0;
    }
    Configuration providerConf = new Configuration(conf);
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        String.join(",", providerPaths));
    List<Token<?>> tokens = CredentialProviderFactory.addDelegationTokens(providerConf,
        delegTokenRenewer, credentials);
    for (Token<?> token : tokens) {
      LOG.info("Got dt for {}; {}", token.getService(), token);
    }
    return tokens.size();
  }

  private static boolean isProviderTokenRenewalExcluded(String providerPath, Configuration conf) {
    String[] hosts = conf.getStrings(
        TezConfiguration.TEZ_JOB_CREDENTIAL_PROVIDERS_TOKEN_RENEWAL_EXCLUDE);
    if (hosts == null) {
      return false;
    }
    String host;
    try {
      host = new URI(providerPath).getHost();
    } catch (URISyntaxException e) {
      return false;
    }
    for (String excluded : hosts) {
      if (excluded.equals(host)) {
        return true;
      }
    }
    return false;
  }

  static boolean isTokenRenewalExcluded(FileSystem fs, Configuration conf) {
    String[] nns =
            conf.getStrings(TezConfiguration.TEZ_JOB_FS_SERVERS_TOKEN_RENEWAL_EXCLUDE);
    if (nns != null) {
      String host = fs.getUri().getHost();
      for (String nn : nns) {
        if (nn.equals(host)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * get delegation token for a specific FS
   */
  static void obtainTokensForFileSystemsInternal(FileSystem fs, 
      Credentials credentials, Configuration conf) throws IOException {
    // TODO Change this to use YARN utilities once YARN-1664 is fixed.
    // RM skips renewing token with empty renewer
    String delegTokenRenewer = "";
    if (!isTokenRenewalExcluded(fs, conf)) {
      delegTokenRenewer = getDelegationTokenRenewer(conf);
      if (delegTokenRenewer == null) {
        throw new IOException(
                "Can't get Master Kerberos principal for use as renewer");
      }
    }

    final Token<?>[] tokens = fs.addDelegationTokens(delegTokenRenewer,
                                                     credentials);
    if (tokens != null) {
      for (Token<?> token : tokens) {
        LOG.info("Got dt for " + fs.getUri() + "; "+token);
      }
    }
  }

  /**
   * @return the principal to record as delegation token renewer, or null if the master
   *     principal is not configured
   */
  private static String getDelegationTokenRenewer(Configuration conf) throws IOException {
    String delegTokenRenewer = Master.getMasterPrincipal(conf);
    return delegTokenRenewer == null || delegTokenRenewer.isEmpty() ? null : delegTokenRenewer;
  }

  private static final Text SESSION_TOKEN = new Text("SessionToken");

  /**
   * store session specific token
   */
  @InterfaceAudience.Private
  public static void setSessionToken(Token<? extends TokenIdentifier> t, 
      Credentials credentials) {
    credentials.addToken(SESSION_TOKEN, t);
  }
  /**
   * 
   * @return session token
   */
  @SuppressWarnings("unchecked")
  @InterfaceAudience.Private
  public static Token<JobTokenIdentifier> getSessionToken(Credentials credentials) {
    Token<?> token = credentials.getToken(SESSION_TOKEN);
    if (token == null) {
      return null;
    }
    return (Token<JobTokenIdentifier>) token;
  }

  /**
   * Merge tokens from a configured binary file into provided Credentials object
   * @param creds Credentials object to add new tokens to
   * @param tokenFilePath Location of tokens' binary file
   */
  @InterfaceAudience.Private
  public static void mergeBinaryTokens(Credentials creds,
      Configuration conf, String tokenFilePath)
      throws IOException {
    if (tokenFilePath == null || tokenFilePath.isEmpty()) {
      throw new RuntimeException("Invalid file path provided"
          + ", tokenFilePath=" + tokenFilePath);
    }
    LOG.info("Merging additional tokens from binary file"
        + ", binaryFileName=" + tokenFilePath);
    Credentials binary = Credentials.readTokenStorageFile(
        new Path("file:///" +  tokenFilePath), conf);

    // supplement existing tokens with the tokens in the binary file
    creds.mergeAll(binary);
  }

}
