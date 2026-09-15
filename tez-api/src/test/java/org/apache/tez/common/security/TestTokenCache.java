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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.TokenIssuingCredentialProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestTokenCache {

  private static Configuration conf;

  private static String renewer;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
    conf.setSocketAddr(YarnConfiguration.RM_ADDRESS,
        InetSocketAddress.createUnresolved("127.0.0.1", 8032));
    renewer = Master.getMasterPrincipal(conf);
  }

  @Test(timeout = 5000)
  @SuppressWarnings("deprecation")
  public void testBinaryCredentials() throws Exception {
    String binaryTokenFile = null;
    try {
      Path TEST_ROOT_DIR = new Path("target");
      binaryTokenFile = FileSystem.getLocal(conf).makeQualified(
        new Path(TEST_ROOT_DIR, "tokenFile")).toUri().getPath();

      MockFileSystem fs1 = createFileSystemForServiceName("service1");
      MockFileSystem fs2 = createFileSystemForServiceName("service2");

      // get the tokens for fs1 & fs2 and write out to binary creds file
      Credentials creds = new Credentials();
      Token<?> token1 = fs1.getDelegationToken(renewer);
      Token<?> token2 = fs2.getDelegationToken(renewer);
      creds.addToken(token1.getService(), token1);
      creds.addToken(token2.getService(), token2);
      creds.writeTokenStorageFile(new Path(binaryTokenFile), conf);


      Credentials newCreds = new Credentials();
      TokenCache.mergeBinaryTokens(newCreds, conf, binaryTokenFile);

      Assert.assertTrue(newCreds.getAllTokens().size() > 0);
      checkTokens(creds, newCreds);
    } finally {
      if (binaryTokenFile != null) {
        try {
          FileSystem.getLocal(conf).delete(new Path(binaryTokenFile));
        } catch (IOException e) {
          // Ignore
        }
      }
    }
  }

  @Test(timeout=5000)
  public void testObtainTokensForFileSystems() throws Exception {
    Path[] paths = makePaths(100, "test://dir/file");
    Credentials creds = new Credentials();
    Configuration conf = new Configuration(TestTokenCache.conf);
    conf.set("fs.test.impl", TestFileSystem.class.getName());

    // Cache enabled should be invoked only once
    conf.setBoolean("fs.test.impl.disable.cache", false);
    TokenCache.obtainTokensForFileSystemsInternal(creds, paths, conf);
    verify(TestFileSystem.fs, times(1)).addDelegationTokens(renewer, creds);

    // Cache disabled should be invoked for every path.
    conf.setBoolean("fs.test.impl.disable.cache", true);
    TokenCache.obtainTokensForFileSystemsInternal(creds, paths, conf);
    verify(TestFileSystem.fs, times(paths.length + 1)).addDelegationTokens(renewer, creds);

    // Excluded filesystem tokens should not be obtained.
    conf.set("tez.job.fs-servers.token-renewal.exclude", "dir");
    TokenCache.obtainTokensForFileSystemsInternal(creds, paths, conf);
    verify(TestFileSystem.fs, times(paths.length + 1)).addDelegationTokens(renewer, creds);
  }

  @Test(timeout=5000)
  public void testObtainTokensForCredentialProviders() throws Exception {
    TokenIssuingCredentialProvider.ISSUED.set(0);
    Configuration conf = new Configuration(TestTokenCache.conf);
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        "user:///,dtissuer://vault-a/,dtissuer://broken/");
    Credentials creds = new Credentials();

    TokenCache.obtainTokensForCredentialProvidersInternal(creds, conf);

    Assert.assertEquals(renewer, identifierOf(creds, "dtissuer://vault-a/"));
    Assert.assertEquals(1, creds.numberOfTokens());

    // A provider that already has a token in the credentials must not issue another one.
    TokenCache.obtainTokensForCredentialProvidersInternal(creds, conf);
    Assert.assertEquals(1, TokenIssuingCredentialProvider.ISSUED.get());
    Assert.assertEquals(1, creds.numberOfTokens());
  }

  @Test(timeout=5000)
  public void testObtainTokensForCredentialProvidersRenewalExcluded() throws Exception {
    TokenIssuingCredentialProvider.ISSUED.set(0);
    Configuration conf = new Configuration(TestTokenCache.conf);
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        "dtissuer://vault-a/, dtissuer://vault-b/");
    conf.set(TezConfiguration.TEZ_JOB_CREDENTIAL_PROVIDERS_TOKEN_RENEWAL_EXCLUDE, "vault-b");
    Credentials creds = new Credentials();

    TokenCache.obtainTokensForCredentialProvidersInternal(creds, conf);

    Assert.assertEquals(renewer, identifierOf(creds, "dtissuer://vault-a/"));
    // an excluded provider gets an empty renewer, which makes the RM skip renewal
    Assert.assertEquals("", identifierOf(creds, "dtissuer://vault-b/"));
  }

  @Test(timeout=5000)
  public void testObtainTokensForCredentialProvidersWithoutRenewer() throws Exception {
    TokenIssuingCredentialProvider.ISSUED.set(0);
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, "dtissuer://vault-a/");
    Credentials creds = new Credentials();

    TokenCache.obtainTokensForCredentialProvidersInternal(creds, conf);

    Assert.assertEquals(0, TokenIssuingCredentialProvider.ISSUED.get());
    Assert.assertEquals(0, creds.numberOfTokens());
  }

  @Test(timeout=5000)
  public void testObtainTokensForCredentialProvidersWithoutProviders() throws Exception {
    Credentials creds = new Credentials();

    TokenCache.obtainTokensForCredentialProvidersInternal(creds, new Configuration());

    Assert.assertEquals(0, creds.numberOfTokens());
  }

  private static String identifierOf(Credentials creds, String service) {
    Token<?> token = creds.getToken(new Text(service));
    Assert.assertNotNull("Token for " + service, token);
    return new String(token.getIdentifier(), StandardCharsets.UTF_8);
  }

  private Path[] makePaths(int count, String prefix) throws Exception {
    Path[] ps = new Path[count];
    for (int i = 0; i < count; ++i) {
      ps[i] = new Path(prefix + i);
    }
    return ps;
  }

  public static class TestFileSystem extends FilterFileSystem {
    static final FileSystem fs = mock(FileSystem.class);
    static {
      try {
        when(fs.getUri()).thenReturn(new URI("test://dir"));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    public TestFileSystem() {
      super(fs);
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
        throws IOException {
      return fs.addDelegationTokens(renewer, credentials);
    }
  }

  private MockFileSystem createFileSystemForServiceName(final String service)
      throws IOException {
    MockFileSystem mockFs = new MockFileSystem();
    when(mockFs.getCanonicalServiceName()).thenReturn(service);
    when(mockFs.getDelegationToken(any())).thenAnswer(
        new Answer<Token<?>>() {
          int unique = 0;
          @Override
          public Token<?> answer(InvocationOnMock invocation) throws Throwable {
            Token<?> token = new Token<TokenIdentifier>();
            token.setService(new Text(service));
            // use unique value so when we restore from token storage, we can
            // tell if it's really the same token
            token.setKind(new Text("token" + unique++));
            return token;
          }
        });
    return mockFs;
  }

  private void checkTokens(Credentials creds, Credentials newCreds) {
    Assert.assertEquals(creds.getAllTokens().size(),
        newCreds.getAllTokens().size());
    for (Token<?> token : newCreds.getAllTokens()) {
      Token<?> credsToken = creds.getToken(token.getService());
      Assert.assertTrue(credsToken != null);
      Assert.assertEquals(token, credsToken);
    }
  }

}
