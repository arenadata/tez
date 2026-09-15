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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.Token;

/**
 * Test credential provider registered for the {@code dtdown} scheme in
 * META-INF/services. It is created without trouble but cannot be reached when asked
 * for a delegation token, the way a credential store is that is down at submission time.
 */
public class UnavailableCredentialProvider extends CredentialProvider
    implements DelegationTokenIssuer {

  public static final String SCHEME = "dtdown";
  public static final AtomicInteger ATTEMPTS = new AtomicInteger();

  private final URI uri;

  private UnavailableCredentialProvider(URI uri) {
    this.uri = uri;
  }

  /**
   * Factory for {@code dtdown://<host>/} provider URIs.
   */
  public static class Factory extends CredentialProviderFactory {
    @Override
    public CredentialProvider createProvider(URI providerName, Configuration conf) {
      return SCHEME.equals(providerName.getScheme())
          ? new UnavailableCredentialProvider(providerName) : null;
    }
  }

  @Override
  public String getCanonicalServiceName() {
    return uri.toString();
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    ATTEMPTS.incrementAndGet();
    throw new IOException("Connection refused: " + uri);
  }

  @Override
  public void flush() {
  }

  @Override
  public CredentialEntry getCredentialEntry(String alias) {
    return null;
  }

  @Override
  public List<String> getAliases() {
    return Collections.emptyList();
  }

  @Override
  public CredentialEntry createCredentialEntry(String name, char[] credential) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteCredentialEntry(String name) {
    throw new UnsupportedOperationException();
  }
}
