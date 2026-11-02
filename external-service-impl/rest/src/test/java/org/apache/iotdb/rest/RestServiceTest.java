/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rest;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.assertArrayEquals;

public class RestServiceTest {

  @Test
  public void testVersionSpecificSslProtocols() throws Exception {
    String[][] cases = {
      {"TLSv1.2", "TLSv1.2"},
      {"tlsv1.2", "TLSv1.2"},
      {"TlsV1.2", "TLSv1.2"},
      {" TLSV1.2 ", "TLSv1.2"},
      {"TLSv1.3", "TLSv1.3"},
      {"tlsv1.3", "TLSv1.3"},
      {"TLSV1.3", "TLSv1.3"},
      {" tLsV1.3 ", "TLSv1.3"}
    };
    for (String[] testCase : cases) {
      String protocol = testCase[0];
      SslContextFactory.Server factory = new SslContextFactory.Server();
      new RestService().configureSSL(factory, protocol);
      try {
        factory.start();
        assertArrayEquals(
            protocol, new String[] {testCase[1]}, factory.newSSLEngine().getEnabledProtocols());
      } finally {
        factory.stop();
      }
    }
  }

  @Test
  public void testSslProtocolNameIsMatchedLiterally() {
    SslContextFactory.Server factory = new SslContextFactory.Server();
    new RestService().configureSSL(factory, "tlsv1.2");
    String[] supportedProtocols = {"TLSv1.2", "TLSv1X2", "TLSv1.3"};
    factory.selectProtocols(supportedProtocols, supportedProtocols);
    assertArrayEquals(new String[] {"TLSv1.2"}, factory.getSelectedProtocols());
  }

  @Test
  public void testGenericSslProtocolsKeepDefaults() throws Exception {
    SslContextFactory.Server defaults = new SslContextFactory.Server();
    try {
      defaults.start();
      for (String protocol : new String[] {"TLS", "tls", "SSL", "ssl", null, " "}) {
        SslContextFactory.Server factory = new SslContextFactory.Server();
        new RestService().configureSSL(factory, protocol);
        try {
          factory.start();
          assertArrayEquals(
              protocol,
              defaults.newSSLEngine().getEnabledProtocols(),
              factory.newSSLEngine().getEnabledProtocols());
        } finally {
          factory.stop();
        }
      }
    } finally {
      defaults.stop();
    }
  }

  @Test
  public void testTlcpSslProtocolKeepsProviderDefaults() throws Exception {
    SslContextFactory.Server factory = new SslContextFactory.Server();
    new RestService().configureSSL(factory, "TLCPv1.1");
    assertArrayEquals(new String[0], factory.getIncludeProtocols());

    // Verify the actual engine as well when running on a JDK that provides TLCP.
    if (Security.getProviders("SSLContext.TLCPv1.1") != null) {
      try {
        factory.start();
        assertArrayEquals(new String[] {"TLCPv1.1"}, factory.newSSLEngine().getEnabledProtocols());
      } finally {
        factory.stop();
      }
    }
  }
}
