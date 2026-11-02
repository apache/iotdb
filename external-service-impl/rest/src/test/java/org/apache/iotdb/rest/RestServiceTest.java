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
    for (String protocol : new String[] {"TLSv1.2", "TLSv1.3"}) {
      SslContextFactory.Server factory = new SslContextFactory.Server();
      new RestService().configureSSL(factory, protocol);
      try {
        factory.start();
        assertArrayEquals(
            protocol, new String[] {protocol}, factory.newSSLEngine().getEnabledProtocols());
      } finally {
        factory.stop();
      }
    }
  }

  @Test
  public void testGenericSslProtocolsKeepDefaults() throws Exception {
    SslContextFactory.Server defaults = new SslContextFactory.Server();
    try {
      defaults.start();
      for (String protocol : new String[] {"TLS", "SSL", null, " "}) {
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
