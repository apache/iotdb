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

package org.apache.iotdb;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SSLClient {

  private static SSLConnectionSocketFactory sslConnectionSocketFactory = null;
  private static PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = null;
  private static SSLContextBuilder sslContextBuilder = null;
  private static ConnectionSocketFactory plainsf = null;

  private static class SSLClientInstance {
    private static final SSLClient instance = new SSLClient();
  }

  public static SSLClient getInstance() {
    return SSLClientInstance.instance;
  }

  private SSLClient() {
    try {
      sslContextBuilder =
          new SSLContextBuilder()
              .loadTrustMaterial(
                  null,
                  new TrustStrategy() {
                    @Override
                    public boolean isTrusted(X509Certificate[] x509Certificates, String s)
                        throws CertificateException {
                      return true;
                    }
                  });
      plainsf = PlainConnectionSocketFactory.getSocketFactory();
      sslConnectionSocketFactory =
          new SSLConnectionSocketFactory(
              sslContextBuilder.build(),
              new String[] {"TLSv1.3"},
              null,
              NoopHostnameVerifier.INSTANCE);
      Registry<ConnectionSocketFactory> registryBuilder =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register("http", plainsf)
              .register("https", sslConnectionSocketFactory)
              .build();
      poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager(registryBuilder);
      poolingHttpClientConnectionManager.setMaxTotal(10);
    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
      e.printStackTrace();
    }
  }

  public CloseableHttpClient getHttpClient() {
    CloseableHttpClient httpClient =
        HttpClients.custom()
            .setSSLSocketFactory(sslConnectionSocketFactory)
            .setConnectionManager(poolingHttpClientConnectionManager)
            .setConnectionManagerShared(true)
            .build();
    return httpClient;
  }
}
