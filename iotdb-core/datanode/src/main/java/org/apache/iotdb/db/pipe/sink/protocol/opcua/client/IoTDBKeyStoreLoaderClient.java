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

package org.apache.iotdb.db.pipe.sink.protocol.opcua.client;

import org.eclipse.milo.opcua.sdk.server.util.HostnameUtil;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.regex.Pattern;

class IoTDBKeyStoreLoaderClient {

  private static final Logger logger = LoggerFactory.getLogger(ClientRunner.class);

  private static final Pattern IP_ADDR_PATTERN =
      Pattern.compile("^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

  private static final String CLIENT_ALIAS = "client-ai";

  private X509Certificate[] clientCertificateChain;
  private X509Certificate clientCertificate;
  private KeyPair clientKeyPair;

  IoTDBKeyStoreLoaderClient load(final Path baseDir, final char[] password) throws Exception {
    final KeyStore keyStore = KeyStore.getInstance("PKCS12");

    final Path serverKeyStore = baseDir.resolve("iotdb-client.pfx");

    logger.info("Loading KeyStore at {}.", serverKeyStore);

    if (!Files.exists(serverKeyStore)) {
      keyStore.load(null, password);

      final KeyPair keyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048);

      final SelfSignedCertificateBuilder builder =
          new SelfSignedCertificateBuilder(keyPair)
              .setCommonName("Apache IoTDB OPC UA client")
              .setOrganization("Apache")
              .setOrganizationalUnit("dev")
              .setLocalityName("Beijing")
              .setStateName("China")
              .setCountryCode("CN")
              .setApplicationUri("urn:apache:iotdb:opc-ua-client")
              .addDnsName("localhost")
              .addIpAddress("127.0.0.1");

      // Get as many hostnames and IP addresses as we can listed in the certificate.
      for (String hostname : HostnameUtil.getHostnames("0.0.0.0")) {
        if (IP_ADDR_PATTERN.matcher(hostname).matches()) {
          builder.addIpAddress(hostname);
        } else {
          builder.addDnsName(hostname);
        }
      }

      final X509Certificate certificate = builder.build();

      keyStore.setKeyEntry(
          CLIENT_ALIAS, keyPair.getPrivate(), password, new X509Certificate[] {certificate});
      try (OutputStream out = Files.newOutputStream(serverKeyStore)) {
        keyStore.store(out, password);
      }
    } else {
      try (InputStream in = Files.newInputStream(serverKeyStore)) {
        keyStore.load(in, password);
      }
    }

    final Key clientPrivateKey = keyStore.getKey(CLIENT_ALIAS, password);
    if (clientPrivateKey instanceof PrivateKey) {
      clientCertificate = (X509Certificate) keyStore.getCertificate(CLIENT_ALIAS);

      clientCertificateChain =
          Arrays.stream(keyStore.getCertificateChain(CLIENT_ALIAS))
              .map(X509Certificate.class::cast)
              .toArray(X509Certificate[]::new);

      final PublicKey serverPublicKey = clientCertificate.getPublicKey();
      clientKeyPair = new KeyPair(serverPublicKey, (PrivateKey) clientPrivateKey);
    }

    return this;
  }

  X509Certificate getClientCertificate() {
    return clientCertificate;
  }

  public X509Certificate[] getClientCertificateChain() {
    return clientCertificateChain;
  }

  KeyPair getClientKeyPair() {
    return clientKeyPair;
  }
}
