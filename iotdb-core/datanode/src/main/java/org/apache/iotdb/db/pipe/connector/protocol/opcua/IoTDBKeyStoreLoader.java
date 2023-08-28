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

package org.apache.iotdb.db.pipe.connector.protocol.opcua;

import com.google.common.collect.Sets;
import org.eclipse.milo.opcua.sdk.server.util.HostnameUtil;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

class IoTDBKeyStoreLoader {

  private static final Pattern IP_ADDR_PATTERN =
      Pattern.compile("^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

  private static final String SERVER_ALIAS = "server-ai";

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private X509Certificate serverCertificate;
  private KeyPair serverKeyPair;

  IoTDBKeyStoreLoader load(Path baseDir, char[] password) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");

    File serverKeyStore = baseDir.resolve("example-server.pfx").toFile();

    logger.info("Loading KeyStore at {}", serverKeyStore);

    if (!serverKeyStore.exists()) {
      keyStore.load(null, password);

      KeyPair keyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048);

      String applicationUri = "urn:apache:iotdb:opc-ua-server:" + UUID.randomUUID();

      SelfSignedCertificateBuilder builder =
          new SelfSignedCertificateBuilder(keyPair)
              .setCommonName("Apache IoTDB OPC UA server")
              .setOrganization("Apache")
              .setOrganizationalUnit("dev")
              .setLocalityName("Beijing")
              .setStateName("China")
              .setCountryCode("CN")
              .setApplicationUri(applicationUri);

      // Get as many hostnames and IP addresses as we can listed in the certificate.
      Set<String> hostnames =
          Sets.union(
              Sets.newHashSet(HostnameUtil.getHostname()),
              HostnameUtil.getHostnames("0.0.0.0", false));

      for (String hostname : hostnames) {
        if (IP_ADDR_PATTERN.matcher(hostname).matches()) {
          builder.addIpAddress(hostname);
        } else {
          builder.addDnsName(hostname);
        }
      }

      X509Certificate certificate = builder.build();

      keyStore.setKeyEntry(
          SERVER_ALIAS, keyPair.getPrivate(), password, new X509Certificate[] {certificate});
      keyStore.store(new FileOutputStream(serverKeyStore), password);
    } else {
      keyStore.load(new FileInputStream(serverKeyStore), password);
    }

    Key serverPrivateKey = keyStore.getKey(SERVER_ALIAS, password);
    if (serverPrivateKey instanceof PrivateKey) {
      serverCertificate = (X509Certificate) keyStore.getCertificate(SERVER_ALIAS);

      PublicKey serverPublicKey = serverCertificate.getPublicKey();
      serverKeyPair = new KeyPair(serverPublicKey, (PrivateKey) serverPrivateKey);
    }

    return this;
  }

  X509Certificate getServerCertificate() {
    return serverCertificate;
  }

  KeyPair getServerKeyPair() {
    return serverKeyPair;
  }
}
