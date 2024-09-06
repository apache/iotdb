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

import org.apache.iotdb.commons.utils.FileUtils;

import com.google.common.collect.Sets;
import org.eclipse.milo.opcua.sdk.server.util.HostnameUtil;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

class OpcUaKeyStoreLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaKeyStoreLoader.class);

  private static final Pattern IP_ADDR_PATTERN =
      Pattern.compile("^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

  private static final String SERVER_ALIAS = "server-ai";

  private X509Certificate serverCertificate;
  private KeyPair serverKeyPair;

  OpcUaKeyStoreLoader load(final Path baseDir, final char[] password) throws Exception {
    final KeyStore keyStore = KeyStore.getInstance("PKCS12");

    final File serverKeyStore = baseDir.resolve("iotdb-server.pfx").toFile();

    LOGGER.info("Loading KeyStore at {}", serverKeyStore);

    if (serverKeyStore.exists()) {
      try {
        keyStore.load(Files.newInputStream(serverKeyStore.toPath()), password);
      } catch (final IOException e) {
        LOGGER.warn("Load keyStore failed, the existing keyStore may be stale, re-constructing...");
        FileUtils.deleteFileOrDirectory(serverKeyStore);
      }
    }

    if (!serverKeyStore.exists()) {
      keyStore.load(null, password);

      final KeyPair keyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048);

      final String applicationUri = "urn:apache:iotdb:opc-ua-server:" + UUID.randomUUID();

      final SelfSignedCertificateBuilder builder =
          new SelfSignedCertificateBuilder(keyPair)
              .setCommonName("Apache IoTDB OPC UA server")
              .setOrganization("Apache")
              .setOrganizationalUnit("dev")
              .setLocalityName("Beijing")
              .setStateName("China")
              .setCountryCode("CN")
              .setApplicationUri(applicationUri);

      // Get as many hostnames and IP addresses as we can list in the certificate.
      final Set<String> hostnames =
          Sets.union(
              Sets.newHashSet(HostnameUtil.getHostname()),
              HostnameUtil.getHostnames("0.0.0.0", false));

      hostnames.forEach(
          hostname -> {
            if (IP_ADDR_PATTERN.matcher(hostname).matches()) {
              builder.addIpAddress(hostname);
            } else {
              builder.addDnsName(hostname);
            }
          });

      final X509Certificate certificate = builder.build();

      keyStore.setKeyEntry(
          SERVER_ALIAS, keyPair.getPrivate(), password, new X509Certificate[] {certificate});
      keyStore.store(Files.newOutputStream(serverKeyStore.toPath()), password);
    }

    final Key serverPrivateKey = keyStore.getKey(SERVER_ALIAS, password);
    if (serverPrivateKey instanceof PrivateKey) {
      serverCertificate = (X509Certificate) keyStore.getCertificate(SERVER_ALIAS);

      final PublicKey serverPublicKey = serverCertificate.getPublicKey();
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
