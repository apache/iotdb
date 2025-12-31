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

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.client.security.DefaultClientCertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

public class ClientRunner {

  private static final Logger logger = LoggerFactory.getLogger(ClientRunner.class);

  static {
    // Required for SecurityPolicy.Aes256_Sha256_RsaPss
    Security.addProvider(new BouncyCastleProvider());
  }

  private final IoTDBOpcUaClient configurableUaClient;
  private final Path securityDir;
  private final String password;

  public ClientRunner(
      final IoTDBOpcUaClient configurableUaClient,
      final String securityDir,
      final String password) {
    this.configurableUaClient = configurableUaClient;
    this.securityDir = Paths.get(securityDir);
    this.password = password;
  }

  private OpcUaClient createClient() throws Exception {
    Files.createDirectories(securityDir);
    if (!Files.exists(securityDir)) {
      throw new Exception("unable to create security dir: " + securityDir);
    }

    final File pkiDir = securityDir.resolve("pki").toFile();

    logger.info("security dir: {}", securityDir.toAbsolutePath());
    logger.info("security pki dir: {}", pkiDir.getAbsolutePath());

    final IoTDBKeyStoreLoaderClient loader =
        new IoTDBKeyStoreLoaderClient().load(securityDir, password.toCharArray());

    final DefaultTrustListManager trustListManager = new DefaultTrustListManager(pkiDir);

    final DefaultClientCertificateValidator certificateValidator =
        new DefaultClientCertificateValidator(trustListManager);

    return OpcUaClient.create(
        configurableUaClient.getNodeUrl(),
        endpoints -> endpoints.stream().filter(configurableUaClient.endpointFilter()).findFirst(),
        configBuilder ->
            configBuilder
                .setApplicationName(LocalizedText.english("Apache IoTDB OPC UA client"))
                .setApplicationUri("urn:apache:iotdb:opc-ua-client")
                .setKeyPair(loader.getClientKeyPair())
                .setCertificate(loader.getClientCertificate())
                .setCertificateChain(loader.getClientCertificateChain())
                .setCertificateValidator(certificateValidator)
                .setIdentityProvider(configurableUaClient.getIdentityProvider())
                .setRequestTimeout(uint(5000))
                .build());
  }

  public void run() {
    try {
      final OpcUaClient client = createClient();

      try {
        configurableUaClient.run(client);
      } catch (final Exception e) {
        throw new PipeException(
            "Error running opc client: " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
      }
    } catch (final Exception e) {
      throw new PipeException(
          "Error getting opc client: " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
    }
  }
}
