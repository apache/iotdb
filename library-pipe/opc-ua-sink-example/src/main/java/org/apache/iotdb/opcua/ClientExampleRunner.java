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

package org.apache.iotdb.opcua;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.security.DefaultClientCertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.FileBasedCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.FileBasedTrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

public class ClientExampleRunner {

  static {
    // Required for SecurityPolicy.Aes256_Sha256_RsaPss
    Security.addProvider(new BouncyCastleProvider());
  }

  private final CompletableFuture<OpcUaClient> future = new CompletableFuture<>();

  private final ClientExample clientExample;
  private FileBasedTrustListManager trustListManager;

  public ClientExampleRunner(ClientExample clientExample) {
    this.clientExample = clientExample;
  }

  private OpcUaClient createClient() throws Exception {
    final Path securityTempDir =
        Paths.get(System.getProperty("java.io.tmpdir"), "client", "security");
    Files.createDirectories(securityTempDir);
    if (!Files.exists(securityTempDir)) {
      throw new Exception("unable to create security dir: " + securityTempDir);
    }

    final Path pkiDir = securityTempDir.resolve("pki");

    System.out.println("security dir: " + securityTempDir.toAbsolutePath());
    LoggerFactory.getLogger(getClass()).info("security pki dir: {}", pkiDir.toAbsolutePath());

    final IoTDBKeyStoreLoaderClient loader = new IoTDBKeyStoreLoaderClient().load(securityTempDir);

    trustListManager = FileBasedTrustListManager.createAndInitialize(pkiDir);
    final FileBasedCertificateQuarantine certificateQuarantine =
        FileBasedCertificateQuarantine.create(pkiDir.resolve("rejected").resolve("certs"));

    final DefaultClientCertificateValidator certificateValidator =
        new DefaultClientCertificateValidator(trustListManager, certificateQuarantine);

    return OpcUaClient.create(
        clientExample.getEndpointUrl(),
        endpoints -> endpoints.stream().filter(clientExample.endpointFilter()).findFirst(),
        transportBuilder -> {},
        configBuilder ->
            configBuilder
                .setApplicationName(LocalizedText.english("eclipse milo opc-ua client"))
                .setApplicationUri("urn:eclipse:milo:examples:client")
                .setKeyPair(loader.getClientKeyPair())
                .setCertificate(loader.getClientCertificate())
                .setCertificateChain(loader.getClientCertificateChain())
                .setCertificateValidator(certificateValidator)
                .setIdentityProvider(clientExample.getIdentityProvider())
                .setRequestTimeout(uint(5000)));
  }

  public void run() {
    try {
      final OpcUaClient client = createClient();

      future.whenCompleteAsync(
          (c, ex) -> {
            if (ex != null) {
              System.out.println("Error running example: " + ex.getMessage());
            }

            try {
              client.disconnectAsync().get();
            } catch (InterruptedException | ExecutionException e) {
              Thread.currentThread().interrupt();
              System.out.println("Error disconnecting: {}" + e.getMessage());
            } finally {
              closeTrustListManager();
              Stack.releaseSharedResources();
            }

            try {
              Thread.sleep(1000);
              System.exit(0);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              e.printStackTrace();
            }
          });

      try {
        clientExample.run(client, future);
        future.get(100000, TimeUnit.SECONDS);
      } catch (Throwable t) {
        System.out.println("Error running client example: " + t.getMessage() + t);
        future.completeExceptionally(t);
      }
    } catch (Throwable t) {
      System.out.println("Error getting client: {}" + t.getMessage());

      closeTrustListManager();
      future.completeExceptionally(t);

      try {
        Thread.sleep(1000);
        System.exit(0);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
      }
    }

    try {
      Thread.sleep(999_999_999);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      e.printStackTrace();
    }
  }

  private void closeTrustListManager() {
    if (trustListManager != null) {
      try {
        trustListManager.close();
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        trustListManager = null;
      }
    }
  }
}
