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
import org.eclipse.milo.opcua.stack.client.security.DefaultClientCertificateValidator;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.slf4j.LoggerFactory;

import java.io.File;
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

    final File pkiDir = securityTempDir.resolve("pki").toFile();

    System.out.println("security dir: " + securityTempDir.toAbsolutePath());
    LoggerFactory.getLogger(getClass()).info("security pki dir: {}", pkiDir.getAbsolutePath());

    final IoTDBKeyStoreLoaderClient loader = new IoTDBKeyStoreLoaderClient().load(securityTempDir);

    final DefaultTrustListManager trustListManager = new DefaultTrustListManager(pkiDir);

    final DefaultClientCertificateValidator certificateValidator =
        new DefaultClientCertificateValidator(trustListManager);

    return OpcUaClient.create(
        clientExample.getEndpointUrl(),
        endpoints -> endpoints.stream().filter(clientExample.endpointFilter()).findFirst(),
        configBuilder ->
            configBuilder
                .setApplicationName(LocalizedText.english("eclipse milo opc-ua client"))
                .setApplicationUri("urn:eclipse:milo:examples:client")
                .setKeyPair(loader.getClientKeyPair())
                .setCertificate(loader.getClientCertificate())
                .setCertificateChain(loader.getClientCertificateChain())
                .setCertificateValidator(certificateValidator)
                .setIdentityProvider(clientExample.getIdentityProvider())
                .setRequestTimeout(uint(5000))
                .build());
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
              client.disconnect().get();
              Stack.releaseSharedResources();
            } catch (InterruptedException | ExecutionException e) {
              Thread.currentThread().interrupt();
              System.out.println("Error disconnecting: {}" + e.getMessage());
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
}
