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

package org.apache.iotdb.db.pipe.sink.protocol.opcua.server;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.identity.UsernameProvider;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

public class OpcUaTcpNoneCompatibilityTest {

  private static final long TIMEOUT_SECONDS = 15;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testTcpNoneSupportsAnonymousAndUsernameSessions() throws Exception {
    final int tcpBindPort = findAvailablePort();
    final Path securityDir = temporaryFolder.newFolder("tcp-none-security").toPath();
    OpcUaServer server = null;

    try (final OpcUaServerBuilder builder =
        new OpcUaServerBuilder()
            .setTcpBindPort(tcpBindPort)
            .setHttpsBindPort(tcpBindPort == 65535 ? 65534 : tcpBindPort + 1)
            .setAdvertisedHost("127.0.0.1")
            .setUser("root")
            .setPassword("root")
            .setSecurityDir(securityDir.toString())
            .setEnableAnonymousAccess(true)
            .setSecurityPolicies(Collections.singleton(SecurityPolicy.None))
            .setDebounceTimeMs(50)) {
      server = builder.build();
      server.startup().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      final String endpointUrl = "opc.tcp://127.0.0.1:" + tcpBindPort + "/iotdb";
      assertCanReadServerState(endpointUrl, AnonymousProvider.INSTANCE);
      assertCanReadServerState(endpointUrl, new UsernameProvider("root", "root"));
    } finally {
      if (server != null) {
        server.shutdown().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
      Stack.releaseSharedResources();
    }
  }

  private static void assertCanReadServerState(
      final String endpointUrl, final IdentityProvider identityProvider) throws Exception {
    final OpcUaClient client =
        OpcUaClient.create(
            endpointUrl,
            OpcUaTcpNoneCompatibilityTest::selectTcpNoneEndpoint,
            transportBuilder -> transportBuilder.setConnectTimeout(uint(TIMEOUT_SECONDS * 1000L)),
            configBuilder ->
                configBuilder
                    .setIdentityProvider(identityProvider)
                    .setRequestTimeout(uint(TIMEOUT_SECONDS * 1000L)));

    try {
      client.connectAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      final EndpointDescription selectedEndpoint = client.getConfig().getEndpoint();
      Assert.assertEquals(MessageSecurityMode.None, selectedEndpoint.getSecurityMode());
      Assert.assertEquals(SecurityPolicy.None.getUri(), selectedEndpoint.getSecurityPolicyUri());
      Assert.assertEquals(
          Stack.TCP_UASC_UABINARY_TRANSPORT_URI, selectedEndpoint.getTransportProfileUri());

      final DataValue serverState =
          client.readValue(0.0, TimestampsToReturn.Neither, NodeIds.Server_ServerStatus_State);
      Assert.assertNotNull(serverState.getValue().getValue());
      Assert.assertFalse(serverState.getStatusCode().isBad());
    } finally {
      client.disconnectAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  private static Optional<EndpointDescription> selectTcpNoneEndpoint(
      final List<EndpointDescription> endpoints) {
    return endpoints.stream()
        .filter(endpoint -> endpoint.getEndpointUrl().endsWith("/iotdb"))
        .filter(endpoint -> endpoint.getSecurityMode() == MessageSecurityMode.None)
        .filter(endpoint -> SecurityPolicy.None.getUri().equals(endpoint.getSecurityPolicyUri()))
        .filter(
            endpoint ->
                Stack.TCP_UASC_UABINARY_TRANSPORT_URI.equals(endpoint.getTransportProfileUri()))
        .findFirst();
  }

  private static int findAvailablePort() throws Exception {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
