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

import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.UserTokenPolicy;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Arrays;

public class ClientRunnerTest {

  private static final String CONFIGURED_NODE_URL = "opc.tcp://10.60.80.65:12686/iotdb";
  private static final SecurityPolicy SECURITY_POLICY = SecurityPolicy.Basic256Sha256;

  @Test
  public void testConfiguredHostAndPortAreUsedByDefault() {
    final EndpointDescription advertisedEndpoint =
        createEndpoint(
            "opc.tcp://fwq03-15:4840/server-path",
            SECURITY_POLICY,
            Stack.TCP_UASC_UABINARY_TRANSPORT_URI);

    final EndpointDescription effectiveEndpoint =
        ClientRunner.selectEndpoint(
                Arrays.asList(advertisedEndpoint), CONFIGURED_NODE_URL, SECURITY_POLICY, false)
            .orElseThrow(AssertionError::new);

    Assert.assertEquals(
        "opc.tcp://10.60.80.65:12686/server-path", effectiveEndpoint.getEndpointUrl());
    Assert.assertNotSame(advertisedEndpoint, effectiveEndpoint);
    Assert.assertSame(
        advertisedEndpoint.getServerCertificate(), effectiveEndpoint.getServerCertificate());
    Assert.assertEquals(advertisedEndpoint.getSecurityMode(), effectiveEndpoint.getSecurityMode());
    Assert.assertEquals(
        advertisedEndpoint.getSecurityPolicyUri(), effectiveEndpoint.getSecurityPolicyUri());
    Assert.assertSame(
        advertisedEndpoint.getUserIdentityTokens(), effectiveEndpoint.getUserIdentityTokens());
    Assert.assertEquals(
        advertisedEndpoint.getTransportProfileUri(), effectiveEndpoint.getTransportProfileUri());
  }

  @Test
  public void testAdvertisedEndpointIsUsedWhenRedirectIsAllowed() {
    final EndpointDescription advertisedEndpoint =
        createEndpoint(
            "opc.tcp://fwq03-15:4840/server-path",
            SECURITY_POLICY,
            Stack.TCP_UASC_UABINARY_TRANSPORT_URI);

    final EndpointDescription effectiveEndpoint =
        ClientRunner.selectEndpoint(
                Arrays.asList(advertisedEndpoint), CONFIGURED_NODE_URL, SECURITY_POLICY, true)
            .orElseThrow(AssertionError::new);

    Assert.assertSame(advertisedEndpoint, effectiveEndpoint);
  }

  @Test
  public void testEndpointSelectionMatchesConfiguredTransport() {
    final EndpointDescription wrongSchemeEndpoint =
        createEndpoint(
            "https://wrong-scheme:12686/iotdb",
            SECURITY_POLICY,
            Stack.HTTPS_UABINARY_TRANSPORT_URI);
    final EndpointDescription wrongTransportEndpoint =
        createEndpoint(
            "opc.tcp://wrong-transport:12686/iotdb",
            SECURITY_POLICY,
            Stack.HTTPS_UABINARY_TRANSPORT_URI);
    final EndpointDescription matchingEndpoint =
        createEndpoint(
            "opc.tcp://matching:12686/iotdb",
            SECURITY_POLICY,
            Stack.TCP_UASC_UABINARY_TRANSPORT_URI);

    final EndpointDescription selectedEndpoint =
        ClientRunner.selectEndpoint(
                Arrays.asList(wrongSchemeEndpoint, wrongTransportEndpoint, matchingEndpoint),
                CONFIGURED_NODE_URL,
                SECURITY_POLICY,
                true)
            .orElseThrow(AssertionError::new);

    Assert.assertSame(matchingEndpoint, selectedEndpoint);
  }

  @Test
  public void testAllowEndpointRedirectParticipatesInConflictDetection() {
    final String securityDir = "target/opcua-client-runner-test";
    final IoTDBOpcUaClient client =
        new IoTDBOpcUaClient(
            CONFIGURED_NODE_URL, SECURITY_POLICY, AnonymousProvider.INSTANCE, false);
    final ClientRunner runner = new ClientRunner(client, securityDir, "password", null, 10, false);

    runner.checkEquals(null, "password", Paths.get(securityDir), SECURITY_POLICY, false);
    final PipeException exception =
        Assert.assertThrows(
            PipeException.class,
            () ->
                runner.checkEquals(
                    null, "password", Paths.get(securityDir), SECURITY_POLICY, true));
    Assert.assertTrue(exception.getMessage().contains("allow endpoint redirect"));
  }

  private static EndpointDescription createEndpoint(
      final String endpointUrl,
      final SecurityPolicy securityPolicy,
      final String transportProfileUri) {
    return new EndpointDescription(
        endpointUrl,
        null,
        ByteString.of(new byte[] {1, 2, 3}),
        MessageSecurityMode.SignAndEncrypt,
        securityPolicy.getUri(),
        new UserTokenPolicy[0],
        transportProfileUri,
        Unsigned.ubyte(1));
  }
}
