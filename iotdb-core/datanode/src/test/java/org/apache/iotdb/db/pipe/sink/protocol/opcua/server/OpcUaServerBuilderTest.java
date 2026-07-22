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

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.util.CertificateUtil;
import org.eclipse.milo.opcua.stack.server.EndpointConfiguration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OpcUaServerBuilderTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDetectedHostsArePublishedByDefault() {
    final Set<SecurityPolicy> securityPolicies = Collections.singleton(SecurityPolicy.None);
    final Set<String> detectedHostnames =
        new LinkedHashSet<>(Arrays.asList("opc-server", "10.60.80.65"));
    final OpcUaServerBuilder builder =
        new OpcUaServerBuilder().setSecurityPolicies(securityPolicies);

    final Set<EndpointConfiguration> endpoints =
        builder.createEndpointConfigurations(null, 12686, 8443, detectedHostnames);

    for (final String hostname : detectedHostnames) {
      Assert.assertEquals(
          2,
          endpoints.stream()
              .filter(endpoint -> endpoint.getPath().equals("/iotdb"))
              .filter(endpoint -> endpoint.getHostname().equals(hostname))
              .filter(endpoint -> endpoint.getSecurityPolicy() == SecurityPolicy.None)
              .count());
    }
    Assert.assertEquals(Collections.singleton(SecurityPolicy.None), securityPolicies);
  }

  @Test
  public void testOnlyExplicitAdvertisedHostIsPublished() {
    final Set<String> detectedHostnames =
        new LinkedHashSet<>(Arrays.asList("opc-server", "10.60.80.65"));
    final OpcUaServerBuilder builder =
        new OpcUaServerBuilder()
            .setAdvertisedHost("opc.example.com")
            .setSecurityPolicies(Collections.singleton(SecurityPolicy.None));

    final Set<EndpointConfiguration> endpoints =
        builder.createEndpointConfigurations(null, 12686, 8443, detectedHostnames);

    Assert.assertEquals(
        Collections.singleton("opc.example.com"),
        endpoints.stream().map(EndpointConfiguration::getHostname).collect(Collectors.toSet()));
    Assert.assertTrue(
        endpoints.stream().allMatch(endpoint -> "0.0.0.0".equals(endpoint.getBindAddress())));
  }

  @Test
  public void testIpv6AdvertisedHostIsNormalizedAndEndpointUrlIsRejected() {
    final OpcUaServerBuilder builder =
        new OpcUaServerBuilder()
            .setAdvertisedHost("[2001:db8::1]")
            .setSecurityPolicies(Collections.singleton(SecurityPolicy.None));

    final Set<EndpointConfiguration> endpoints =
        builder.createEndpointConfigurations(
            null, 12686, 8443, Collections.singleton("opc-server"));

    Assert.assertEquals(
        Collections.singleton("[2001:db8::1]"),
        endpoints.stream().map(EndpointConfiguration::getHostname).collect(Collectors.toSet()));
    Assert.assertTrue(
        endpoints.stream()
            .map(EndpointConfiguration::getEndpointUrl)
            .allMatch(endpointUrl -> endpointUrl.contains("://[2001:db8::1]:")));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> builder.setAdvertisedHost("opc.tcp://opc.example.com:12686/iotdb"));
  }

  @Test
  public void testNewCertificateContainsAdvertisedHost() throws Exception {
    final String advertisedHost = "opc.example.com";
    final Path securityDir = temporaryFolder.newFolder("security").toPath();

    try (final OpcUaServerBuilder builder =
        new OpcUaServerBuilder()
            .setTcpBindPort(12686)
            .setHttpsBindPort(8443)
            .setAdvertisedHost(advertisedHost)
            .setUser("root")
            .setPassword("root")
            .setSecurityDir(securityDir.toString())
            .setEnableAnonymousAccess(true)
            .setSecurityPolicies(Collections.singleton(SecurityPolicy.None))
            .setDebounceTimeMs(50)) {
      final OpcUaServer server = builder.build();
      final Set<EndpointConfiguration> endpoints = server.getConfig().getEndpoints();

      Assert.assertEquals(
          Collections.singleton(advertisedHost),
          endpoints.stream().map(EndpointConfiguration::getHostname).collect(Collectors.toSet()));
      Assert.assertTrue(
          endpoints.stream()
              .map(EndpointConfiguration::getCertificate)
              .allMatch(
                  certificate ->
                      CertificateUtil.getSanDnsNames(certificate).contains(advertisedHost)));
    }
  }

  @Test
  public void testAdvertisedHostParticipatesInConflictDetection() {
    final Path securityDir = temporaryFolder.getRoot().toPath();
    final Set<SecurityPolicy> securityPolicies = Collections.singleton(SecurityPolicy.None);
    final OpcUaServerBuilder builder =
        new OpcUaServerBuilder()
            .setAdvertisedHost("opc.example.com")
            .setUser("root")
            .setPassword("root")
            .setSecurityDir(securityDir.toString())
            .setEnableAnonymousAccess(true)
            .setSecurityPolicies(securityPolicies)
            .setDebounceTimeMs(50);

    builder.checkEquals("opc.example.com", "root", "root", securityDir, true, securityPolicies, 50);
    final PipeException exception =
        Assert.assertThrows(
            PipeException.class,
            () ->
                builder.checkEquals(
                    "other.example.com", "root", "root", securityDir, true, securityPolicies, 50));

    Assert.assertTrue(exception.getMessage().contains("advertised host"));
  }
}
