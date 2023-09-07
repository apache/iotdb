/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.connector.protocol.opcua;

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.identity.CompositeValidator;
import org.eclipse.milo.opcua.sdk.server.identity.UsernameIdentityValidator;
import org.eclipse.milo.opcua.sdk.server.identity.X509IdentityValidator;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.ServerTypeNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.server.util.HostnameUtil;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaRuntimeException;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.transport.TransportProfile;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.structured.BuildInfo;
import org.eclipse.milo.opcua.stack.core.util.CertificateUtil;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedHttpsCertificateBuilder;
import org.eclipse.milo.opcua.stack.server.EndpointConfiguration;
import org.eclipse.milo.opcua.stack.server.security.DefaultServerCertificateValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS;
import static org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig.USER_TOKEN_POLICY_USERNAME;
import static org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig.USER_TOKEN_POLICY_X509;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ubyte;

/**
 * OPC UA Server builder for IoTDB to send data. The coding style referenced ExampleServer.java in
 * Eclipse Milo.
 */
public class OpcUaServerBuilder {

  private static final String WILD_CARD_ADDRESS = "0.0.0.0";
  private final Logger logger = LoggerFactory.getLogger(OpcUaServerBuilder.class);

  private int tcpBindPort;
  private int httpsBindPort;
  private String user;
  private String password;

  public OpcUaServerBuilder() {
    tcpBindPort = 12686;
    httpsBindPort = 8443;
    user = "root";
    password = "root";
  }

  public OpcUaServerBuilder setTcpBindPort(int tcpBindPort) {
    this.tcpBindPort = tcpBindPort;
    return this;
  }

  public OpcUaServerBuilder setHttpsBindPort(int httpsBindPort) {
    this.httpsBindPort = httpsBindPort;
    return this;
  }

  public OpcUaServerBuilder setUser(String user) {
    this.user = user;
    return this;
  }

  public OpcUaServerBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public OpcUaServer build() throws Exception {
    Path securityTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "iotdb", "security");
    Files.createDirectories(securityTempDir);
    if (!Files.exists(securityTempDir)) {
      throw new PipeException("Unable to create security temp dir: " + securityTempDir);
    }

    File pkiDir = securityTempDir.resolve("pki").toFile();

    LoggerFactory.getLogger(OpcUaServerBuilder.class)
        .info("Security dir: {}", securityTempDir.toAbsolutePath());
    LoggerFactory.getLogger(OpcUaServerBuilder.class)
        .info("Security pki dir: {}", pkiDir.getAbsolutePath());

    OpcUaKeyStoreLoader loader =
        new OpcUaKeyStoreLoader().load(securityTempDir, password.toCharArray());

    DefaultCertificateManager certificateManager =
        new DefaultCertificateManager(loader.getServerKeyPair(), loader.getServerCertificate());

    DefaultTrustListManager trustListManager = new DefaultTrustListManager(pkiDir);
    logger.info(
        "Certificate directory is: {}, Please move certificates from the reject dir to the trusted directory to allow encrypted access",
        pkiDir.getAbsolutePath());

    KeyPair httpsKeyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048);

    SelfSignedHttpsCertificateBuilder httpsCertificateBuilder =
        new SelfSignedHttpsCertificateBuilder(httpsKeyPair);
    httpsCertificateBuilder.setCommonName(HostnameUtil.getHostname());
    HostnameUtil.getHostnames(WILD_CARD_ADDRESS).forEach(httpsCertificateBuilder::addDnsName);
    X509Certificate httpsCertificate = httpsCertificateBuilder.build();

    DefaultServerCertificateValidator certificateValidator =
        new DefaultServerCertificateValidator(trustListManager);

    UsernameIdentityValidator identityValidator =
        new UsernameIdentityValidator(
            true,
            authChallenge -> {
              String inputUsername = authChallenge.getUsername();
              String inputPassword = authChallenge.getPassword();

              return inputUsername.equals(user) && inputPassword.equals(password);
            });

    X509IdentityValidator x509IdentityValidator = new X509IdentityValidator(c -> true);

    X509Certificate certificate =
        certificateManager.getCertificates().stream()
            .findFirst()
            .orElseThrow(
                () ->
                    new UaRuntimeException(
                        StatusCodes.Bad_ConfigurationError, "No certificate found"));

    String applicationUri =
        CertificateUtil.getSanUri(certificate)
            .orElseThrow(
                () ->
                    new UaRuntimeException(
                        StatusCodes.Bad_ConfigurationError,
                        "Certificate is missing the application URI"));

    Set<EndpointConfiguration> endpointConfigurations =
        createEndpointConfigurations(certificate, tcpBindPort, httpsBindPort);

    OpcUaServerConfig serverConfig =
        OpcUaServerConfig.builder()
            .setApplicationUri(applicationUri)
            .setApplicationName(LocalizedText.english("Apache IoTDB OPC UA server"))
            .setEndpoints(endpointConfigurations)
            .setBuildInfo(
                new BuildInfo(
                    "urn:apache:iotdb:opc-ua-server",
                    "apache",
                    "Apache IoTDB OPC UA server",
                    OpcUaServer.SDK_VERSION,
                    "",
                    DateTime.now()))
            .setCertificateManager(certificateManager)
            .setTrustListManager(trustListManager)
            .setCertificateValidator(certificateValidator)
            .setHttpsKeyPair(httpsKeyPair)
            .setHttpsCertificateChain(new X509Certificate[] {httpsCertificate})
            .setIdentityValidator(new CompositeValidator(identityValidator, x509IdentityValidator))
            .setProductUri("urn:apache:iotdb:opc-ua-server")
            .build();

    // Setup server to enable event posting
    OpcUaServer server = new OpcUaServer(serverConfig);
    UaNode serverNode =
        server.getAddressSpaceManager().getManagedNode(Identifiers.Server).orElse(null);
    if (serverNode instanceof ServerTypeNode) {
      ((ServerTypeNode) serverNode).setEventNotifier(ubyte(1));
    }
    return server;
  }

  private Set<EndpointConfiguration> createEndpointConfigurations(
      X509Certificate certificate, int tcpBindPort, int httpsBindPort) {
    Set<EndpointConfiguration> endpointConfigurations = new LinkedHashSet<>();

    List<String> bindAddresses = newArrayList();
    bindAddresses.add(WILD_CARD_ADDRESS);

    Set<String> hostnames = new LinkedHashSet<>();
    hostnames.add(HostnameUtil.getHostname());
    hostnames.addAll(HostnameUtil.getHostnames(WILD_CARD_ADDRESS));

    for (String bindAddress : bindAddresses) {
      for (String hostname : hostnames) {
        EndpointConfiguration.Builder builder =
            EndpointConfiguration.newBuilder()
                .setBindAddress(bindAddress)
                .setHostname(hostname)
                .setPath("/iotdb")
                .setCertificate(certificate)
                .addTokenPolicies(
                    USER_TOKEN_POLICY_ANONYMOUS,
                    USER_TOKEN_POLICY_USERNAME,
                    USER_TOKEN_POLICY_X509);

        EndpointConfiguration.Builder noSecurityBuilder =
            builder
                .copy()
                .setSecurityPolicy(SecurityPolicy.None)
                .setSecurityMode(MessageSecurityMode.None);

        endpointConfigurations.add(buildTcpEndpoint(noSecurityBuilder, tcpBindPort));
        endpointConfigurations.add(buildHttpsEndpoint(noSecurityBuilder, httpsBindPort));

        endpointConfigurations.add(
            buildTcpEndpoint(
                builder
                    .copy()
                    .setSecurityPolicy(SecurityPolicy.Basic256Sha256)
                    .setSecurityMode(MessageSecurityMode.SignAndEncrypt),
                tcpBindPort));

        endpointConfigurations.add(
            buildHttpsEndpoint(
                builder
                    .copy()
                    .setSecurityPolicy(SecurityPolicy.Basic256Sha256)
                    .setSecurityMode(MessageSecurityMode.Sign),
                httpsBindPort));

        EndpointConfiguration.Builder discoveryBuilder =
            builder
                .copy()
                .setPath("/iotdb/discovery")
                .setSecurityPolicy(SecurityPolicy.None)
                .setSecurityMode(MessageSecurityMode.None);

        endpointConfigurations.add(buildTcpEndpoint(discoveryBuilder, tcpBindPort));
        endpointConfigurations.add(buildHttpsEndpoint(discoveryBuilder, httpsBindPort));
      }
    }

    return endpointConfigurations;
  }

  private EndpointConfiguration buildTcpEndpoint(
      EndpointConfiguration.Builder base, int tcpBindPort) {
    return base.copy()
        .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
        .setBindPort(tcpBindPort)
        .build();
  }

  private EndpointConfiguration buildHttpsEndpoint(
      EndpointConfiguration.Builder base, int httpsBindPort) {
    return base.copy()
        .setTransportProfile(TransportProfile.HTTPS_UABINARY)
        .setBindPort(httpsBindPort)
        .build();
  }
}
