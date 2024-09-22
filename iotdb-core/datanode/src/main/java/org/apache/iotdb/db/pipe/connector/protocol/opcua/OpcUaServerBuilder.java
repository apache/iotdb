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

import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
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
public class OpcUaServerBuilder implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaServerBuilder.class);

  private static final String WILD_CARD_ADDRESS = "0.0.0.0";

  private int tcpBindPort;
  private int httpsBindPort;
  private String user;
  private String password;
  private Path securityDir;
  private boolean enableAnonymousAccess;
  private DefaultTrustListManager trustListManager;

  OpcUaServerBuilder() {
    tcpBindPort = PipeConnectorConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE;
    httpsBindPort = PipeConnectorConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE;
    user = PipeConnectorConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
    password = PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
    securityDir = Paths.get(PipeConnectorConstant.CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE);
    enableAnonymousAccess =
        PipeConnectorConstant.CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_DEFAULT_VALUE;
  }

  OpcUaServerBuilder setTcpBindPort(final int tcpBindPort) {
    this.tcpBindPort = tcpBindPort;
    return this;
  }

  OpcUaServerBuilder setHttpsBindPort(final int httpsBindPort) {
    this.httpsBindPort = httpsBindPort;
    return this;
  }

  OpcUaServerBuilder setUser(final String user) {
    this.user = user;
    return this;
  }

  OpcUaServerBuilder setPassword(final String password) {
    this.password = password;
    return this;
  }

  OpcUaServerBuilder setSecurityDir(final String securityDir) {
    this.securityDir = Paths.get(securityDir);
    return this;
  }

  OpcUaServerBuilder setEnableAnonymousAccess(final boolean enableAnonymousAccess) {
    this.enableAnonymousAccess = enableAnonymousAccess;
    return this;
  }

  OpcUaServer build() throws Exception {
    Files.createDirectories(securityDir);
    if (!Files.exists(securityDir)) {
      throw new PipeException("Unable to create security dir: " + securityDir);
    }

    final File pkiDir = securityDir.resolve("pki").toFile();

    LoggerFactory.getLogger(OpcUaServerBuilder.class)
        .info("Security dir: {}", securityDir.toAbsolutePath());
    LoggerFactory.getLogger(OpcUaServerBuilder.class)
        .info("Security pki dir: {}", pkiDir.getAbsolutePath());

    final OpcUaKeyStoreLoader loader =
        new OpcUaKeyStoreLoader().load(securityDir, password.toCharArray());

    final DefaultCertificateManager certificateManager =
        new DefaultCertificateManager(loader.getServerKeyPair(), loader.getServerCertificate());

    final OpcUaServerConfig serverConfig;

    trustListManager = new DefaultTrustListManager(pkiDir);

    LOGGER.info(
        "Certificate directory is: {}, Please move certificates from the reject dir to the trusted directory to allow encrypted access",
        pkiDir.getAbsolutePath());

    final KeyPair httpsKeyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048);

    final SelfSignedHttpsCertificateBuilder httpsCertificateBuilder =
        new SelfSignedHttpsCertificateBuilder(httpsKeyPair);
    httpsCertificateBuilder.setCommonName(HostnameUtil.getHostname());
    HostnameUtil.getHostnames(WILD_CARD_ADDRESS).forEach(httpsCertificateBuilder::addDnsName);
    final X509Certificate httpsCertificate = httpsCertificateBuilder.build();

    final DefaultServerCertificateValidator certificateValidator =
        new DefaultServerCertificateValidator(trustListManager);

    final UsernameIdentityValidator identityValidator =
        new UsernameIdentityValidator(
            enableAnonymousAccess,
            authChallenge ->
                authChallenge.getUsername().equals(user)
                    && authChallenge.getPassword().equals(password));

    final X509IdentityValidator x509IdentityValidator = new X509IdentityValidator(c -> true);

    final X509Certificate certificate =
        certificateManager.getCertificates().stream()
            .findFirst()
            .orElseThrow(
                () ->
                    new UaRuntimeException(
                        StatusCodes.Bad_ConfigurationError, "No certificate found"));

    final String applicationUri =
        CertificateUtil.getSanUri(certificate)
            .orElseThrow(
                () ->
                    new UaRuntimeException(
                        StatusCodes.Bad_ConfigurationError,
                        "Certificate is missing the application URI"));

    final Set<EndpointConfiguration> endpointConfigurations =
        createEndpointConfigurations(certificate, tcpBindPort, httpsBindPort);

    serverConfig =
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
    final OpcUaServer server = new OpcUaServer(serverConfig);
    final UaNode serverNode =
        server.getAddressSpaceManager().getManagedNode(Identifiers.Server).orElse(null);
    if (serverNode instanceof ServerTypeNode) {
      ((ServerTypeNode) serverNode).setEventNotifier(ubyte(1));
    }
    return server;
  }

  private Set<EndpointConfiguration> createEndpointConfigurations(
      final X509Certificate certificate, final int tcpBindPort, final int httpsBindPort) {
    final Set<EndpointConfiguration> endpointConfigurations = new LinkedHashSet<>();

    final List<String> bindAddresses = newArrayList();
    bindAddresses.add(WILD_CARD_ADDRESS);

    final Set<String> hostnames = new LinkedHashSet<>();
    hostnames.add(HostnameUtil.getHostname());
    hostnames.addAll(HostnameUtil.getHostnames(WILD_CARD_ADDRESS));

    for (final String bindAddress : bindAddresses) {
      for (final String hostname : hostnames) {
        final EndpointConfiguration.Builder builder =
            EndpointConfiguration.newBuilder()
                .setBindAddress(bindAddress)
                .setHostname(hostname)
                .setPath("/iotdb")
                .setCertificate(certificate)
                .addTokenPolicies(
                    USER_TOKEN_POLICY_ANONYMOUS,
                    USER_TOKEN_POLICY_USERNAME,
                    USER_TOKEN_POLICY_X509);

        final EndpointConfiguration.Builder noSecurityBuilder =
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

        final EndpointConfiguration.Builder discoveryBuilder =
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
      final EndpointConfiguration.Builder base, final int tcpBindPort) {
    return base.copy()
        .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
        .setBindPort(tcpBindPort)
        .build();
  }

  private EndpointConfiguration buildHttpsEndpoint(
      final EndpointConfiguration.Builder base, final int httpsBindPort) {
    return base.copy()
        .setTransportProfile(TransportProfile.HTTPS_UABINARY)
        .setBindPort(httpsBindPort)
        .build();
  }

  /////////////////////////////// Conflict detection ///////////////////////////////

  void checkEquals(
      final String user,
      final String password,
      final Path securityDir,
      final boolean enableAnonymousAccess) {
    checkEquals("user", this.user, user);
    checkEquals("password", this.password, password);
    checkEquals(
        "security dir",
        FileSystems.getDefault().getPath(this.securityDir.toAbsolutePath().toString()),
        FileSystems.getDefault().getPath(securityDir.toAbsolutePath().toString()));
    checkEquals("enableAnonymousAccess option", this.enableAnonymousAccess, enableAnonymousAccess);
  }

  private void checkEquals(final String attrName, final Object thisAttr, final Object thatAttr) {
    if (!Objects.equals(thisAttr, thatAttr)) {
      throw new PipeException(
          String.format(
              "The existing server with tcp port %s and https port %s's %s %s conflicts to the new %s %s, reject reusing.",
              tcpBindPort, httpsBindPort, attrName, thisAttr, attrName, thatAttr));
    }
  }

  @Override
  public void close() {
    if (Objects.nonNull(trustListManager)) {
      try {
        trustListManager.close();
      } catch (final IOException e) {
        LOGGER.warn("Failed to close trustListManager, because {}.", e.getMessage());
      }
    }
  }
}
