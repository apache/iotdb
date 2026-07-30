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

package org.apache.iotdb.db.pipe.sink.protocol.opcua.server;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.net.InetAddresses;
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
import java.util.HashSet;
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
  private static final int BACKSLASH = 0x5c;

  private int tcpBindPort;
  private int httpsBindPort;
  private String advertisedHost;
  private String user;
  private String password;
  private Path securityDir;
  private boolean enableAnonymousAccess;
  private Set<SecurityPolicy> securityPolicies;
  private DefaultTrustListManager trustListManager;
  private long debounceTimeMs;

  public OpcUaServerBuilder setTcpBindPort(final int tcpBindPort) {
    this.tcpBindPort = tcpBindPort;
    return this;
  }

  public OpcUaServerBuilder setHttpsBindPort(final int httpsBindPort) {
    this.httpsBindPort = httpsBindPort;
    return this;
  }

  /** Configures the host published in endpoint URLs without changing the wildcard bind address. */
  public OpcUaServerBuilder setAdvertisedHost(final String advertisedHost) {
    this.advertisedHost = normalizeAdvertisedHost(advertisedHost);
    return this;
  }

  private static String normalizeAdvertisedHost(final String advertisedHost) {
    if (Objects.isNull(advertisedHost)) {
      return null;
    }

    String normalizedAdvertisedHost = advertisedHost.trim();
    if (normalizedAdvertisedHost.isEmpty()) {
      throw invalidAdvertisedHost();
    }
    final boolean bracketed =
        normalizedAdvertisedHost.startsWith("[") || normalizedAdvertisedHost.endsWith("]");
    if (bracketed) {
      if (!normalizedAdvertisedHost.startsWith("[") || !normalizedAdvertisedHost.endsWith("]")) {
        throw invalidAdvertisedHost();
      }
      normalizedAdvertisedHost =
          normalizedAdvertisedHost.substring(1, normalizedAdvertisedHost.length() - 1);
    }

    final boolean isIpAddress = InetAddresses.isInetAddress(normalizedAdvertisedHost);
    if (normalizedAdvertisedHost.isEmpty()
        || (bracketed && !isIpAddress)
        || normalizedAdvertisedHost.chars().anyMatch(Character::isWhitespace)
        || normalizedAdvertisedHost.contains("/")
        || normalizedAdvertisedHost.indexOf(BACKSLASH) >= 0
        || normalizedAdvertisedHost.contains("?")
        || normalizedAdvertisedHost.contains("#")
        || normalizedAdvertisedHost.contains("@")
        || normalizedAdvertisedHost.contains("[")
        || normalizedAdvertisedHost.contains("]")
        || (!isIpAddress && normalizedAdvertisedHost.contains(":"))) {
      throw invalidAdvertisedHost();
    }
    return normalizedAdvertisedHost;
  }

  private static IllegalArgumentException invalidAdvertisedHost() {
    return new IllegalArgumentException(
        DataNodePipeMessages
            .EXCEPTION_THE_ADVERTISED_HOST_MUST_BE_A_HOSTNAME_OR_IP_ADDRESS_WITHOUT_A_SCHEME_PORT_OR_PATH_6857C67A);
  }

  public OpcUaServerBuilder setUser(final String user) {
    this.user = user;
    return this;
  }

  public OpcUaServerBuilder setPassword(final String password) {
    this.password = password;
    return this;
  }

  public OpcUaServerBuilder setSecurityDir(final String securityDir) {
    this.securityDir = Paths.get(securityDir);
    return this;
  }

  public OpcUaServerBuilder setEnableAnonymousAccess(final boolean enableAnonymousAccess) {
    this.enableAnonymousAccess = enableAnonymousAccess;
    return this;
  }

  // Must be a modifiable set.
  public OpcUaServerBuilder setSecurityPolicies(final Set<SecurityPolicy> securityPolicies) {
    this.securityPolicies = securityPolicies;
    return this;
  }

  public OpcUaServerBuilder setDebounceTimeMs(long debounceTimeMs) {
    this.debounceTimeMs = debounceTimeMs;
    return this;
  }

  public long getDebounceTimeMs() {
    return debounceTimeMs;
  }

  public OpcUaServer build() throws Exception {
    Files.createDirectories(securityDir);
    if (!Files.exists(securityDir)) {
      throw new PipeException(DataNodePipeMessages.UNABLE_CREATE_SECURITY_DIR + securityDir);
    }

    final File pkiDir = securityDir.resolve("pki").toFile();

    LoggerFactory.getLogger(OpcUaServerBuilder.class)
        .info(DataNodePipeMessages.OPC_UA_SECURITY_DIR, securityDir.toAbsolutePath());
    LoggerFactory.getLogger(OpcUaServerBuilder.class)
        .info(DataNodePipeMessages.OPC_UA_SECURITY_PKI_DIR, pkiDir.getAbsolutePath());

    final Set<String> endpointHostnames = getEndpointHostnames();
    final Set<String> certificateHostnames = getCertificateHostnames(endpointHostnames);
    final OpcUaKeyStoreLoader loader =
        new OpcUaKeyStoreLoader().load(securityDir, password.toCharArray(), certificateHostnames);

    final DefaultCertificateManager certificateManager =
        new DefaultCertificateManager(loader.getServerKeyPair(), loader.getServerCertificate());

    final OpcUaServerConfig serverConfig;

    trustListManager = new DefaultTrustListManager(pkiDir);

    LOGGER.info(
        DataNodePipeMessages.CERTIFICATE_DIRECTORY_IS_PLEASE_MOVE_CERTIFICATES_FROM,
        pkiDir.getAbsolutePath());

    final KeyPair httpsKeyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048);

    final SelfSignedHttpsCertificateBuilder httpsCertificateBuilder =
        new SelfSignedHttpsCertificateBuilder(httpsKeyPair);
    httpsCertificateBuilder.setCommonName(certificateHostnames.iterator().next());
    certificateHostnames.forEach(
        hostname -> {
          if (InetAddresses.isInetAddress(hostname)) {
            httpsCertificateBuilder.addIpAddress(hostname);
          } else {
            httpsCertificateBuilder.addDnsName(hostname);
          }
        });
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
                        StatusCodes.Bad_ConfigurationError,
                        DataNodePipeMessages.NO_CERTIFICATE_FOUND));

    if (Objects.nonNull(advertisedHost) && !isAdvertisedHostInCertificate(certificate)) {
      LOGGER.warn(
          DataNodePipeMessages
              .LOG_ADVERTISED_HOST_ARG_IS_NOT_PRESENT_IN_THE_LOADED_OPC_UA_SERVER_CERTIFICATE_SUBJECT_ALTERNATIVE_NAMES_SECURED_CLIENTS_MAY_REJECT_IT_REPLACE_OR_REGENERATE_THE_CERTIFICATE_AND_ESTABLISH_TRUST_AGAIN_912358AF,
          advertisedHost);
    }

    final String applicationUri =
        CertificateUtil.getSanUri(certificate)
            .orElseThrow(
                () ->
                    new UaRuntimeException(
                        StatusCodes.Bad_ConfigurationError,
                        DataNodePipeMessages.CERTIFICATE_MISSING_APPLICATION_URI));

    final Set<EndpointConfiguration> endpointConfigurations =
        createEndpointConfigurations(certificate, tcpBindPort, httpsBindPort, endpointHostnames);

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

  private Set<String> getEndpointHostnames() {
    if (Objects.nonNull(advertisedHost)) {
      final Set<String> hostnames = new LinkedHashSet<>();
      hostnames.add(toEndpointHostname(advertisedHost));
      return hostnames;
    }
    final Set<String> hostnames = new LinkedHashSet<>();
    hostnames.add(toEndpointHostname(HostnameUtil.getHostname()));
    HostnameUtil.getHostnames(WILD_CARD_ADDRESS).stream()
        .map(OpcUaServerBuilder::toEndpointHostname)
        .forEach(hostnames::add);
    return hostnames;
  }

  private static Set<String> getCertificateHostnames(final Set<String> endpointHostnames) {
    final Set<String> certificateHostnames = new LinkedHashSet<>();
    endpointHostnames.stream()
        .map(OpcUaServerBuilder::removeIpv6Brackets)
        .forEach(certificateHostnames::add);
    return certificateHostnames;
  }

  private static String toEndpointHostname(final String hostname) {
    return InetAddresses.isInetAddress(hostname) && hostname.indexOf(':') >= 0
        ? '[' + hostname + ']'
        : hostname;
  }

  private static String removeIpv6Brackets(final String hostname) {
    return hostname.startsWith("[") && hostname.endsWith("]")
        ? hostname.substring(1, hostname.length() - 1)
        : hostname;
  }

  private boolean isAdvertisedHostInCertificate(final X509Certificate certificate) {
    if (InetAddresses.isInetAddress(advertisedHost)) {
      return CertificateUtil.getSanIpAddresses(certificate).stream()
          .filter(InetAddresses::isInetAddress)
          .map(InetAddresses::forString)
          .anyMatch(InetAddresses.forString(advertisedHost)::equals);
    }
    return CertificateUtil.getSanDnsNames(certificate).stream()
        .anyMatch(hostname -> hostname.equalsIgnoreCase(advertisedHost));
  }

  Set<EndpointConfiguration> createEndpointConfigurations(
      final X509Certificate certificate,
      final int tcpBindPort,
      final int httpsBindPort,
      final Set<String> hostnames) {
    final Set<EndpointConfiguration> endpointConfigurations = new LinkedHashSet<>();
    final Set<String> effectiveHostnames = new LinkedHashSet<>();
    if (Objects.nonNull(advertisedHost)) {
      effectiveHostnames.add(toEndpointHostname(advertisedHost));
    } else {
      hostnames.stream()
          .map(OpcUaServerBuilder::toEndpointHostname)
          .forEach(effectiveHostnames::add);
    }

    final List<String> bindAddresses = newArrayList();
    bindAddresses.add(WILD_CARD_ADDRESS);

    for (final String bindAddress : bindAddresses) {
      for (final String hostname : effectiveHostnames) {
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

        final Set<SecurityPolicy> securityPolicySet = new HashSet<>(securityPolicies);
        if (securityPolicySet.contains(SecurityPolicy.None)) {
          final EndpointConfiguration.Builder noSecurityBuilder =
              builder
                  .copy()
                  .setSecurityPolicy(SecurityPolicy.None)
                  .setSecurityMode(MessageSecurityMode.None);

          endpointConfigurations.add(buildTcpEndpoint(noSecurityBuilder, tcpBindPort));
          endpointConfigurations.add(buildHttpsEndpoint(noSecurityBuilder, httpsBindPort));
          securityPolicySet.remove(SecurityPolicy.None);
        }

        for (final SecurityPolicy securityPolicy : securityPolicySet) {
          endpointConfigurations.add(
              buildTcpEndpoint(
                  builder
                      .copy()
                      .setSecurityPolicy(securityPolicy)
                      .setSecurityMode(MessageSecurityMode.SignAndEncrypt),
                  tcpBindPort));

          endpointConfigurations.add(
              buildHttpsEndpoint(
                  builder
                      .copy()
                      .setSecurityPolicy(securityPolicy)
                      .setSecurityMode(MessageSecurityMode.Sign),
                  httpsBindPort));
        }

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
      final String advertisedHost,
      final String user,
      final String password,
      final Path securityDir,
      final boolean enableAnonymousAccess,
      final Set<SecurityPolicy> securityPolicies,
      final long debounceTimeMs) {
    checkEquals("advertised host", this.advertisedHost, normalizeAdvertisedHost(advertisedHost));
    checkEquals("user", this.user, user);
    checkEquals("password", this.password, password);
    checkEquals(
        "security dir",
        FileSystems.getDefault().getPath(this.securityDir.toAbsolutePath().toString()),
        FileSystems.getDefault().getPath(securityDir.toAbsolutePath().toString()));
    checkEquals("enableAnonymousAccess option", this.enableAnonymousAccess, enableAnonymousAccess);
    checkEquals("securityPolicies", this.securityPolicies, securityPolicies);
    checkEquals("debounceTimeMs", this.debounceTimeMs, debounceTimeMs);
  }

  private void checkEquals(final String attrName, Object thisAttr, Object thatAttr) {
    if (!Objects.equals(thisAttr, thatAttr)) {
      if (attrName.equals("password")) {
        thisAttr = "****";
        thatAttr = "****";
      }
      throw new PipeException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_TCP_PORT_S_AND_HTTPS_PORT_S_S_S_08C076F7,
              tcpBindPort,
              httpsBindPort,
              attrName,
              thisAttr,
              attrName,
              thatAttr));
    }
  }

  @Override
  public void close() {
    if (Objects.nonNull(trustListManager)) {
      try {
        trustListManager.close();
      } catch (final IOException e) {
        LOGGER.warn(DataNodePipeMessages.FAILED_TO_CLOSE_TRUSTLISTMANAGER_BECAUSE, e.getMessage());
      }
    }
  }
}
