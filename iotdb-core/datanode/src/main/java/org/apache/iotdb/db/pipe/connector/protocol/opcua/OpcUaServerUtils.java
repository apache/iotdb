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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.identity.CompositeValidator;
import org.eclipse.milo.opcua.sdk.server.identity.UsernameIdentityValidator;
import org.eclipse.milo.opcua.sdk.server.identity.X509IdentityValidator;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.BaseEventTypeNode;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.ServerTypeNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.server.util.HostnameUtil;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.UaRuntimeException;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.transport.TransportProfile;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
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
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS;
import static org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig.USER_TOKEN_POLICY_USERNAME;
import static org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig.USER_TOKEN_POLICY_X509;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ubyte;

/**
 * OPC UA Server builder for IoTDB to send data. The coding style referenced ExampleServer.java in
 * Eclipse Milo.
 */
public class OpcUaServerUtils {

  private static final String WILD_CARD_ADDRESS = "0.0.0.0";
  private static int eventId = 0;

  public static OpcUaServer getOpcUaServer(
      int tcpBindPort, int httpsBindPort, String user, String password) throws Exception {
    final Logger logger = LoggerFactory.getLogger(OpcUaServerUtils.class);

    Path securityTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "iotdb", "security");
    Files.createDirectories(securityTempDir);
    if (!Files.exists(securityTempDir)) {
      throw new PipeException("Unable to create security temp dir: " + securityTempDir);
    }

    File pkiDir = securityTempDir.resolve("pki").toFile();

    LoggerFactory.getLogger(OpcUaServerUtils.class)
        .info("Security dir: {}", securityTempDir.toAbsolutePath());
    LoggerFactory.getLogger(OpcUaServerUtils.class)
        .info("Security pki dir: {}", pkiDir.getAbsolutePath());

    IoTDBKeyStoreLoader loader =
        new IoTDBKeyStoreLoader().load(securityTempDir, password.toCharArray());

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

  private static Set<EndpointConfiguration> createEndpointConfigurations(
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

  private static EndpointConfiguration buildTcpEndpoint(
      EndpointConfiguration.Builder base, int tcpBindPort) {
    return base.copy()
        .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
        .setBindPort(tcpBindPort)
        .build();
  }

  private static EndpointConfiguration buildHttpsEndpoint(
      EndpointConfiguration.Builder base, int httpsBindPort) {
    return base.copy()
        .setTransportProfile(TransportProfile.HTTPS_UABINARY)
        .setBindPort(httpsBindPort)
        .build();
  }

  /**
   * Transfer tablet into eventNodes and post it on the eventBus, so that they will be heard at the
   * subscribers. Notice that an eventNode is reused to reduce object creation costs.
   *
   * @param server OpcUaServer
   * @param tablet the tablet to send
   * @throws UaException if failed to create event
   */
  public static void transferTablet(OpcUaServer server, Tablet tablet) throws UaException {
    // There is no nameSpace, so that nameSpaceIndex is always 0
    int pseudoNameSpaceIndex = 0;
    BaseEventTypeNode eventNode =
        server
            .getEventFactory()
            .createEvent(
                new NodeId(pseudoNameSpaceIndex, UUID.randomUUID()), Identifiers.BaseEventType);
    // Use eventNode here because other nodes doesn't support values and times simultaneously
    for (int columnIndex = 0; columnIndex < tablet.getSchemas().size(); ++columnIndex) {

      TSDataType dataType = tablet.getSchemas().get(columnIndex).getType();

      // Source name --> Sensor path, like root.test.d_0.s_0
      eventNode.setSourceName(
          tablet.deviceId + "." + tablet.getSchemas().get(columnIndex).getMeasurementId());

      // Source node --> Sensor type, like double
      eventNode.setSourceNode(convertToOpcDataType(dataType));

      for (int rowIndex = 0; rowIndex < tablet.rowSize; ++rowIndex) {
        // Filter null value
        if (tablet.bitMaps[columnIndex].isMarked(rowIndex)) {
          continue;
        }

        // time --> timeStamp
        eventNode.setTime(new DateTime(tablet.timestamps[rowIndex]));

        // Message --> Value
        switch (dataType) {
          case BOOLEAN:
            eventNode.setMessage(
                LocalizedText.english(
                    Boolean.toString(((boolean[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case INT32:
            eventNode.setMessage(
                LocalizedText.english(
                    Integer.toString(((int[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case INT64:
            eventNode.setMessage(
                LocalizedText.english(
                    Long.toString(((long[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case FLOAT:
            eventNode.setMessage(
                LocalizedText.english(
                    Float.toString(((float[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case DOUBLE:
            eventNode.setMessage(
                LocalizedText.english(
                    Double.toString(((double[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case TEXT:
            eventNode.setMessage(
                LocalizedText.english(
                    ((Binary[]) tablet.values[columnIndex])[rowIndex].toString()));
            break;
          case VECTOR:
          case UNKNOWN:
          default:
            throw new PipeRuntimeNonCriticalException(
                "Unsupported data type: " + tablet.getSchemas().get(columnIndex).getType());
        }

        // Reset the eventId each time
        eventNode.setEventId(ByteString.of(Integer.toString(eventId++).getBytes()));

        // Send the event
        server.getEventBus().post(eventNode);
      }
    }
    eventNode.delete();
  }

  private static NodeId convertToOpcDataType(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return Identifiers.Boolean;
      case INT32:
        return Identifiers.Int32;
      case INT64:
        return Identifiers.Int64;
      case FLOAT:
        return Identifiers.Float;
      case DOUBLE:
        return Identifiers.Double;
      case TEXT:
        return Identifiers.String;
      case VECTOR:
      case UNKNOWN:
      default:
        throw new PipeRuntimeNonCriticalException("Unsupported data type: " + type);
    }
  }

  private OpcUaServerUtils() {
    // Utility Class
  }
}
