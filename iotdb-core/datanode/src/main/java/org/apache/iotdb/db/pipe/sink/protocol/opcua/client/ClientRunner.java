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

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.client.security.DefaultClientCertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.transport.TransportProfile;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.util.EndpointUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

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
  private final long timeoutSeconds;
  private final boolean allowEndpointRedirect;

  // For conflict checking
  private final String user;

  public ClientRunner(
      final IoTDBOpcUaClient configurableUaClient,
      final String securityDir,
      final String password,
      final String user,
      final long timeoutSeconds) {
    this(configurableUaClient, securityDir, password, user, timeoutSeconds, false);
  }

  public ClientRunner(
      final IoTDBOpcUaClient configurableUaClient,
      final String securityDir,
      final String password,
      final String user,
      final long timeoutSeconds,
      final boolean allowEndpointRedirect) {
    this.configurableUaClient = configurableUaClient;
    this.securityDir = Paths.get(securityDir);
    this.password = password;
    this.user = user;
    this.timeoutSeconds = timeoutSeconds;
    this.allowEndpointRedirect = allowEndpointRedirect;
    configurableUaClient.setRunner(this);
  }

  private OpcUaClient createClient() throws Exception {
    Files.createDirectories(securityDir);
    if (!Files.exists(securityDir)) {
      throw new Exception(DataNodePipeMessages.UNABLE_TO_CREATE_SECURITY_DIR + securityDir);
    }

    final File pkiDir = securityDir.resolve("pki").toFile();

    logger.info(DataNodePipeMessages.SECURITY_DIR, securityDir.toAbsolutePath());
    logger.info(DataNodePipeMessages.SECURITY_PKI_DIR, pkiDir.getAbsolutePath());

    final IoTDBKeyStoreLoaderClient loader =
        new IoTDBKeyStoreLoaderClient().load(securityDir, password.toCharArray());

    final DefaultTrustListManager trustListManager = new DefaultTrustListManager(pkiDir);

    final DefaultClientCertificateValidator certificateValidator =
        new DefaultClientCertificateValidator(trustListManager);

    return OpcUaClient.create(
        configurableUaClient.getNodeUrl(),
        endpoints ->
            selectEndpoint(
                endpoints,
                configurableUaClient.getNodeUrl(),
                configurableUaClient.getSecurityPolicy(),
                allowEndpointRedirect),
        configBuilder ->
            configBuilder
                .setApplicationName(LocalizedText.english("Apache IoTDB OPC UA client"))
                .setApplicationUri("urn:apache:iotdb:opc-ua-client")
                .setKeyPair(loader.getClientKeyPair())
                .setCertificate(loader.getClientCertificate())
                .setCertificateChain(loader.getClientCertificateChain())
                .setCertificateValidator(certificateValidator)
                .setIdentityProvider(configurableUaClient.getIdentityProvider())
                .setRequestTimeout(uint(timeoutSeconds * 1000L))
                .setConnectTimeout(uint(timeoutSeconds * 1000L))
                .setMaxResponseMessageSize(uint(0))
                .build());
  }

  static Optional<EndpointDescription> selectEndpoint(
      final List<EndpointDescription> endpoints,
      final String configuredNodeUrl,
      final SecurityPolicy securityPolicy,
      final boolean allowEndpointRedirect) {
    final String configuredScheme = normalizeScheme(EndpointUtil.getScheme(configuredNodeUrl));

    return endpoints.stream()
        .filter(endpoint -> securityPolicy.getUri().equals(endpoint.getSecurityPolicyUri()))
        .filter(endpoint -> matchesConfiguredTransport(endpoint, configuredScheme))
        .findFirst()
        .map(
            advertisedEndpoint -> {
              final EndpointDescription effectiveEndpoint =
                  allowEndpointRedirect
                      ? advertisedEndpoint
                      : EndpointUtil.updateUrl(
                          advertisedEndpoint,
                          EndpointUtil.getHost(configuredNodeUrl),
                          EndpointUtil.getPort(configuredNodeUrl));
              logger.info(
                  DataNodePipeMessages
                      .LOG_OPC_UA_ENDPOINT_SELECTED_CONFIGURED_ARG_ADVERTISED_ARG_EFFECTIVE_ARG_ALLOWENDPOINTREDIRECT_ARG_4FE076CB,
                  configuredNodeUrl,
                  advertisedEndpoint.getEndpointUrl(),
                  effectiveEndpoint.getEndpointUrl(),
                  allowEndpointRedirect);
              return effectiveEndpoint;
            });
  }

  private static boolean matchesConfiguredTransport(
      final EndpointDescription endpoint, final String configuredScheme) {
    if (Objects.isNull(configuredScheme)
        || !Objects.equals(
            configuredScheme, normalizeScheme(EndpointUtil.getScheme(endpoint.getEndpointUrl())))) {
      return false;
    }

    final String transportProfileUri = endpoint.getTransportProfileUri();
    if (Objects.isNull(transportProfileUri)) {
      return true;
    }
    try {
      return Objects.equals(
          configuredScheme,
          normalizeScheme(TransportProfile.fromUri(transportProfileUri).getScheme()));
    } catch (final IllegalArgumentException ignored) {
      // Preserve compatibility with servers that advertise a custom transport profile. The
      // endpoint URL scheme still has to match the configured URL.
      return true;
    }
  }

  private static String normalizeScheme(final String scheme) {
    return Objects.nonNull(scheme) && scheme.equalsIgnoreCase("opc.https")
        ? "https"
        : Objects.isNull(scheme) ? null : scheme.toLowerCase(Locale.ROOT);
  }

  public void run() {
    try {
      final OpcUaClient client = createClient();

      try {
        configurableUaClient.run(client);
      } catch (final Exception e) {
        throw new PipeException(
            String.format(
                DataNodePipeMessages.ERROR_RUNNING_OPC_CLIENT_FMT,
                e.getClass().getSimpleName(),
                e.getMessage()),
            e);
      }
    } catch (final Exception e) {
      throw new PipeException(
          String.format(
              DataNodePipeMessages.ERROR_GETTING_OPC_CLIENT_FMT,
              e.getClass().getSimpleName(),
              e.getMessage()),
          e);
    }
  }

  long getTimeoutSeconds() {
    return timeoutSeconds;
  }

  /////////////////////////////// Conflict detection ///////////////////////////////

  void checkEquals(
      final String user,
      final String password,
      final Path securityDir,
      final SecurityPolicy securityPolicy,
      final boolean allowEndpointRedirect) {
    checkEquals("user", this.user, user);
    checkEquals("password", this.password, password);
    checkEquals(
        "security dir",
        FileSystems.getDefault().getPath(this.securityDir.toAbsolutePath().toString()),
        FileSystems.getDefault().getPath(securityDir.toAbsolutePath().toString()));
    checkEquals("securityPolicy", configurableUaClient.getSecurityPolicy(), securityPolicy);
    checkEquals("allow endpoint redirect", this.allowEndpointRedirect, allowEndpointRedirect);
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
                  .PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_NODEURL_S_S_S_S_CONFLICTS_TO_THE_1C06A4F6,
              configurableUaClient.getNodeUrl(),
              attrName,
              thisAttr,
              attrName,
              thatAttr));
    }
  }
}
