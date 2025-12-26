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

package org.apache.iotdb.db.pipe.sink.protocol.opcua;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.client.ClientRunner;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.client.IoTDBOpcUaClient;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaServerBuilder;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_DEFAULT_QUALITY_BAD_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_DEFAULT_QUALITY_GOOD_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_DEFAULT_QUALITY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_DEFAULT_QUALITY_UNCERTAIN_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_HISTORIZING_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_HISTORIZING_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_NODE_URL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_PLACEHOLDER_4_NULL_TAG_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_PLACEHOLDER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_QUALITY_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_QUALITY_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_DIR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_AES128_SHA256_RSAOAEP_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_AES256_SHA256_RSAPSS_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_BASIC_128_RSA_15_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_BASIC_256_SHA_256_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_BASIC_256_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_NONE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_SERVER_DEFAULT_VALUES;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_VALUE_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_VALUE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_WITH_QUALITY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_WITH_QUALITY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_DEFAULT_QUALITY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_HISTORIZING_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_HTTPS_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_MODEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_NODE_URL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_PLACEHOLDER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_QUALITY_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_SECURITY_DIR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_SECURITY_POLICY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_TCP_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_VALUE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_UA_WITH_QUALITY_KEY;

/**
 * Send data in IoTDB based on Opc Ua protocol, using Eclipse Milo. All data are converted into
 * tablets, and then:
 *
 * <p>1. In pub-sub mode, converted to eventNodes to send to the subscriber clients.
 *
 * <p>2. In client-server mode, push the newest value to the local server.
 */
@TreeModel
@TableModel
public class OpcUaSink implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaSink.class);

  private static final Map<String, Pair<AtomicInteger, OpcUaNameSpace>>
      SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP = new ConcurrentHashMap<>();

  private String serverKey;
  private boolean isClientServerModel;
  private String databaseName;
  private String placeHolder4NullTag;
  private @Nullable String valueName;
  private @Nullable String qualityName;
  private StatusCode defaultQuality;

  // Inner server
  private @Nullable OpcUaNameSpace nameSpace;

  // Outer server
  private @Nullable IoTDBOpcUaClient client;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator
        .validateAttributeValueRange(
            CONNECTOR_OPC_UA_MODEL_KEY,
            true,
            CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE,
            CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE)
        .validateAttributeValueRange(
            SINK_OPC_UA_MODEL_KEY,
            true,
            CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE,
            CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE)
        .validateSynonymAttributes(
            Arrays.asList(CONNECTOR_IOTDB_USER_KEY, SINK_IOTDB_USER_KEY),
            Arrays.asList(CONNECTOR_IOTDB_USERNAME_KEY, SINK_IOTDB_USERNAME_KEY),
            false);

    final PipeParameters parameters = validator.getParameters();
    if (validator
            .getParameters()
            .hasAnyAttributes(CONNECTOR_OPC_UA_NODE_URL_KEY, SINK_OPC_UA_NODE_URL_KEY)
        || parameters.getBooleanOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_WITH_QUALITY_KEY, SINK_OPC_UA_WITH_QUALITY_KEY),
            CONNECTOR_OPC_UA_WITH_QUALITY_DEFAULT_VALUE)) {
      validator.validate(
          CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE::equals,
          String.format(
              "When the OPC UA sink points to an outer server or sets 'with-quality' to true, the %s or %s must be %s.",
              CONNECTOR_OPC_UA_MODEL_KEY,
              SINK_OPC_UA_MODEL_KEY,
              CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE),
          parameters.getStringOrDefault(
              Arrays.asList(CONNECTOR_OPC_UA_MODEL_KEY, SINK_OPC_UA_MODEL_KEY),
              CONNECTOR_OPC_UA_MODEL_DEFAULT_VALUE));
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final boolean withQuality =
        parameters.getBooleanOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_WITH_QUALITY_KEY, SINK_OPC_UA_WITH_QUALITY_KEY),
            CONNECTOR_OPC_UA_WITH_QUALITY_DEFAULT_VALUE);
    valueName =
        withQuality
            ? parameters.getStringOrDefault(
                Arrays.asList(CONNECTOR_OPC_UA_VALUE_NAME_KEY, SINK_OPC_UA_VALUE_NAME_KEY),
                CONNECTOR_OPC_UA_VALUE_NAME_DEFAULT_VALUE)
            : null;
    qualityName =
        withQuality
            ? parameters.getStringOrDefault(
                Arrays.asList(CONNECTOR_OPC_UA_QUALITY_NAME_KEY, SINK_OPC_UA_QUALITY_NAME_KEY),
                CONNECTOR_OPC_UA_QUALITY_NAME_DEFAULT_VALUE)
            : null;
    defaultQuality =
        getQuality(
            withQuality
                ? parameters.getStringOrDefault(
                    Arrays.asList(
                        CONNECTOR_OPC_UA_DEFAULT_QUALITY_KEY, SINK_OPC_UA_DEFAULT_QUALITY_KEY),
                    CONNECTOR_OPC_UA_DEFAULT_QUALITY_UNCERTAIN_VALUE)
                : CONNECTOR_OPC_UA_DEFAULT_QUALITY_GOOD_VALUE);
    isClientServerModel =
        parameters
            .getStringOrDefault(
                Arrays.asList(CONNECTOR_OPC_UA_MODEL_KEY, SINK_OPC_UA_MODEL_KEY),
                CONNECTOR_OPC_UA_MODEL_DEFAULT_VALUE)
            .equals(CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE);
    placeHolder4NullTag =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_PLACEHOLDER_KEY, SINK_OPC_UA_PLACEHOLDER_KEY),
            CONNECTOR_OPC_UA_PLACEHOLDER_4_NULL_TAG_DEFAULT_VALUE);
    final DataRegion region =
        StorageEngine.getInstance()
            .getDataRegion(new DataRegionId(configuration.getRuntimeEnvironment().getRegionId()));
    databaseName = Objects.nonNull(region) ? region.getDatabaseName() : "root.__temp_db";

    if (withQuality && PathUtils.isTableModelDatabase(databaseName)) {
      throw new PipeException(
          "When the OPC UA sink sets 'with-quality' to true, the table model data is not supported.");
    }

    final String nodeUrl =
        parameters.getStringByKeys(CONNECTOR_OPC_UA_NODE_URL_KEY, SINK_OPC_UA_NODE_URL_KEY);
    if (Objects.isNull(nodeUrl)) {
      customizeServer(parameters);
    } else {
      if (PathUtils.isTableModelDatabase(databaseName)) {
        throw new PipeException(
            "When the OPC UA sink points to an outer server, the table model data is not supported.");
      }
      customizeClient(nodeUrl, parameters);
    }
  }

  private void customizeServer(final PipeParameters parameters) {
    final int tcpBindPort =
        parameters.getIntOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY, SINK_OPC_UA_TCP_BIND_PORT_KEY),
            CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE);
    final int httpsBindPort =
        parameters.getIntOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY, SINK_OPC_UA_HTTPS_BIND_PORT_KEY),
            CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE);

    final String user =
        parameters.getStringOrDefault(
            Arrays.asList(
                CONNECTOR_IOTDB_USER_KEY,
                SINK_IOTDB_USER_KEY,
                CONNECTOR_IOTDB_USERNAME_KEY,
                SINK_IOTDB_USERNAME_KEY),
            CONNECTOR_IOTDB_USER_DEFAULT_VALUE);
    final String password =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_PASSWORD_KEY, SINK_IOTDB_PASSWORD_KEY),
            CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);
    final String securityDir =
        IoTDBConfig.addDataHomeDir(
            parameters.getStringOrDefault(
                Arrays.asList(CONNECTOR_OPC_UA_SECURITY_DIR_KEY, SINK_OPC_UA_SECURITY_DIR_KEY),
                CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE
                    + File.separatorChar
                    + httpsBindPort
                    + "_"
                    + tcpBindPort));
    final boolean enableAnonymousAccess =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY,
                SINK_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY),
            CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_DEFAULT_VALUE);
    final Set<SecurityPolicy> securityPolicies =
        (parameters.hasAnyAttributes(
                    CONNECTOR_OPC_UA_SECURITY_POLICY_KEY, SINK_OPC_UA_SECURITY_POLICY_KEY)
                ? Arrays.stream(
                    parameters
                        .getStringByKeys(
                            CONNECTOR_OPC_UA_SECURITY_POLICY_KEY, SINK_OPC_UA_SECURITY_POLICY_KEY)
                        .replace(" ", "")
                        .split(","))
                : CONNECTOR_OPC_UA_SECURITY_POLICY_SERVER_DEFAULT_VALUES.stream())
            .map(this::getSecurityPolicy)
            .collect(Collectors.toSet());
    if (securityPolicies.isEmpty()) {
      throw new PipeException("The security policy cannot be empty.");
    }

    synchronized (SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP) {
      serverKey = httpsBindPort + ":" + tcpBindPort;

      nameSpace =
          SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP
              .compute(
                  serverKey,
                  (key, oldValue) -> {
                    try {
                      if (Objects.isNull(oldValue)) {
                        final OpcUaServerBuilder builder =
                            new OpcUaServerBuilder()
                                .setTcpBindPort(tcpBindPort)
                                .setHttpsBindPort(httpsBindPort)
                                .setUser(user)
                                .setPassword(password)
                                .setSecurityDir(securityDir)
                                .setEnableAnonymousAccess(enableAnonymousAccess)
                                .setSecurityPolicies(securityPolicies);
                        final OpcUaServer newServer = builder.build();
                        nameSpace = new OpcUaNameSpace(newServer, builder);
                        nameSpace.startup();
                        newServer.startup().get();
                        return new Pair<>(new AtomicInteger(0), nameSpace);
                      } else {
                        oldValue
                            .getRight()
                            .checkEquals(
                                user,
                                password,
                                securityDir,
                                enableAnonymousAccess,
                                securityPolicies);
                        return oldValue;
                      }
                    } catch (final PipeException e) {
                      throw e;
                    } catch (final Exception e) {
                      throw new PipeException("Failed to build and startup OpcUaServer", e);
                    }
                  })
              .getRight();
      SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP.get(serverKey).getLeft().incrementAndGet();
    }
  }

  private void customizeClient(final String nodeUrl, final PipeParameters parameters) {
    final SecurityPolicy policy =
        getSecurityPolicy(
            parameters
                .getStringOrDefault(
                    Arrays.asList(
                        CONNECTOR_OPC_UA_SECURITY_POLICY_KEY, SINK_OPC_UA_SECURITY_POLICY_KEY),
                    CONNECTOR_OPC_UA_SECURITY_POLICY_BASIC_256_SHA_256_VALUE)
                .toUpperCase());

    final IdentityProvider provider;
    final String userName =
        parameters.getStringByKeys(CONNECTOR_IOTDB_USER_KEY, SINK_IOTDB_USER_KEY);
    final String password =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_PASSWORD_KEY, SINK_IOTDB_PASSWORD_KEY),
            CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);
    provider =
        Objects.nonNull(userName)
            ? new UsernameProvider(userName, password)
            : new AnonymousProvider();

    final String securityDir =
        IoTDBConfig.addDataHomeDir(
            parameters.getStringOrDefault(
                Arrays.asList(CONNECTOR_OPC_UA_SECURITY_DIR_KEY, SINK_OPC_UA_SECURITY_DIR_KEY),
                CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE
                    + File.separatorChar
                    + UUID.nameUUIDFromBytes(nodeUrl.getBytes(TSFileConfig.STRING_CHARSET))));

    client =
        new IoTDBOpcUaClient(
            nodeUrl,
            policy,
            provider,
            parameters.getBooleanOrDefault(
                Arrays.asList(CONNECTOR_OPC_UA_HISTORIZING_KEY, SINK_OPC_UA_HISTORIZING_KEY),
                CONNECTOR_OPC_UA_HISTORIZING_DEFAULT_VALUE));
    new ClientRunner(client, securityDir, password).run();
  }

  private SecurityPolicy getSecurityPolicy(final String securityPolicy) {
    switch (securityPolicy.toUpperCase()) {
      case CONNECTOR_OPC_UA_SECURITY_POLICY_NONE_VALUE:
        return SecurityPolicy.None;
      case CONNECTOR_OPC_UA_SECURITY_POLICY_BASIC_128_RSA_15_VALUE:
        return SecurityPolicy.Basic128Rsa15;
      case CONNECTOR_OPC_UA_SECURITY_POLICY_BASIC_256_VALUE:
        return SecurityPolicy.Basic256;
      case CONNECTOR_OPC_UA_SECURITY_POLICY_BASIC_256_SHA_256_VALUE:
        return SecurityPolicy.Basic256Sha256;
      case CONNECTOR_OPC_UA_SECURITY_POLICY_AES128_SHA256_RSAOAEP_VALUE:
        return SecurityPolicy.Aes128_Sha256_RsaOaep;
      case CONNECTOR_OPC_UA_SECURITY_POLICY_AES256_SHA256_RSAPSS_VALUE:
        return SecurityPolicy.Aes256_Sha256_RsaPss;
      default:
        throw new PipeException(
            "The security policy can only be 'None', 'Basic128Rsa15', 'Basic256', 'Basic256Sha256', 'Aes128_Sha256_RsaOaep' or 'Aes256_Sha256_RsaPss'.");
    }
  }

  private StatusCode getQuality(final String quality) {
    switch (quality.toUpperCase()) {
      case CONNECTOR_OPC_UA_DEFAULT_QUALITY_GOOD_VALUE:
        return StatusCode.GOOD;
      case CONNECTOR_OPC_UA_DEFAULT_QUALITY_BAD_VALUE:
        return StatusCode.BAD;
      case CONNECTOR_OPC_UA_DEFAULT_QUALITY_UNCERTAIN_VALUE:
        return StatusCode.UNCERTAIN;
      default:
        throw new PipeException("The default quality can only be 'GOOD', 'BAD' or 'UNCERTAIN'.");
    }
  }

  @Override
  public void handshake() throws Exception {
    // Server side, do nothing
  }

  @Override
  public void heartbeat() throws Exception {
    // Server side, do nothing
  }

  @Override
  public void transfer(final Event event) throws Exception {
    // Do nothing when receive heartbeat or other events
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    transferByTablet(
        tabletInsertionEvent,
        LOGGER,
        (tablet, isTableModel) -> {
          if (Objects.nonNull(nameSpace)) {
            nameSpace.transfer(tablet, isTableModel, this);
          } else if (Objects.nonNull(client)) {
            client.transfer(tablet, this);
          } else {
            throw new PipeException(
                "No OPC client or server is specified when transferring tablet");
          }
        });
  }

  public static void transferByTablet(
      final TabletInsertionEvent tabletInsertionEvent,
      final Logger logger,
      final ThrowingBiConsumer<Tablet, Boolean, Exception> transferTablet)
      throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      logger.warn(
          "This Connector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      transferTabletWrapper(
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent, transferTablet);
    } else {
      transferTabletWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent, transferTablet);
    }
  }

  private static void transferTabletWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent,
      final ThrowingBiConsumer<Tablet, Boolean, Exception> transferTablet)
      throws Exception {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(OpcUaSink.class.getName())) {
      return;
    }
    try {
      for (final Tablet tablet : pipeInsertNodeTabletInsertionEvent.convertToTablets()) {
        transferTablet.accept(tablet, pipeInsertNodeTabletInsertionEvent.isTableModelEvent());
      }
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(OpcUaSink.class.getName(), false);
    }
  }

  private static void transferTabletWrapper(
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent,
      final ThrowingBiConsumer<Tablet, Boolean, Exception> transferTablet)
      throws Exception {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(OpcUaSink.class.getName())) {
      return;
    }
    try {
      transferTablet.accept(
          pipeRawTabletInsertionEvent.convertToTablet(),
          pipeRawTabletInsertionEvent.isTableModelEvent());
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(OpcUaSink.class.getName(), false);
    }
  }

  @FunctionalInterface
  public interface ThrowingBiConsumer<T, U, E extends Exception> {
    void accept(final T t, final U u) throws E;
  }

  @Override
  public void close() throws Exception {
    if (Objects.nonNull(client)) {
      client.disconnect();
    }

    if (serverKey == null) {
      return;
    }

    synchronized (SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP) {
      final Pair<AtomicInteger, OpcUaNameSpace> pair =
          SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP.get(serverKey);
      if (pair == null) {
        return;
      }

      if (pair.getLeft().decrementAndGet() <= 0) {
        try {
          pair.getRight().shutdown();
        } finally {
          SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP.remove(serverKey);
        }
      }
    }
  }

  /////////////////////////////// Getter ///////////////////////////////

  public boolean isClientServerModel() {
    return isClientServerModel;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getPlaceHolder4NullTag() {
    return placeHolder4NullTag;
  }

  @Nullable
  public String getValueName() {
    return valueName;
  }

  @Nullable
  public String getQualityName() {
    return qualityName;
  }

  public StatusCode getDefaultQuality() {
    return defaultQuality;
  }
}
