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

package org.apache.iotdb.db.pipe.connector.protocol.opcua;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_MODEL_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_MODEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_PLACEHOLDER_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_PLACEHOLDER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_SECURITY_DIR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_HTTPS_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_MODEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_PLACEHOLDER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_SECURITY_DIR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_TCP_BIND_PORT_KEY;

/**
 * Send data in IoTDB based on Opc Ua protocol, using Eclipse Milo. All data are converted into
 * tablets, then eventNodes to send to the subscriber clients. Notice that there is no namespace
 * since the eventNodes do not need to be saved.
 */
public class OpcUaConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaConnector.class);

  private static final Map<String, Pair<AtomicInteger, OpcUaNameSpace>>
      SERVER_KEY_TO_REFERENCE_COUNT_AND_NAME_SPACE_MAP = new ConcurrentHashMap<>();

  private String serverKey;
  private OpcUaNameSpace nameSpace;

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
            CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE);
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
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
    final String placeHolder =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_PLACEHOLDER_KEY, SINK_OPC_UA_PLACEHOLDER_KEY),
            CONNECTOR_OPC_UA_PLACEHOLDER_DEFAULT_VALUE);

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
                                .setEnableAnonymousAccess(enableAnonymousAccess);
                        final OpcUaServer newServer = builder.build();
                        final DataRegion region =
                            StorageEngine.getInstance()
                                .getDataRegion(
                                    new DataRegionId(
                                        configuration.getRuntimeEnvironment().getRegionId()));
                        nameSpace =
                            new OpcUaNameSpace(
                                newServer,
                                parameters
                                    .getStringOrDefault(
                                        Arrays.asList(
                                            CONNECTOR_OPC_UA_MODEL_KEY, SINK_OPC_UA_MODEL_KEY),
                                        CONNECTOR_OPC_UA_MODEL_DEFAULT_VALUE)
                                    .equals(CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE),
                                builder,
                                Objects.nonNull(region)
                                    ? region.getDatabaseName()
                                    : "root.__temp_db",
                                placeHolder);
                        nameSpace.startup();
                        newServer.startup().get();
                        return new Pair<>(new AtomicInteger(0), nameSpace);
                      } else {
                        oldValue
                            .getRight()
                            .checkEquals(user, password, securityDir, enableAnonymousAccess);
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
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "OpcUaConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      transferTabletWrapper((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    } else {
      transferTabletWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent);
    }
  }

  private void transferTabletWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws UaException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
        OpcUaConnector.class.getName())) {
      return;
    }
    try {
      for (final Tablet tablet : pipeInsertNodeTabletInsertionEvent.convertToTablets()) {
        nameSpace.transfer(tablet, pipeInsertNodeTabletInsertionEvent.isTableModelEvent());
      }
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(
          OpcUaConnector.class.getName(), false);
    }
  }

  private void transferTabletWrapper(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws UaException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(OpcUaConnector.class.getName())) {
      return;
    }
    try {
      nameSpace.transfer(
          pipeRawTabletInsertionEvent.convertToTablet(),
          pipeRawTabletInsertionEvent.isTableModelEvent());
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(OpcUaConnector.class.getName(), false);
    }
  }

  @Override
  public void close() throws Exception {
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
}
