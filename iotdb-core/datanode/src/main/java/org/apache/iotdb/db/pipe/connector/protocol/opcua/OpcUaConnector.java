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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.BaseEventTypeNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_SECURITY_DIR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_HTTPS_BIND_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_SECURITY_DIR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_UA_TCP_BIND_PORT_KEY;

/**
 * Send data in IoTDB based on Opc Ua protocol, using Eclipse Milo. All data are converted into
 * tablets, then eventNodes to send to the subscriber clients. Notice that there is no namespace
 * since the eventNodes do not need to be saved.
 */
public class OpcUaConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaConnector.class);

  private static final Map<String, Pair<AtomicInteger, OpcUaServer>>
      SERVER_KEY_TO_REFERENCE_COUNT_AND_SERVER_MAP = new ConcurrentHashMap<>();

  private String serverKey;
  private OpcUaServer server;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // All the parameters are optional
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    int tcpBindPort =
        parameters.getIntOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY, SINK_OPC_UA_TCP_BIND_PORT_KEY),
            CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE);
    int httpsBindPort =
        parameters.getIntOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY, SINK_OPC_UA_HTTPS_BIND_PORT_KEY),
            CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE);

    String user =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_USER_KEY, SINK_IOTDB_USER_KEY),
            CONNECTOR_IOTDB_USER_DEFAULT_VALUE);
    String password =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_PASSWORD_KEY, SINK_IOTDB_PASSWORD_KEY),
            CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);
    String securityDir =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_OPC_UA_SECURITY_DIR_KEY, SINK_OPC_UA_SECURITY_DIR_KEY),
            CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE);

    synchronized (SERVER_KEY_TO_REFERENCE_COUNT_AND_SERVER_MAP) {
      serverKey = httpsBindPort + ":" + tcpBindPort;

      server =
          SERVER_KEY_TO_REFERENCE_COUNT_AND_SERVER_MAP
              .computeIfAbsent(
                  serverKey,
                  key -> {
                    try {
                      final OpcUaServer newServer =
                          new OpcUaServerBuilder()
                              .setTcpBindPort(tcpBindPort)
                              .setHttpsBindPort(httpsBindPort)
                              .setUser(user)
                              .setPassword(password)
                              .setSecurityDir(securityDir)
                              .build();
                      newServer.startup();
                      return new Pair<>(new AtomicInteger(0), newServer);
                    } catch (Exception e) {
                      throw new PipeException("Failed to build and startup OpcUaServer", e);
                    }
                  })
              .getRight();
      SERVER_KEY_TO_REFERENCE_COUNT_AND_SERVER_MAP.get(serverKey).getLeft().incrementAndGet();
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
  public void transfer(Event event) throws Exception {
    // Do nothing when receive heartbeat or other events
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
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
      transferTablet(
          server, ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    } else {
      transferTablet(
          server, ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    }
  }

  /**
   * Transfer tablet into eventNodes and post it on the eventBus, so that they will be heard at the
   * subscribers. Notice that an eventNode is reused to reduce object creation costs.
   *
   * @param server OpcUaServer
   * @param tablet the tablet to send
   * @throws UaException if failed to create event
   */
  private void transferTablet(OpcUaServer server, Tablet tablet) throws UaException {
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
          tablet.deviceId
              + TsFileConstant.PATH_SEPARATOR
              + tablet.getSchemas().get(columnIndex).getMeasurementId());

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

        // Send the event
        server.getEventBus().post(eventNode);
      }
    }
    eventNode.delete();
  }

  private NodeId convertToOpcDataType(TSDataType type) {
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

  @Override
  public void close() throws Exception {
    if (serverKey == null) {
      return;
    }

    synchronized (SERVER_KEY_TO_REFERENCE_COUNT_AND_SERVER_MAP) {
      final Pair<AtomicInteger, OpcUaServer> pair =
          SERVER_KEY_TO_REFERENCE_COUNT_AND_SERVER_MAP.get(serverKey);
      if (pair == null) {
        return;
      }

      if (pair.getLeft().decrementAndGet() <= 0) {
        try {
          pair.getRight().shutdown();
        } finally {
          SERVER_KEY_TO_REFERENCE_COUNT_AND_SERVER_MAP.remove(serverKey);
        }
      }
    }
  }
}
