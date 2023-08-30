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

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.utils.Pair;

import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_OPC_UA_ENABLE_CACHE_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_OPC_UA_ENABLE_CACHE_DATA_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_OPC_UA_HTTPS_BIND_PORT_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_OPC_UA_TCP_BIND_PORT_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_KEY;

/**
 * Send data in IoTDB based on Opc Ua protocol, using Eclipse Milo. All data are converted into
 * tablets, then eventNodes to send to the subscriber clients. Notice that there is no namespace
 * since the eventNodes do not need to be saved.
 */
public class IoTDBOpcUaConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBOpcUaConnector.class);

  private OpcUaServer server;
  private boolean enableCacheData;
  private final PriorityBlockingQueue<Pair<Long, Event>> events =
      new PriorityBlockingQueue<>(11, Comparator.comparing(o -> o.left));

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // All the parameters are optional
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    int tcpBindPort =
        parameters.getIntOrDefault(
            CONNECTOR_IOTDB_OPC_UA_TCP_BIND_PORT_KEY,
            CONNECTOR_IOTDB_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE);
    int httpsBindPort =
        parameters.getIntOrDefault(
            CONNECTOR_IOTDB_OPC_UA_HTTPS_BIND_PORT_KEY,
            CONNECTOR_IOTDB_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE);

    String user =
        parameters.getStringOrDefault(CONNECTOR_IOTDB_USER_KEY, CONNECTOR_IOTDB_USER_DEFAULT_VALUE);
    String password =
        parameters.getStringOrDefault(
            CONNECTOR_IOTDB_PASSWORD_KEY, CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);

    server = IoTDBOpcUaServerUtils.getIoTDBOpcUaServer(tcpBindPort, httpsBindPort, user, password);
    server.startup();

    enableCacheData =
        parameters.getBooleanOrDefault(
            CONNECTOR_IOTDB_OPC_UA_ENABLE_CACHE_DATA_KEY,
            CONNECTOR_IOTDB_OPC_UA_ENABLE_CACHE_DATA_DEFAULT_VALUE);
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
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (((EnrichedEvent) tabletInsertionEvent).shouldParsePattern()) {
      transfer(tabletInsertionEvent.parseEventWithPattern());
      return;
    }

    IoTDBOpcUaServerUtils.transferTablet(server, tabletInsertionEvent.convertToTablet());
  }

  @Override
  public void transfer(Event event) throws Exception {
    // Do nothing when receive heartbeat or other events
  }

  @Override
  public void close() throws Exception {
    server.shutdown();
  }
}
