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
package org.apache.iotdb.db.pipe.connector.protocol.websocket;

import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

public class WebSocketConnector implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnector.class);
  private Integer port;
  private WebSocketConnectorServer server;
  private String pipeName;

  public String getPipeName() {
    return pipeName;
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    port =
        parameters.getIntOrDefault(
            Arrays.asList(
                PipeConnectorConstant.CONNECTOR_WEBSOCKET_PORT_KEY,
                PipeConnectorConstant.SINK_WEBSOCKET_PORT_KEY),
            PipeConnectorConstant.CONNECTOR_WEBSOCKET_PORT_DEFAULT_VALUE);
    server = WebSocketConnectorServer.getInstance(port);
    if (server.getPort() != port) {
      throw new PipeException(
          String.format(
              "The webSocketServer has been created by port %d. Please set the option cdc.port=%d.",
              server.getPort(), server.getPort()));
    }
    pipeName = configuration.getRuntimeEnvironment().getPipeName();
  }

  @Override
  public void handshake() throws Exception {
    server = WebSocketConnectorServer.getInstance(port);
    server.register(this);
    if (!WebSocketConnectorServer.isStarted) {
      synchronized (WebSocketConnectorServer.isStarted) {
        if (!WebSocketConnectorServer.isStarted) {
          server.start();
          WebSocketConnectorServer.isStarted = true;
        }
      }
    }
  }

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "WebsocketConnector only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }

    ((EnrichedEvent) tabletInsertionEvent)
        .increaseReferenceCount(WebSocketConnector.class.getName());

    server.addEvent(tabletInsertionEvent, this);
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "WebsocketConnector only support PipeTsFileInsertionEvent. Current event: {}.",
          tsFileInsertionEvent);
      return;
    }

    try {
      for (TabletInsertionEvent event : tsFileInsertionEvent.toTabletInsertionEvents()) {
        ((EnrichedEvent) event).increaseReferenceCount(WebSocketConnector.class.getName());

        server.addEvent(event, this);
      }
    } finally {
      tsFileInsertionEvent.close();
    }
  }

  @Override
  public void transfer(Event event) throws Exception {}

  @Override
  public void close() throws Exception {
    if (port == null) {
      return;
    }
    server.unregister(this);
  }

  public synchronized void commit(@Nullable EnrichedEvent enrichedEvent) {
    Optional.ofNullable(enrichedEvent)
        .ifPresent(event -> event.decreaseReferenceCount(WebSocketConnector.class.getName(), true));
  }
}
