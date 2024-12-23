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

import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
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

import java.util.Arrays;
import java.util.Optional;

public class WebSocketConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnector.class);

  private Integer port;
  private WebSocketConnectorServer server;
  private String pipeName;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();

    port =
        parameters.getIntOrDefault(
            Arrays.asList(
                PipeConnectorConstant.CONNECTOR_WEBSOCKET_PORT_KEY,
                PipeConnectorConstant.SINK_WEBSOCKET_PORT_KEY),
            PipeConnectorConstant.CONNECTOR_WEBSOCKET_PORT_DEFAULT_VALUE);

    server = WebSocketConnectorServer.getOrCreateInstance(port);
    if (server.getPort() != port) {
      throw new PipeException(
          String.format(
              "The websocket server has already been created with port = %d. "
                  + "Please set the option cdc.port = %d.",
              server.getPort(), server.getPort()));
    }
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration) {
    pipeName = configuration.getRuntimeEnvironment().getPipeName();
  }

  @Override
  public void handshake() {
    server = WebSocketConnectorServer.getOrCreateInstance(port);
    server.register(this);

    if (!server.isStarted()) {
      synchronized (WebSocketConnectorServer.class) {
        if (!server.isStarted()) {
          server.start();
        }
      }
    }
  }

  @Override
  public void heartbeat() throws Exception {
    // Do nothing
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "WebsocketConnector only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent event =
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
      for (final PipeRawTabletInsertionEvent rawTabletInsertionEvent :
          event.toRawTabletInsertionEvents()) {
        // Skip report if any tablet events is added
        event.skipReportOnCommit();
        // Transfer raw tablet insertion event to make sure one event binds
        // to only one tablet in the server
        transfer(rawTabletInsertionEvent);
      }
      return;
    }

    if (!((EnrichedEvent) tabletInsertionEvent)
        .increaseReferenceCount(WebSocketConnector.class.getName())) {
      LOGGER.warn(
          "WebsocketConnector failed to increase the reference count of the event. Ignore it. Current event: {}.",
          tabletInsertionEvent);
      return;
    }

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
        // Skip report if any tablet events is added
        ((PipeTsFileInsertionEvent) tsFileInsertionEvent).skipReportOnCommit();
        transfer(event);
      }
    } finally {
      tsFileInsertionEvent.close();
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    // Do nothing
  }

  @Override
  public void close() throws Exception {
    if (server != null) {
      server.unregister(this);
    }
  }

  public void commit(EnrichedEvent enrichedEvent) {
    Optional.ofNullable(enrichedEvent)
        .ifPresent(event -> event.decreaseReferenceCount(WebSocketConnector.class.getName(), true));
  }

  public String getPipeName() {
    return pipeName;
  }
}
