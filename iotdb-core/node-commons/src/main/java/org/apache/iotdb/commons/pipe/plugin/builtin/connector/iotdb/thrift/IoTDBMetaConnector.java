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

package org.apache.iotdb.commons.pipe.plugin.builtin.connector.iotdb.thrift;

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.iotdb.IoTDBConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class IoTDBMetaConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBMetaConnector.class);
  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();
  private final List<IoTDBThriftSyncConnectorClient> clients = new ArrayList<>();
  private final List<Boolean> isClientAlive = new ArrayList<>();

  private long currentClientIndex = 0;

  protected IoTDBMetaConnector() {
    // Do nothing
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    for (int i = 0; i < nodeUrls.size(); i++) {
      isClientAlive.add(false);
      clients.add(null);
    }
  }

  @Override
  public void handshake() throws Exception {
    for (int i = 0; i < clients.size(); i++) {
      if (Boolean.TRUE.equals(isClientAlive.get(i))) {
        continue;
      }

      final String ip = nodeUrls.get(i).getIp();
      final int port = nodeUrls.get(i).getPort();

      // Close the client if necessary
      if (clients.get(i) != null) {
        try {
          clients.set(i, null).close();
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to close client with target server ip: {}, port: {}, because: {}. Ignore it.",
              ip,
              port,
              e.getMessage());
        }
      }

      clients.set(
          i,
          new IoTDBThriftSyncConnectorClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs((int) PIPE_CONFIG.getPipeConnectorHandshakeTimeoutMs())
                  .setRpcThriftCompressionEnabled(
                      PIPE_CONFIG.isPipeConnectorRPCThriftCompressionEnabled())
                  .build(),
              ip,
              port,
              false,
              null,
              null));

      // TODO: validate client connectivity here, just like in ThriftSync.
      isClientAlive.set(i, true);
      clients.get(i).setTimeout((int) PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs());
      LOGGER.info("Handshake success. Target server ip: {}, port: {}", ip, port);
    }

    for (int i = 0; i < clients.size(); i++) {
      if (Boolean.TRUE.equals(isClientAlive.get(i))) {
        return;
      }
    }
    throw new PipeConnectionException(
        String.format("All target servers %s are not available.", nodeUrls));
  }

  @Override
  public void heartbeat() throws Exception {
    // TODO: heartbeat
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBMetaConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBMetaConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(Event event) throws Exception {
    // TODO: transfer schema events
  }

  private int nextClientIndex() {
    final int clientSize = clients.size();
    // Round-robin, find the next alive client
    for (int tryCount = 0; tryCount < clientSize; ++tryCount) {
      final int clientIndex = (int) (currentClientIndex++ % clientSize);
      if (Boolean.TRUE.equals(isClientAlive.get(clientIndex))) {
        return clientIndex;
      }
    }
    throw new PipeConnectionException(
        "All clients are dead, please check the connection to the receiver.");
  }

  @Override
  public void close() {
    for (int i = 0; i < clients.size(); ++i) {
      try {
        if (clients.get(i) != null) {
          clients.set(i, null).close();
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to close client {}.", i, e);
      } finally {
        isClientAlive.set(i, false);
      }
    }
  }
}
