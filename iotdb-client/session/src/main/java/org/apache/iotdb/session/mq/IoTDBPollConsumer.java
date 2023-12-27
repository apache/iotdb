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

package org.apache.iotdb.session.mq;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.java_websocket.enums.ReadyState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class IoTDBPollConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPollConsumer.class);

  private final int brokerPort;

  private final Session session;

  private final Set<IoTDBWebSocketClient> clients = new HashSet<>();

  private final BlockingQueue<TabletWrapper> tabletBuffer;

  private final AtomicReference<TabletWrapper> lastTabletWrapper = new AtomicReference<>();

  private final ClientsDaemonThread clientsDaemonThread = new ClientsDaemonThread();

  private final String pattern;

  private final String id;

  private final String pipeName;

  public IoTDBPollConsumer(Builder builder) throws IoTDBConnectionException {
    String host = builder.host;
    int rpcPort = builder.rpcPort;
    this.brokerPort = builder.brokerPort;
    String username = builder.username;
    String pw = builder.pw;
    int tabletBufferSize = builder.tabletBufferSize;
    pattern = builder.pattern;

    if ("".equals(builder.id)) {
      throw new IoTDBConnectionException("The option `id` is required.");
    }
    id = builder.id;

    session =
        new Session.Builder().host(host).port(rpcPort).username(username).password(pw).build();

    tabletBuffer = new ArrayBlockingQueue<>(tabletBufferSize);

    pipeName = String.format("topic_%s", id);
  }

  public void open()
      throws IoTDBConnectionException, StatementExecutionException, URISyntaxException,
          InterruptedException {
    session.open();

    createAndStartPipeIfNecessary();
    createWebsocketConnections();
    sendBindMessages();

    // start the daemon thread of clients
    clientsDaemonThread.start();
  }

  public void close(boolean dropPipe) throws IoTDBConnectionException, StatementExecutionException {
    if (dropPipe) {
      session.executeNonQueryStatement(String.format("drop pipe %s", pipeName));
    }

    for (IoTDBWebSocketClient client : clients) {
      client.close();
    }

    if (session != null) {
      session.close();
    }

    if (clientsDaemonThread.isAlive()) {
      clientsDaemonThread.stop();
    }
  }

  public synchronized Tablet poll(int timeout) throws InterruptedException {
    TabletWrapper oldTabletWrapper = lastTabletWrapper.get();
    if (oldTabletWrapper != null) {
      commit(oldTabletWrapper);
    }
    TabletWrapper newTabletWrapper = tabletBuffer.poll(timeout, TimeUnit.SECONDS);
    lastTabletWrapper.set(newTabletWrapper);
    if (newTabletWrapper == null) {
      return null;
    }
    return newTabletWrapper.getTablet();
  }

  public synchronized Tablet poll() {
    TabletWrapper oldTabletWrapper = lastTabletWrapper.get();
    if (oldTabletWrapper != null) {
      commit(oldTabletWrapper);
    }
    TabletWrapper newTabletWrapper = tabletBuffer.poll();
    lastTabletWrapper.set(newTabletWrapper);
    if (newTabletWrapper == null) {
      return null;
    }
    return newTabletWrapper.getTablet();
  }

  private void createAndStartPipeIfNecessary()
      throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet pipes =
        session.executeQueryStatement(String.format("show pipe %s", pipeName))) {
      // create pipe if necessary
      if (!pipes.hasNext()) {
        String createStatement =
            String.format(
                "create pipe %s\n"
                    + "with extractor (\n"
                    + "    'extractor.pattern'='%s'\n"
                    + ")\n"
                    + "with connector (\n"
                    + "    'connector'='websocket-connector',\n"
                    + "    'connector.websocket.port'='18080',\n"
                    + "    'connector.websocket.id'='%s'\n"
                    + ")",
                pipeName, pattern, id);
        session.executeNonQueryStatement(createStatement);
        session.executeNonQueryStatement(String.format("start pipe %s", pipeName));
      } else {
        // start pipe if necessary
        List<String> columnNames = pipes.getColumnNames();
        RowRecord pipe = pipes.next();
        String state = pipe.getFields().get(columnNames.indexOf("State")).getStringValue();
        if (!"RUNNING".equals(state)) {
          session.executeNonQueryStatement(String.format("start pipe %s", pipeName));
        }
      }
    }
  }

  private void createWebsocketConnections()
      throws IoTDBConnectionException, StatementExecutionException, URISyntaxException {
    try (SessionDataSet clusterDetails = session.executeQueryStatement("show cluster details")) {
      List<String> columnNames = clusterDetails.getColumnNames();
      while (clusterDetails.hasNext()) {
        RowRecord record = clusterDetails.next();
        if ("DataNode"
            .equals(record.getFields().get(columnNames.indexOf("NodeType")).getStringValue())) {
          String address =
              record.getFields().get(columnNames.indexOf("InternalAddress")).getStringValue();
          URI uri = new URI(String.format("ws://%s:%d", address, brokerPort));

          if (!isURIAvailable(uri)) {
            throw new IoTDBConnectionException(
                String.format(
                    "Websocket server %s:%d is not available!", uri.getHost(), uri.getPort()));
          }

          IoTDBWebSocketClient client = initAndGetClient(uri);
          clients.add(client);
        }
      }
    }
  }

  private void sendBindMessages() throws IoTDBConnectionException, StatementExecutionException {
    for (IoTDBWebSocketClient client : clients) {
      sendBindMessage(client);
    }
  }

  private void sendBindMessage(IoTDBWebSocketClient client)
      throws IoTDBConnectionException, StatementExecutionException {
    client.send(String.format("BIND:%s", pipeName));
    while (IoTDBWebSocketClient.Status.WAITING == client.getStatus()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (IoTDBWebSocketClient.Status.ERROR == client.getStatus()) {
      close(false);
      throw new IoTDBConnectionException(
          String.format(
              "Unable to subscribe this topic. Topic `%s` was subscribed in another program, stop it first.",
              pipeName));
    }
  }

  private void commit(TabletWrapper tabletWrapper) {
    tabletWrapper.getWebSocketClient().send(String.format("ACK:%d", tabletWrapper.getCommitId()));
  }

  protected void addTablet(TabletWrapper tabletWrapper) {
    try {
      tabletBuffer.put(tabletWrapper);
    } catch (InterruptedException e) {
      String log = String.format("Tablet can't be put into queue, because: %s", e.getMessage());
      LOGGER.warn(log);
      Thread.currentThread().interrupt();
    }
  }

  private boolean isURIAvailable(URI uri) {
    try {
      new Socket(uri.getHost(), uri.getPort()).close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private IoTDBWebSocketClient initAndGetClient(URI uri) {
    IoTDBWebSocketClient client = new IoTDBWebSocketClient(uri, this);
    client.connect();
    while (!client.getReadyState().equals(ReadyState.OPEN)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return client;
  }

  public static class Builder {
    private String host = SessionConfig.DEFAULT_HOST;
    private int rpcPort = SessionConfig.DEFAULT_PORT;
    private int brokerPort = SessionConfig.DEFAULT_BROKER_PORT;
    private String username = SessionConfig.DEFAULT_USER;
    private String pw = SessionConfig.DEFAULT_PASSWORD;
    private int tabletBufferSize = SessionConfig.DEFAULT_TABLET_BUFFER_SIZE;
    private String pattern = SessionConfig.DEFAULT_PATTERN;
    private String id = "";

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.rpcPort = port;
      return this;
    }

    public Builder brokerPort(int port) {
      this.brokerPort = port;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.pw = password;
      return this;
    }

    public Builder tabletBufferSize(int tabletBufferSize) {
      this.tabletBufferSize = tabletBufferSize;
      return this;
    }

    public Builder pattern(String pattern) {
      this.pattern = pattern;
      return this;
    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public IoTDBPollConsumer build() throws IoTDBConnectionException {
      return new IoTDBPollConsumer(this);
    }
  }

  // To keep clients and session alive.
  private class ClientsDaemonThread extends Thread {

    @Override
    public void run() {
      while (true) {
        for (IoTDBWebSocketClient client : clients) {
          try {
            if (client.getReadyState().equals(ReadyState.OPEN)) {
              continue;
            }
            while (!isURIAvailable(client.getURI())) {
              String log =
                  String.format(
                      "The websocket server %s:%d is not available now, sleep 5 seconds.",
                      client.getURI().getHost(), client.getURI().getPort());
              LOGGER.warn(log);
              Thread.sleep(5000);
            }
            client.reconnect();
            while (!client.getReadyState().equals(ReadyState.OPEN)) {
              Thread.sleep(1000);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
