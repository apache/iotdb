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

  private final AtomicReference<String> topic = new AtomicReference<>();

  private final AtomicReference<TabletWrapper> lastTabletWrapper = new AtomicReference<>();

  private final ClientsDaemonThread clientsDaemonThread = new ClientsDaemonThread();

  public IoTDBPollConsumer(Builder builder) {
    String host = builder.host;
    int rpcPort = builder.rpcPort;
    this.brokerPort = builder.brokerPort;
    String username = builder.username;
    String pw = builder.pw;
    int tabletBufferSize = builder.tabletBufferSize;

    session =
        new Session.Builder().host(host).port(rpcPort).username(username).password(pw).build();

    tabletBuffer = new ArrayBlockingQueue<>(tabletBufferSize);
  }

  public void open()
      throws IoTDBConnectionException, StatementExecutionException, URISyntaxException,
          InterruptedException {
    session.open();

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
    clientsDaemonThread.start();
  }

  public void close() throws IoTDBConnectionException {
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

  public void subscribe(String topic) throws IoTDBConnectionException, StatementExecutionException {
    if (this.topic.get() != null) {
      LOGGER.warn("This consumer has subscribed a topic, please unsubscribe it first!");
      return;
    }

    try (SessionDataSet pipes =
        session.executeQueryStatement(String.format("show pipe `%s`", topic))) {
      if (!pipes.hasNext()) {
        throw new StatementExecutionException(
            String.format("Topic `%s` has not been created.", topic));
      }
      List<String> columnNames = pipes.getColumnNames();
      RowRecord pipe = pipes.next();
      String state = pipe.getFields().get(columnNames.indexOf("State")).getStringValue();
      if (!"RUNNING".equals(state)) {
        session.executeNonQueryStatement(String.format("start pipe `%s`", topic));
      }
    }

    for (IoTDBWebSocketClient client : clients) {
      client.send(String.format("BIND:%s", topic));
      while (IoTDBWebSocketClient.Status.WAITING == client.getStatus()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (IoTDBWebSocketClient.Status.ERROR == client.getStatus()) {
        throw new IoTDBConnectionException(
            String.format(
                "Unable to subscribe this topic. Topic `%s` was subscribed in another program, stop it first.",
                topic));
      }
    }
    this.topic.set(topic);
  }

  public void unsubscribe() {
    for (IoTDBWebSocketClient client : clients) {
      client.send("UNBIND");
    }

    this.topic.set(null);
  }

  public synchronized TabletWrapper poll(int timeout) throws InterruptedException {
    TabletWrapper oldTabletWrapper = lastTabletWrapper.get();
    if (oldTabletWrapper != null) {
      commit(oldTabletWrapper);
    }
    TabletWrapper newTabletWrapper = tabletBuffer.poll(timeout, TimeUnit.SECONDS);
    lastTabletWrapper.set(newTabletWrapper);
    return newTabletWrapper;
  }

  public synchronized TabletWrapper poll() {
    TabletWrapper oldTabletWrapper = lastTabletWrapper.get();
    if (oldTabletWrapper != null) {
      commit(oldTabletWrapper);
    }
    TabletWrapper newTabletWrapper = tabletBuffer.poll();
    lastTabletWrapper.set(newTabletWrapper);
    return newTabletWrapper;
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

    public IoTDBPollConsumer build() {
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
            String currentTopic = topic.getAndSet(null);
            subscribe(currentTopic);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            LOGGER.warn(String.format("Failed to subscribe topic, because: %s.", e.getMessage()));
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
