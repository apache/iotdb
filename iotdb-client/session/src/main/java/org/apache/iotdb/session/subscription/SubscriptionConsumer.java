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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class SubscriptionConsumer implements AutoCloseable {

  private final TEndPoint defaultEndPoint;
  private final String username;
  private final String password;

  private final String consumerId;
  private final String consumerGroupId;

  private Map<Integer, SubscriptionProvider>
      subscriptionProviders; // contains default subscription provider, used for poll and commit
  private SubscriptionProvider defaultSubscriptionProvider; // used for subscribe and unsubscribe

  private static final long HEARTBEAT_INTERVAL = 5000; // unit: ms
  private ScheduledExecutorService heartbeatWorkerExecutor;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionConsumer(Builder builder) {
    this.defaultEndPoint = new TEndPoint(builder.host, builder.port);
    this.username = builder.username;
    this.password = builder.password;

    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;
  }

  protected SubscriptionConsumer(Builder builder, Properties config) {
    this(
        builder
            .host(
                (String) config.getOrDefault(ConsumerConstant.HOST_KEY, SessionConfig.DEFAULT_HOST))
            .port(
                (Integer)
                    config.getOrDefault(ConsumerConstant.PORT_KEY, SessionConfig.DEFAULT_PORT))
            .username(
                (String)
                    config.getOrDefault(ConsumerConstant.USERNAME_KEY, SessionConfig.DEFAULT_USER))
            .password(
                (String)
                    config.getOrDefault(
                        ConsumerConstant.PASSWORD_KEY, SessionConfig.DEFAULT_PASSWORD))
            .consumerId((String) config.get(ConsumerConstant.CONSUMER_ID_KEY))
            .consumerGroupId((String) config.get(ConsumerConstant.CONSUMER_GROUP_ID_KEY)));
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open()
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return;
    }

    subscriptionProviders = new HashMap<>();
    defaultSubscriptionProvider =
        new SubscriptionProvider(defaultEndPoint, username, password, consumerId, consumerGroupId);

    int defaultDataNodeId = defaultSubscriptionProvider.handshake();
    for (Map.Entry<Integer, TEndPoint> entry :
        getDefaultSessionConnection().fetchAllEndPoints().entrySet()) {
      if (defaultDataNodeId == entry.getKey()) {
        subscriptionProviders.put(defaultDataNodeId, defaultSubscriptionProvider);
        continue;
      }
      SubscriptionProvider subscriptionProvider =
          new SubscriptionProvider(
              entry.getValue(), username, password, consumerId, consumerGroupId);
      subscriptionProvider.handshake();
      subscriptionProviders.put(entry.getKey(), subscriptionProvider);
    }

    launchHeartbeatWorker();

    isClosed.set(false);
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      // shutdown heartbeat worker
      shutdownHeartbeatWorker();

      // close subscription provider
      for (SubscriptionProvider provider : subscriptionProviders.values()) {
        provider.close();
      }
    } finally {
      isClosed.set(true);
    }
  }

  /////////////////////////////// subscribe & unsubscribe ///////////////////////////////

  public void subscribe(String topicName)
      throws TException, IOException, StatementExecutionException {
    subscribe(Collections.singleton(topicName));
  }

  public void subscribe(String... topicNames)
      throws TException, IOException, StatementExecutionException {
    subscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void subscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException {
    getDefaultSessionConnection().subscribe(topicNames);
  }

  public void unsubscribe(String topicName)
      throws TException, IOException, StatementExecutionException {
    unsubscribe(Collections.singleton(topicName));
  }

  public void unsubscribe(String... topicNames)
      throws TException, IOException, StatementExecutionException {
    unsubscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void unsubscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException {
    getDefaultSessionConnection().unsubscribe(topicNames);
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchHeartbeatWorker() {
    heartbeatWorkerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(), r, "ConsumerHeartbeatWorker", 0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    heartbeatWorkerExecutor.scheduleAtFixedRate(
        new ConsumerHeartbeatWorker(this), 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
  }

  private void shutdownHeartbeatWorker() {
    heartbeatWorkerExecutor.shutdown();
    heartbeatWorkerExecutor = null;
  }

  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// utility ///////////////////////////////

  private SubscriptionSessionConnection getDefaultSessionConnection() {
    return defaultSubscriptionProvider.getSessionConnection();
  }

  protected List<SubscriptionSessionConnection> getSessionConnections() {
    return subscriptionProviders.values().stream()
        .map(SubscriptionProvider::getSessionConnection)
        .collect(Collectors.toList());
  }

  protected SubscriptionSessionConnection getSessionConnection(int dataNodeId) {
    return subscriptionProviders
        .getOrDefault(dataNodeId, defaultSubscriptionProvider)
        .getSessionConnection();
  }

  /////////////////////////////// builder ///////////////////////////////

  public abstract static class Builder {

    protected String host = SessionConfig.DEFAULT_HOST;
    protected int port = SessionConfig.DEFAULT_PORT;

    protected String username = SessionConfig.DEFAULT_USER;
    protected String password = SessionConfig.DEFAULT_PASSWORD;

    protected String consumerId;
    protected String consumerGroupId;

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder consumerId(String consumerId) {
      this.consumerId = consumerId;
      return this;
    }

    public Builder consumerGroupId(String consumerGroupId) {
      this.consumerGroupId = consumerGroupId;
      return this;
    }

    public abstract SubscriptionPullConsumer buildPullConsumer();

    public abstract SubscriptionPushConsumer buildPushConsumer();
  }
}
