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
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class SubscriptionConsumer implements AutoCloseable {

  protected final String consumerId;
  protected final String consumerGroupId;

  private final Map<Integer, SubscriptionProvider> subscriptionProviders;
  private final SubscriptionProvider defaultSubscriptionProvider;

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionConsumer(Builder builder)
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;

    TEndPoint defaultEndPoint = new TEndPoint(builder.host, builder.port);

    this.subscriptionProviders = new HashMap<>();
    this.defaultSubscriptionProvider =
        new SubscriptionProvider(
            defaultEndPoint,
            builder.username,
            builder.password,
            builder.consumerId,
            builder.consumerGroupId);

    Map<Integer, TEndPoint> endPoints = defaultSubscriptionProvider.handshake();
    Optional<Integer> defaultDataNodeId =
        endPoints.entrySet().stream()
            .filter(entry -> entry.getValue().equals(defaultEndPoint))
            .map(Map.Entry::getKey)
            .findFirst();
    if (!defaultDataNodeId.isPresent()) {
      throw new IoTDBConnectionException(
          "something unexpected happened when construct subscription consumer...");
    }

    subscriptionProviders.put(defaultDataNodeId.get(), defaultSubscriptionProvider);
    for (Map.Entry<Integer, TEndPoint> entry : endPoints.entrySet()) {
      if (Objects.equals(entry.getValue(), defaultEndPoint)) {
        continue;
      }
      SubscriptionProvider subscriptionProvider =
          new SubscriptionProvider(
              entry.getValue(),
              builder.username,
              builder.password,
              builder.consumerId,
              builder.consumerGroupId);
      subscriptionProvider.handshake();
      subscriptionProviders.put(entry.getKey(), subscriptionProvider);
    }
  }

  protected SubscriptionConsumer(Builder builder, Properties config)
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
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

  /////////////////////////////// APIs ///////////////////////////////

  @Override
  public void close() throws IoTDBConnectionException {
    for (SubscriptionProvider provider : subscriptionProviders.values()) {
      provider.close();
    }
  }

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

    public abstract SubscriptionPullConsumer buildPullConsumer()
        throws IoTDBConnectionException, TException, IOException, StatementExecutionException;

    public abstract SubscriptionPushConsumer buildPushConsumer()
        throws IoTDBConnectionException, TException, IOException, StatementExecutionException;
  }
}
