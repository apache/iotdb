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
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
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

public abstract class SubscriptionConsumer extends SubscriptionSession {

  protected final String host;
  protected final int port;
  protected final List<String> nodeUrls;

  protected final String username;
  protected final String password;

  protected final String consumerId;
  protected final String consumerGroupId;

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionConsumer(Builder builder)
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    super(builder.host, String.valueOf(builder.port), builder.username, builder.password);

    this.host = builder.host;
    this.port = builder.port;
    this.nodeUrls = builder.nodeUrls;
    this.username = builder.username;
    this.password = builder.password;
    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;

    handshake();
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
    try {
      getSessionConnection().closeConsumer();
    } catch (TException | StatementExecutionException e) {
      // wrap to IoTDBConnectionException to keep interface consistent
      throw new IoTDBConnectionException(e);
    }
    super.close();
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
    getSessionConnection().subscribe(topicNames);
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
    getSessionConnection().unsubscribe(topicNames);
  }

  /////////////////////////////// utility ///////////////////////////////

  protected SubscriptionSessionConnection getSessionConnection() {
    return (SubscriptionSessionConnection) defaultSessionConnection;
  }

  private void handshake()
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    open();

    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    Map<Integer, TEndPoint> endPoints =
        getSessionConnection().handshake(new ConsumerConfig(consumerAttributes));
    // TODO: connect to all DNs
  }

  /////////////////////////////// builder ///////////////////////////////

  public abstract static class Builder {

    protected String host = SessionConfig.DEFAULT_HOST;
    protected int port = SessionConfig.DEFAULT_PORT;
    protected List<String> nodeUrls = null;

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

    public Builder nodeUrls(List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
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
