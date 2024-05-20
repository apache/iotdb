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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHandshakeResp;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

final class SubscriptionProvider extends SubscriptionSession {

  private String consumerId;
  private String consumerGroupId;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);
  private final AtomicBoolean isAvailable = new AtomicBoolean(false);

  private final TEndPoint endPoint;
  private int dataNodeId;

  SubscriptionProvider(
      TEndPoint endPoint,
      String username,
      String password,
      String consumerId,
      String consumerGroupId) {
    super(endPoint.ip, endPoint.port, username, password);

    this.endPoint = endPoint;
    this.consumerId = consumerId;
    this.consumerGroupId = consumerGroupId;
  }

  synchronized void handshake()
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return;
    }

    super.open();

    final Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);

    final PipeSubscribeHandshakeResp resp =
        getSessionConnection().handshake(new ConsumerConfig(consumerAttributes));
    dataNodeId = resp.getDataNodeId();
    consumerId = resp.getConsumerId();
    consumerGroupId = resp.getConsumerGroupId();

    isClosed.set(false);
    setAvailable();
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      getSessionConnection().closeConsumer();
    } catch (TException | StatementExecutionException e) {
      // wrap to IoTDBConnectionException to keep interface consistent
      throw new IoTDBConnectionException(e);
    } finally {
      super.close();
      setUnavailable();
      isClosed.set(true);
    }
  }

  SubscriptionSessionConnection getSessionConnection() {
    return (SubscriptionSessionConnection) defaultSessionConnection;
  }

  boolean isAvailable() {
    return isAvailable.get();
  }

  void setAvailable() {
    isAvailable.set(true);
  }

  void setUnavailable() {
    isAvailable.set(false);
  }

  int getDataNodeId() {
    return dataNodeId;
  }

  String getConsumerId() {
    return consumerId;
  }

  String getConsumerGroupId() {
    return consumerGroupId;
  }

  TEndPoint getEndPoint() {
    return endPoint;
  }

  @Override
  public String toString() {
    return "SubscriptionProvider{endPoint="
        + endPoint
        + ", dataNodeId="
        + dataNodeId
        + ", consumerId="
        + consumerId
        + ", consumerGroupId="
        + consumerGroupId
        + ", isAvailable="
        + isAvailable
        + ", isClosed="
        + isClosed
        + "}";
  }
}
