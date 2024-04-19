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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionNonRetryableException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRetryableException;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPollMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCommitReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribePollReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

final class SubscriptionProvider extends SubscriptionSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionProvider.class);

  private final String consumerId;
  private final String consumerGroupId;

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

  TEndPoint getEndPoint() {
    return endPoint;
  }

  /////////////////////////////// open & close ///////////////////////////////

  synchronized int handshake()
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return -1;
    }

    super.open();

    final Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    dataNodeId = getSessionConnection().handshake(new ConsumerConfig(consumerAttributes));

    isClosed.set(false);
    setAvailable();
    return dataNodeId;
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

  List<SubscriptionPolledMessage> poll(SubscriptionPollMessage pollMessage)
      throws SubscriptionException {
    final PipeSubscribePollReq req;
    try {
      req = PipeSubscribePollReq.toTPipeSubscribeReq(pollMessage);
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when serialize poll request {}: {}", pollMessage, e.getMessage());
      throw new SubscriptionRetryableException(e.getMessage(), e);
    }
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(req);
    } catch (final TException e) {
      // TODO: Distinguish between TTransportException, TApplicationException, and
      // TProtocolException.
      LOGGER.warn(
          "TException occurred when poll with request {}: {}, set SubscriptionProvider {} unavailable",
          pollMessage,
          e.getMessage(),
          true);
      setUnavailable();
      throw new SubscriptionNonRetryableException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
    final PipeSubscribePollResp pollResp = PipeSubscribePollResp.fromTPipeSubscribeResp(resp);
    return pollResp.getMessages();
  }

  void commitSync(List<SubscriptionCommitContext> subscriptionCommitContexts)
      throws SubscriptionException {
    final PipeSubscribeCommitReq req;
    try {
      req = PipeSubscribeCommitReq.toTPipeSubscribeReq(subscriptionCommitContexts);
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when serialize commit request {}: {}",
          subscriptionCommitContexts,
          e.getMessage());
      throw new SubscriptionRetryableException(e.getMessage(), e);
    }
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(req);
    } catch (final TException e) {
      // TODO: Distinguish between TTransportException, TApplicationException, and
      // TProtocolException.
      LOGGER.warn(
          "TException occurred when commit with request {}: {}, set SubscriptionProvider {} unavailable",
          subscriptionCommitContexts,
          e.getMessage(),
          true);
      setUnavailable();
      throw new SubscriptionNonRetryableException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
  }

  private static void verifyPipeSubscribeSuccess(TSStatus status) throws SubscriptionException {
    switch (status.code) {
      case 200: // SUCCESS_STATUS
        return;
      case 1900: // SUBSCRIPTION_VERSION_ERROR
      case 1901: // SUBSCRIPTION_TYPE_ERROR
      case 1902: // SUBSCRIPTION_HANDSHAKE_ERROR
      case 1903: // SUBSCRIPTION_HEARTBEAT_ERROR
      case 1904: // SUBSCRIPTION_POLL_ERROR
      case 1905: // SUBSCRIPTION_COMMIT_ERROR
      case 1906: // SUBSCRIPTION_CLOSE_ERROR
      case 1907: // SUBSCRIPTION_SUBSCRIBE_ERROR
      case 1908: // SUBSCRIPTION_UNSUBSCRIBE_ERROR
      case 1909: // SUBSCRIPTION_MISSING_CUSTOMER
        LOGGER.warn(
            "Internal error occurred, status code {}, status message {}",
            status.code,
            status.message);
        throw new SubscriptionNonRetryableException(status.message);
      case 1911: // SUBSCRIPTION_SERIALIZATION_ERROR
        LOGGER.warn(
            "Internal error occurred when serialize response, status code {}, status message {}",
            status.code,
            status.message);
        throw new SubscriptionRetryableException(status.message);
      default:
        LOGGER.warn(
            "Internal error occurred, status code {}, status message {}",
            status.code,
            status.message);
        throw new SubscriptionNonRetryableException(status.message);
    }
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
