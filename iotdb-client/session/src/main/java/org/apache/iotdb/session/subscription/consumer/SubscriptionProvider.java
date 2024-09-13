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

package org.apache.iotdb.session.subscription.consumer;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.payload.poll.PollFilePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.PollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.PollTabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequest;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequestType;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCloseReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCommitReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHeartbeatReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribePollReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeUnsubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHandshakeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeSubscribeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeUnsubscribeResp;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.SubscriptionSessionConnection;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

final class SubscriptionProvider extends SubscriptionSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionProvider.class);

  private String consumerId;
  private String consumerGroupId;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);
  private final AtomicBoolean isAvailable = new AtomicBoolean(false);

  private final TEndPoint endPoint;
  private int dataNodeId;

  SubscriptionProvider(
      final TEndPoint endPoint,
      final String username,
      final String password,
      final String consumerId,
      final String consumerGroupId,
      final int thriftMaxFrameSize) {
    super(endPoint.ip, endPoint.port, username, password, thriftMaxFrameSize);

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

  String getConsumerId() {
    return consumerId;
  }

  String getConsumerGroupId() {
    return consumerGroupId;
  }

  TEndPoint getEndPoint() {
    return endPoint;
  }

  SubscriptionSessionConnection getSessionConnection() {
    return (SubscriptionSessionConnection) defaultSessionConnection;
  }

  /////////////////////////////// open & close ///////////////////////////////

  synchronized void handshake() throws SubscriptionException, IoTDBConnectionException {
    if (!isClosed.get()) {
      return;
    }

    super.open(); // throw IoTDBConnectionException

    // TODO: pass the complete consumer parameter configuration to the server
    final Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);

    final PipeSubscribeHandshakeResp resp =
        handshake(new ConsumerConfig(consumerAttributes)); // throw SubscriptionException
    dataNodeId = resp.getDataNodeId();
    consumerId = resp.getConsumerId();
    consumerGroupId = resp.getConsumerGroupId();

    isClosed.set(false);
    setAvailable();
  }

  PipeSubscribeHandshakeResp handshake(final ConsumerConfig consumerConfig)
      throws SubscriptionException {
    final PipeSubscribeHandshakeReq req;
    try {
      req = PipeSubscribeHandshakeReq.toTPipeSubscribeReq(consumerConfig);
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when SubscriptionProvider {} serialize handshake request {}",
          this,
          consumerConfig,
          e);
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(req);
    } catch (final TException e) {
      // Assume provider unavailable
      LOGGER.warn(
          "TException occurred when SubscriptionProvider {} handshake with request {}, set SubscriptionProvider unavailable",
          this,
          consumerConfig,
          e);
      setUnavailable();
      throw new SubscriptionConnectionException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
    return PipeSubscribeHandshakeResp.fromTPipeSubscribeResp(resp);
  }

  @Override
  public synchronized void close() throws SubscriptionException, IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      closeInternal(); // throw SubscriptionException
    } finally {
      super.close(); // throw IoTDBConnectionException
      setUnavailable();
      isClosed.set(true);
    }
  }

  void closeInternal() throws SubscriptionException {
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(PipeSubscribeCloseReq.toTPipeSubscribeReq());
    } catch (final TException e) {
      // Assume provider unavailable
      LOGGER.warn(
          "TException occurred when SubscriptionProvider {} close, set SubscriptionProvider unavailable",
          this,
          e);
      setUnavailable();
      throw new SubscriptionConnectionException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
  }

  /////////////////////////////// subscription APIs ///////////////////////////////

  Map<String, TopicConfig> heartbeat() throws SubscriptionException {
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(PipeSubscribeHeartbeatReq.toTPipeSubscribeReq());
    } catch (final TException e) {
      // Assume provider unavailable
      LOGGER.warn(
          "TException occurred when SubscriptionProvider {} heartbeat, set SubscriptionProvider unavailable",
          this,
          e);
      setUnavailable();
      throw new SubscriptionConnectionException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
    final PipeSubscribeHeartbeatResp heartbeatResp =
        PipeSubscribeHeartbeatResp.fromTPipeSubscribeResp(resp);
    return heartbeatResp.getTopics();
  }

  Map<String, TopicConfig> subscribe(final Set<String> topicNames) throws SubscriptionException {
    final PipeSubscribeSubscribeReq req;
    try {
      req = PipeSubscribeSubscribeReq.toTPipeSubscribeReq(topicNames);
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when SubscriptionProvider {} serialize subscribe request {}",
          this,
          topicNames,
          e);
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(req);
    } catch (final TException e) {
      // Assume provider unavailable
      LOGGER.warn(
          "TException occurred when SubscriptionProvider {} subscribe with request {}, set SubscriptionProvider unavailable",
          this,
          topicNames,
          e);
      setUnavailable();
      throw new SubscriptionConnectionException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
    final PipeSubscribeSubscribeResp subscribeResp =
        PipeSubscribeSubscribeResp.fromTPipeSubscribeResp(resp);
    return subscribeResp.getTopics();
  }

  Map<String, TopicConfig> unsubscribe(final Set<String> topicNames) throws SubscriptionException {
    final PipeSubscribeUnsubscribeReq req;
    try {
      req = PipeSubscribeUnsubscribeReq.toTPipeSubscribeReq(topicNames);
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when SubscriptionProvider {} serialize unsubscribe request {}",
          this,
          topicNames,
          e);
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(req);
    } catch (final TException e) {
      // Assume provider unavailable
      LOGGER.warn(
          "TException occurred when SubscriptionProvider {} unsubscribe with request {}, set SubscriptionProvider unavailable",
          this,
          topicNames,
          e);
      setUnavailable();
      throw new SubscriptionConnectionException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
    final PipeSubscribeUnsubscribeResp unsubscribeResp =
        PipeSubscribeUnsubscribeResp.fromTPipeSubscribeResp(resp);
    return unsubscribeResp.getTopics();
  }

  List<SubscriptionPollResponse> poll(final Set<String> topicNames) throws SubscriptionException {
    return poll(
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL.getType(),
            new PollPayload(topicNames),
            0L,
            thriftMaxFrameSize));
  }

  List<SubscriptionPollResponse> pollFile(
      final SubscriptionCommitContext commitContext, final long writingOffset)
      throws SubscriptionException {
    return poll(
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL_FILE.getType(),
            new PollFilePayload(commitContext, writingOffset),
            0L,
            thriftMaxFrameSize));
  }

  List<SubscriptionPollResponse> pollTablets(
      final SubscriptionCommitContext commitContext, final int offset)
      throws SubscriptionException {
    return poll(
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL_TABLETS.getType(),
            new PollTabletsPayload(commitContext, offset),
            0L,
            thriftMaxFrameSize));
  }

  List<SubscriptionPollResponse> poll(final SubscriptionPollRequest pollMessage)
      throws SubscriptionException {
    final PipeSubscribePollReq req;
    try {
      req = PipeSubscribePollReq.toTPipeSubscribeReq(pollMessage);
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when SubscriptionProvider {} serialize poll request {}",
          this,
          pollMessage,
          e);
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(req);
    } catch (final TException e) {
      // Assume provider unavailable
      LOGGER.warn(
          "TException occurred when SubscriptionProvider {} poll with request {}, set SubscriptionProvider unavailable",
          this,
          pollMessage,
          e);
      setUnavailable();
      throw new SubscriptionConnectionException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
    final PipeSubscribePollResp pollResp = PipeSubscribePollResp.fromTPipeSubscribeResp(resp);
    return pollResp.getResponses();
  }

  void commit(final List<SubscriptionCommitContext> subscriptionCommitContexts, final boolean nack)
      throws SubscriptionException {
    final PipeSubscribeCommitReq req;
    try {
      req = PipeSubscribeCommitReq.toTPipeSubscribeReq(subscriptionCommitContexts, nack);
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when SubscriptionProvider {} serialize commit request {}",
          this,
          subscriptionCommitContexts,
          e);
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
    final TPipeSubscribeResp resp;
    try {
      resp = getSessionConnection().pipeSubscribe(req);
    } catch (final TException e) {
      // Assume provider unavailable
      LOGGER.warn(
          "TException occurred when SubscriptionProvider {} commit with request {}, set SubscriptionProvider unavailable",
          this,
          subscriptionCommitContexts,
          e);
      setUnavailable();
      throw new SubscriptionConnectionException(e.getMessage(), e);
    }
    verifyPipeSubscribeSuccess(resp.status);
  }

  private static void verifyPipeSubscribeSuccess(final TSStatus status)
      throws SubscriptionException {
    switch (status.code) {
      case 200: // SUCCESS_STATUS
        return;
      case 1902: // SUBSCRIPTION_HANDSHAKE_ERROR
      case 1903: // SUBSCRIPTION_HEARTBEAT_ERROR
      case 1904: // SUBSCRIPTION_POLL_ERROR
      case 1905: // SUBSCRIPTION_COMMIT_ERROR
      case 1906: // SUBSCRIPTION_CLOSE_ERROR
      case 1907: // SUBSCRIPTION_SUBSCRIBE_ERROR
      case 1908: // SUBSCRIPTION_UNSUBSCRIBE_ERROR
        LOGGER.warn(
            "Internal error occurred, status code {}, status message {}",
            status.code,
            status.message);
        throw new SubscriptionRuntimeNonCriticalException(status.message);
      case 1900: // SUBSCRIPTION_VERSION_ERROR
      case 1901: // SUBSCRIPTION_TYPE_ERROR
      case 1909: // SUBSCRIPTION_MISSING_CUSTOMER
      default:
        LOGGER.warn(
            "Internal error occurred, status code {}, status message {}",
            status.code,
            status.message);
        throw new SubscriptionRuntimeCriticalException(status.message);
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
