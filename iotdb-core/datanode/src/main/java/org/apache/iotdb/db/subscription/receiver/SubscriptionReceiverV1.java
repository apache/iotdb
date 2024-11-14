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

package org.apache.iotdb.db.subscription.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.metric.SubscriptionPrefetchingQueueMetrics;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionPayloadExceedException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionPipeTimeoutException;
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
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeUnsubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeCloseResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeCommitResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHandshakeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseVersion;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeSubscribeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeUnsubscribeResp;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.subscription.util.PollTimer;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SubscriptionReceiverV1 implements SubscriptionReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionReceiverV1.class);

  private static final double POLL_PAYLOAD_SIZE_EXCEED_THRESHOLD = 0.95;

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private static final TPipeSubscribeResp SUBSCRIPTION_MISSING_CUSTOMER_RESP =
      new TPipeSubscribeResp(
          RpcUtils.getStatus(
              TSStatusCode.SUBSCRIPTION_MISSING_CUSTOMER,
              "Missing consumer config, please handshake first."),
          PipeSubscribeResponseVersion.VERSION_1.getVersion(),
          PipeSubscribeResponseType.ACK.getType());

  private final ThreadLocal<ConsumerConfig> consumerConfigThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<PollTimer> pollTimerThreadLocal = new ThreadLocal<>();

  @Override
  public final TPipeSubscribeResp handle(final TPipeSubscribeReq req) {
    final short reqType = req.getType();
    if (PipeSubscribeRequestType.isValidatedRequestType(reqType)) {
      switch (PipeSubscribeRequestType.valueOf(reqType)) {
        case HANDSHAKE:
          return handlePipeSubscribeHandshake(PipeSubscribeHandshakeReq.fromTPipeSubscribeReq(req));
        case HEARTBEAT:
          return handlePipeSubscribeHeartbeat(PipeSubscribeHeartbeatReq.fromTPipeSubscribeReq(req));
        case SUBSCRIBE:
          return handlePipeSubscribeSubscribe(PipeSubscribeSubscribeReq.fromTPipeSubscribeReq(req));
        case UNSUBSCRIBE:
          return handlePipeSubscribeUnsubscribe(
              PipeSubscribeUnsubscribeReq.fromTPipeSubscribeReq(req));
        case POLL:
          return handlePipeSubscribePoll(PipeSubscribePollReq.fromTPipeSubscribeReq(req));
        case COMMIT:
          return handlePipeSubscribeCommit(PipeSubscribeCommitReq.fromTPipeSubscribeReq(req));
        case CLOSE:
          return handlePipeSubscribeClose(PipeSubscribeCloseReq.fromTPipeSubscribeReq(req));
        default:
          break;
      }
    }

    final TSStatus status =
        RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_TYPE_ERROR,
            String.format("Unknown PipeSubscribeRequestType %s.", reqType));
    LOGGER.warn("Subscription: Unknown PipeSubscribeRequestType, response status = {}.", status);
    return new TPipeSubscribeResp(
        status,
        PipeSubscribeResponseVersion.VERSION_1.getVersion(),
        PipeSubscribeResponseType.ACK.getType());
  }

  @Override
  public PipeSubscribeRequestVersion getVersion() {
    return PipeSubscribeRequestVersion.VERSION_1;
  }

  @Override
  public void handleExit() {
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.nonNull(consumerConfig)) {
      LOGGER.info(
          "Subscription: remove consumer config {} when handling exit",
          consumerConfigThreadLocal.get());
      // closeConsumer(consumerConfig);
      consumerConfigThreadLocal.remove();
    }
  }

  @Override
  public long remainingMs() {
    final PollTimer pollTimer = pollTimerThreadLocal.get();
    if (Objects.isNull(pollTimer)) {
      return SubscriptionConfig.getInstance().getSubscriptionDefaultTimeoutInMs();
    }
    return pollTimer.remainingMs();
  }

  private TPipeSubscribeResp handlePipeSubscribeHandshake(final PipeSubscribeHandshakeReq req) {
    try {
      return handlePipeSubscribeHandshakeInternal(req);
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when handshaking with request {}", req, e);
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when handshaking with request %s: %s",
              req, e);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HANDSHAKE_ERROR, exceptionMessage),
          -1,
          "",
          "");
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeHandshakeInternal(
      final PipeSubscribeHandshakeReq req) throws SubscriptionException {
    // set consumer config thread local
    final ConsumerConfig existedConsumerConfig = consumerConfigThreadLocal.get();
    final ConsumerConfig consumerConfig = req.getConsumerConfig();

    String consumerId = consumerConfig.getConsumerId();
    if (Objects.isNull(consumerId)) {
      consumerId = UUID.randomUUID().toString();
      consumerConfig.setConsumerId(consumerId);
    }
    String consumerGroupId = consumerConfig.getConsumerGroupId();
    if (Objects.isNull(consumerGroupId)) {
      consumerGroupId = UUID.randomUUID().toString();
      consumerConfig.setConsumerGroupId(consumerGroupId);
    }

    if (Objects.isNull(existedConsumerConfig)) {
      consumerConfigThreadLocal.set(consumerConfig);
    } else {
      if (!existedConsumerConfig.equals(consumerConfig)) {
        LOGGER.warn(
            "Subscription: Detect stale consumer config when handshaking, stale consumer config {} will be cleared, consumer config will set to the incoming consumer config {}.",
            existedConsumerConfig,
            consumerConfig);
        // drop stale consumer
        dropConsumer(existedConsumerConfig);
        consumerConfigThreadLocal.set(consumerConfig);
      }
    }

    // create consumer if not existed
    if (!SubscriptionAgent.consumer().isConsumerExisted(consumerGroupId, consumerId)) {
      createConsumer(consumerConfig);
    } else {
      LOGGER.info(
          "Subscription: The consumer {} has already existed when handshaking, skip creating consumer.",
          consumerConfig);
    }

    final int dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    LOGGER.info(
        "Subscription: consumer {} handshake successfully, data node id: {}",
        req.getConsumerConfig(),
        dataNodeId);
    return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
        RpcUtils.SUCCESS_STATUS, dataNodeId, consumerId, consumerGroupId);
  }

  private TPipeSubscribeResp handlePipeSubscribeHeartbeat(final PipeSubscribeHeartbeatReq req) {
    try {
      return handlePipeSubscribeHeartbeatInternal(req);
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when heartbeat with request {}", req, e);
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when heartbeat with request %s: %s",
              req, e);
      return PipeSubscribeHeartbeatResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HEARTBEAT_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeHeartbeatInternal(
      final PipeSubscribeHeartbeatReq req) throws IOException {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeHeartbeatReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // TODO: do something

    LOGGER.info("Subscription: consumer {} heartbeat successfully", consumerConfig);
    return PipeSubscribeHeartbeatResp.toTPipeSubscribeResp(
        RpcUtils.SUCCESS_STATUS,
        SubscriptionAgent.topic()
            .getTopicConfigs(
                SubscriptionAgent.consumer()
                    .getTopicNamesSubscribedByConsumer(
                        consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId())));
  }

  private TPipeSubscribeResp handlePipeSubscribeSubscribe(final PipeSubscribeSubscribeReq req) {
    try {
      return handlePipeSubscribeSubscribeInternal(req);
    } catch (final SubscriptionPipeTimeoutException e) {
      return PipeSubscribeSubscribeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR, e.getMessage()));
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when subscribing with request {}", req, e);
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when subscribing with request %s: %s",
              req, e);
      return PipeSubscribeSubscribeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_SUBSCRIBE_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeSubscribeInternal(
      final PipeSubscribeSubscribeReq req) throws SubscriptionException, IOException {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeSubscribeReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // subscribe topics
    final Set<String> topicNames = req.getTopicNames();
    subscribe(consumerConfig, topicNames);

    LOGGER.info("Subscription: consumer {} subscribe {} successfully", consumerConfig, topicNames);
    return PipeSubscribeSubscribeResp.toTPipeSubscribeResp(
        RpcUtils.SUCCESS_STATUS,
        SubscriptionAgent.topic()
            .getTopicConfigs(
                SubscriptionAgent.consumer()
                    .getTopicNamesSubscribedByConsumer(
                        consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId())));
  }

  private TPipeSubscribeResp handlePipeSubscribeUnsubscribe(final PipeSubscribeUnsubscribeReq req) {
    try {
      return handlePipeSubscribeUnsubscribeInternal(req);
    } catch (final SubscriptionPipeTimeoutException e) {
      return PipeSubscribeSubscribeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR, e.getMessage()));
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when unsubscribing with request {}", req, e);
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when unsubscribing with request %s: %s",
              req, e);
      return PipeSubscribeUnsubscribeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_UNSUBSCRIBE_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeUnsubscribeInternal(
      final PipeSubscribeUnsubscribeReq req) throws IOException {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeUnsubscribeReq: {}",
          req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // unsubscribe topics
    final Set<String> topicNames = req.getTopicNames();
    unsubscribe(consumerConfig, topicNames);

    LOGGER.info(
        "Subscription: consumer {} unsubscribe {} successfully", consumerConfig, topicNames);
    return PipeSubscribeUnsubscribeResp.toTPipeSubscribeResp(
        RpcUtils.SUCCESS_STATUS,
        SubscriptionAgent.topic()
            .getTopicConfigs(
                SubscriptionAgent.consumer()
                    .getTopicNamesSubscribedByConsumer(
                        consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId())));
  }

  private TPipeSubscribeResp handlePipeSubscribePoll(final PipeSubscribePollReq req) {
    try {
      return handlePipeSubscribePollInternal(req);
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when polling with request {}", req, e);
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when polling with request %s: %s",
              req, e);
      return PipeSubscribePollResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_POLL_ERROR, exceptionMessage),
          Collections.emptyList());
    } finally {
      pollTimerThreadLocal.remove();
    }
  }

  private TPipeSubscribeResp handlePipeSubscribePollInternal(final PipeSubscribePollReq req)
      throws SubscriptionException {
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribePollReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    final List<SubscriptionEvent> events;
    final SubscriptionPollRequest request = req.getRequest();

    pollTimerThreadLocal.set(new PollTimer(System.currentTimeMillis(), request.getTimeoutMs()));

    final long maxBytes = (long) (request.getMaxBytes() * POLL_PAYLOAD_SIZE_EXCEED_THRESHOLD);
    final short requestType = request.getRequestType();
    if (SubscriptionPollRequestType.isValidatedRequestType(requestType)) {
      switch (SubscriptionPollRequestType.valueOf(requestType)) {
        case POLL:
          events =
              handlePipeSubscribePollRequest(
                  consumerConfig, (PollPayload) request.getPayload(), maxBytes);
          break;
        case POLL_FILE:
          events =
              handlePipeSubscribePollTsFileRequest(
                  consumerConfig, (PollFilePayload) request.getPayload());
          break;
        case POLL_TABLETS:
          events =
              handlePipeSubscribePollTabletsRequest(
                  consumerConfig, (PollTabletsPayload) request.getPayload());
          break;
        default:
          events = null;
          break;
      }
    } else {
      events = null;
    }

    if (Objects.isNull(events)) {
      throw new SubscriptionException(String.format("unexpected request type: %s", requestType));
    }

    // generate response
    final AtomicLong totalSize = new AtomicLong();
    return PipeSubscribePollResp.toTPipeSubscribeResp(
        RpcUtils.SUCCESS_STATUS,
        events.stream()
            .map(
                (event) -> {
                  final SubscriptionCommitContext commitContext = event.getCommitContext();
                  final SubscriptionPollResponse response = event.getCurrentResponse();
                  if (Objects.isNull(response)) {
                    LOGGER.warn(
                        "Subscription: consumer {} poll null response for event {} with request: {}",
                        consumerConfig,
                        event,
                        req.getRequest());
                    // nack
                    SubscriptionAgent.broker()
                        .commit(consumerConfig, Collections.singletonList(commitContext), true);
                    return null;
                  }

                  try {
                    final ByteBuffer byteBuffer = event.getCurrentResponseByteBuffer();

                    // payload size control
                    final long size = event.getCurrentResponseSize();
                    if (totalSize.get() + size > maxBytes) {
                      throw new SubscriptionPayloadExceedException(
                          String.format(
                              "payload size %s byte(s) will exceed the threshold %s byte(s)",
                              totalSize.get() + size, maxBytes));
                    }
                    totalSize.getAndAdd(size);

                    SubscriptionPrefetchingQueueMetrics.getInstance()
                        .mark(
                            SubscriptionPrefetchingQueue.generatePrefetchingQueueId(
                                commitContext.getConsumerGroupId(), commitContext.getTopicName()),
                            size);
                    event.invalidateCurrentResponseByteBuffer();
                    LOGGER.info(
                        "Subscription: consumer {} poll {} successfully with request: {}",
                        consumerConfig,
                        response,
                        req.getRequest());
                    return byteBuffer;
                  } catch (final Exception e) {
                    if (e instanceof SubscriptionPayloadExceedException) {
                      LOGGER.error(
                          "Subscription: consumer {} poll excessive payload {} with request: {}, something unexpected happened with parameter configuration or payload control...",
                          consumerConfig,
                          response,
                          req.getRequest(),
                          e);
                    } else {
                      LOGGER.warn(
                          "Subscription: consumer {} poll {} failed with request: {}",
                          consumerConfig,
                          response,
                          req.getRequest(),
                          e);
                    }
                    // nack
                    SubscriptionAgent.broker()
                        .commit(consumerConfig, Collections.singletonList(commitContext), true);
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));
  }

  private List<SubscriptionEvent> handlePipeSubscribePollRequest(
      final ConsumerConfig consumerConfig, final PollPayload messagePayload, final long maxBytes) {
    final Set<String> subscribedTopicNames =
        SubscriptionAgent.consumer()
            .getTopicNamesSubscribedByConsumer(
                consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId());
    final Set<String> topicNames = messagePayload.getTopicNames();
    if (topicNames.isEmpty()) {
      return Collections.emptyList();
    }

    // filter unsubscribed topics
    topicNames.removeIf((topicName) -> !subscribedTopicNames.contains(topicName));
    return SubscriptionAgent.broker().poll(consumerConfig, topicNames, maxBytes);
  }

  private List<SubscriptionEvent> handlePipeSubscribePollTsFileRequest(
      final ConsumerConfig consumerConfig, final PollFilePayload messagePayload) {
    return SubscriptionAgent.broker()
        .pollTsFile(
            consumerConfig, messagePayload.getCommitContext(), messagePayload.getWritingOffset());
  }

  private List<SubscriptionEvent> handlePipeSubscribePollTabletsRequest(
      final ConsumerConfig consumerConfig, final PollTabletsPayload messagePayload) {
    return SubscriptionAgent.broker()
        .pollTablets(consumerConfig, messagePayload.getCommitContext(), messagePayload.getOffset());
  }

  private TPipeSubscribeResp handlePipeSubscribeCommit(final PipeSubscribeCommitReq req) {
    try {
      return handlePipeSubscribeCommitInternal(req);
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when committing with request {}", req, e);
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when committing with request %s: %s",
              req, e);
      return PipeSubscribeCommitResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_COMMIT_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeCommitInternal(final PipeSubscribeCommitReq req) {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeCommitReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // commit (ack or nack)
    final List<SubscriptionCommitContext> commitContexts = req.getCommitContexts();
    final boolean nack = req.isNack();
    final List<SubscriptionCommitContext> successfulCommitContexts =
        SubscriptionAgent.broker().commit(consumerConfig, commitContexts, nack);

    if (Objects.equals(successfulCommitContexts.size(), commitContexts.size())) {
      LOGGER.info(
          "Subscription: consumer {} commit (nack: {}) successfully, commit contexts: {}",
          consumerConfig,
          nack,
          commitContexts);
    } else {
      LOGGER.warn(
          "Subscription: consumer {} commit (nack: {}) partially successful, commit contexts: {}, successful commit contexts: {}",
          consumerConfig,
          nack,
          commitContexts,
          successfulCommitContexts);
    }

    return PipeSubscribeCommitResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeClose(final PipeSubscribeCloseReq req) {
    try {
      return handlePipeSubscribeCloseInternal(req);
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when closing with request {}", req, e);
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when closing with request %s: %s",
              req, e);
      return PipeSubscribeCloseResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_CLOSE_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeCloseInternal(final PipeSubscribeCloseReq req) {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeCloseReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    closeConsumer(consumerConfig);
    return PipeSubscribeCloseResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private void closeConsumer(final ConsumerConfig consumerConfig) {
    // unsubscribe all subscribed topics
    final Set<String> topicNames =
        SubscriptionAgent.consumer()
            .getTopicNamesSubscribedByConsumer(
                consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId());
    if (!topicNames.isEmpty()) {
      LOGGER.info(
          "Subscription: unsubscribe all subscribed topics {} before close consumer {}",
          topicNames,
          consumerConfig);
      try {
        unsubscribe(consumerConfig, topicNames);
      } catch (final SubscriptionPipeTimeoutException e) {
        LOGGER.warn(e.getMessage());
        // continue drop consumer operation
      } // rethrow other exceptions
    }

    // drop consumer if existed
    if (SubscriptionAgent.consumer()
        .isConsumerExisted(consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId())) {
      dropConsumer(consumerConfig);
    } else {
      LOGGER.info(
          "Subscription: The consumer {} does not existed when closing, skip dropping consumer.",
          consumerConfig);
    }

    LOGGER.info("Subscription: consumer {} close successfully", consumerConfig);
  }

  //////////////////////////// consumer operations ////////////////////////////

  private void createConsumer(final ConsumerConfig consumerConfig) throws SubscriptionException {
    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TCreateConsumerReq req =
          new TCreateConsumerReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId())
              .setConsumerAttributes(consumerConfig.getAttribute());
      final TSStatus tsStatus = configNodeClient.createConsumer(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Unexpected status code {} when creating consumer {} in config node",
            tsStatus,
            consumerConfig);
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to create consumer %s in config node, status is %s.",
                consumerConfig, tsStatus);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (final ClientManagerException | TException e) {
      LOGGER.warn("Exception occurred when creating consumer {} in config node", consumerConfig, e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to create consumer %s in config node, exception is %s.",
              consumerConfig, e);
      throw new SubscriptionException(exceptionMessage);
    }
  }

  private void dropConsumer(final ConsumerConfig consumerConfig) throws SubscriptionException {
    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TCloseConsumerReq req =
          new TCloseConsumerReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId());
      final TSStatus tsStatus = configNodeClient.closeConsumer(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Unexpected status code {} when closing consumer {} in config node",
            tsStatus,
            consumerConfig);
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to close consumer %s in config node, status is %s.",
                consumerConfig, tsStatus);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (final ClientManagerException | TException e) {
      LOGGER.warn("Exception occurred when closing consumer {} in config node", consumerConfig, e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to close consumer %s in config node, exception is %s.",
              consumerConfig, e);
      throw new SubscriptionException(exceptionMessage);
    }

    // TODO: broker TTL if no consumer in consumer group
  }

  private void subscribe(final ConsumerConfig consumerConfig, final Set<String> topicNames)
      throws SubscriptionException {
    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSubscribeReq req =
          new TSubscribeReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId())
              .setTopicNames(topicNames);
      final TSStatus tsStatus = configNodeClient.createSubscription(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Unexpected status code {} when subscribing topics {} for consumer {} in config node",
            tsStatus,
            topicNames,
            consumerConfig);
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to subscribe topics %s for consumer %s in config node, status is %s.",
                topicNames, consumerConfig, tsStatus);
        if (TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR.getStatusCode() == tsStatus.getCode()) {
          throw new SubscriptionPipeTimeoutException(exceptionMessage);
        } else {
          throw new SubscriptionException(exceptionMessage);
        }
      }
    } catch (final ClientManagerException | TException e) {
      LOGGER.warn(
          "Exception occurred when subscribing topics {} for consumer {} in config node",
          topicNames,
          consumerConfig,
          e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to subscribe topics %s for consumer %s in config node, exception is %s.",
              topicNames, consumerConfig, e);
      throw new SubscriptionException(exceptionMessage);
    }
  }

  private void unsubscribe(final ConsumerConfig consumerConfig, final Set<String> topicNames)
      throws SubscriptionException {
    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TUnsubscribeReq req =
          new TUnsubscribeReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId())
              .setTopicNames(topicNames);
      final TSStatus tsStatus = configNodeClient.dropSubscription(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Unexpected status code {} when unsubscribing topics {} for consumer {} in config node",
            tsStatus,
            topicNames,
            consumerConfig);
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to unsubscribe topics %s for consumer %s in config node, status is %s.",
                topicNames, consumerConfig, tsStatus);
        if (TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR.getStatusCode() == tsStatus.getCode()) {
          throw new SubscriptionPipeTimeoutException(exceptionMessage);
        } else {
          throw new SubscriptionException(exceptionMessage);
        }
      }
    } catch (final ClientManagerException | TException e) {
      LOGGER.warn(
          "Exception occurred when unsubscribing topics {} for consumer {} in config node",
          topicNames,
          consumerConfig,
          e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to unsubscribe topics %s for consumer %s in config node, exception is %s.",
              topicNames, consumerConfig, e);
      throw new SubscriptionException(exceptionMessage);
    }
  }
}
