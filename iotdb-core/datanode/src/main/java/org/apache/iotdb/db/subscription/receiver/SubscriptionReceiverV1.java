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
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.common.PollMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.PollTsFileMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPollMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPollMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
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

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionReceiverV1 implements SubscriptionReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionReceiverV1.class);

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

  @Override
  public PipeSubscribeRequestVersion getVersion() {
    return PipeSubscribeRequestVersion.VERSION_1;
  }

  @Override
  public void handleExit() {
    LOGGER.info(
        "Subscription: remove consumer config {} when handling exit",
        consumerConfigThreadLocal.get());
    consumerConfigThreadLocal.remove();
  }

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

  private TPipeSubscribeResp handlePipeSubscribeHandshake(final PipeSubscribeHandshakeReq req) {
    try {
      return handlePipeSubscribeHandshakeInternal(req);
    } catch (final SubscriptionException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when handshaking: %s, req: %s", e, req);
      LOGGER.warn(exceptionMessage);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HANDSHAKE_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeHandshakeInternal(
      final PipeSubscribeHandshakeReq req) throws SubscriptionException {
    // set consumer config thread local
    final ConsumerConfig existedConsumerConfig = consumerConfigThreadLocal.get();
    final ConsumerConfig consumerConfig = req.getConsumerConfig();

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
    if (!SubscriptionAgent.consumer()
        .isConsumerExisted(consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId())) {
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
    return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS, dataNodeId);
  }

  private TPipeSubscribeResp handlePipeSubscribeHeartbeat(final PipeSubscribeHeartbeatReq req) {
    try {
      return handlePipeSubscribeHeartbeatInternal(req);
    } catch (final SubscriptionException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when heartbeat: %s, req: %s", e, req);
      LOGGER.warn(exceptionMessage);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HEARTBEAT_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeHeartbeatInternal(
      final PipeSubscribeHeartbeatReq req) {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeHeartbeatReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // TODO: do something

    LOGGER.info("Subscription: consumer {} heartbeat successfully", consumerConfig);
    return PipeSubscribeHeartbeatResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeSubscribe(final PipeSubscribeSubscribeReq req) {
    try {
      return handlePipeSubscribeSubscribeInternal(req);
    } catch (final SubscriptionException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when subscribing: %s, req: %s", e, req);
      LOGGER.warn(exceptionMessage);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_SUBSCRIBE_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeSubscribeInternal(
      final PipeSubscribeSubscribeReq req) {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeSubscribeReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // subscribe topics
    Set<String> topicNames = req.getTopicNames();
    topicNames = topicNames.stream().map(ASTVisitor::parseIdentifier).collect(Collectors.toSet());
    subscribe(consumerConfig, topicNames);

    LOGGER.info("Subscription: consumer {} subscribe {} successfully", consumerConfig, topicNames);
    return PipeSubscribeSubscribeResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeUnsubscribe(final PipeSubscribeUnsubscribeReq req) {
    try {
      return handlePipeSubscribeUnsubscribeInternal(req);
    } catch (final SubscriptionException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when unsubscribing: %s, req: %s",
              e, req);
      LOGGER.warn(exceptionMessage);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_UNSUBSCRIBE_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeUnsubscribeInternal(
      final PipeSubscribeUnsubscribeReq req) {
    // check consumer config thread local
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeUnsubscribeReq: {}",
          req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // unsubscribe topics
    Set<String> topicNames = req.getTopicNames();
    topicNames = topicNames.stream().map(ASTVisitor::parseIdentifier).collect(Collectors.toSet());
    unsubscribe(consumerConfig, topicNames);

    LOGGER.info(
        "Subscription: consumer {} unsubscribe {} successfully", consumerConfig, topicNames);
    return PipeSubscribeUnsubscribeResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribePoll(final PipeSubscribePollReq req) {
    final ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribePollReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    try {
      final SubscriptionPollMessage pollMessage = req.getPollMessage();
      final short messageType = pollMessage.getMessageType();

      if (SubscriptionPollMessageType.isValidatedMessageType(messageType)) {
        switch (SubscriptionPollMessageType.valueOf(messageType)) {
          case POLL:
            return handlePipeSubscribePollInternal(
                consumerConfig, (PollMessagePayload) pollMessage.getMessagePayload());
          case POLL_TS_FILE:
            return handlePipeSubscribePollTsFileInternal(
                consumerConfig, (PollTsFileMessagePayload) pollMessage.getMessagePayload());
          default:
            break;
        }
      }
      throw new SubscriptionException(String.format("unexpected message type: %s", messageType));
    } catch (final SubscriptionException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when polling: %s, req: %s", e, req);
      LOGGER.warn(exceptionMessage);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_POLL_ERROR, exceptionMessage));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribePollInternal(
      final ConsumerConfig consumerConfig, final PollMessagePayload messagePayload) {
    Set<String> topicNames = messagePayload.getTopicNames();
    if (topicNames.isEmpty()) {
      // poll all subscribed topics
      topicNames =
          SubscriptionAgent.consumer()
              .getTopicsSubscribedByConsumer(
                  consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId());
    } else {
      topicNames = topicNames.stream().map(ASTVisitor::parseIdentifier).collect(Collectors.toSet());
    }

    // poll
    final List<SubscriptionEvent> events =
        SubscriptionAgent.broker().poll(consumerConfig, topicNames);

    final List<SubscriptionPolledMessage> polledMessages =
        events.stream().map(SubscriptionEvent::getMessage).collect(Collectors.toList());

    final List<SubscriptionCommitContext> commitContexts =
        polledMessages.stream()
            .map(SubscriptionPolledMessage::getCommitContext)
            .collect(Collectors.toList());

    LOGGER.info(
        "Subscription: consumer {} poll topics {} successfully, commit contexts: {}",
        consumerConfig,
        topicNames,
        commitContexts);

    // generate response
    return PipeSubscribePollResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS, polledMessages);
  }

  private TPipeSubscribeResp handlePipeSubscribePollTsFileInternal(
      final ConsumerConfig consumerConfig, final PollTsFileMessagePayload messagePayload) {
    // poll
    final List<SubscriptionEvent> events =
        SubscriptionAgent.broker()
            .pollTsFile(
                consumerConfig,
                messagePayload.getTopicName(),
                messagePayload.getFileName(),
                messagePayload.getWritingOffset());

    final List<SubscriptionPolledMessage> polledMessages =
        events.stream().map(SubscriptionEvent::getMessage).collect(Collectors.toList());

    final List<SubscriptionCommitContext> commitContexts =
        polledMessages.stream()
            .map(SubscriptionPolledMessage::getCommitContext)
            .collect(Collectors.toList());

    LOGGER.info(
        "Subscription: consumer {} poll TsFile (topic name: {}, file name: {}, writing offset: {}) successfully, commit contexts: {}",
        consumerConfig,
        messagePayload.getTopicName(),
        messagePayload.getFileName(),
        messagePayload.getWritingOffset(),
        commitContexts);

    return PipeSubscribePollResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS, polledMessages);
  }

  private TPipeSubscribeResp handlePipeSubscribeCommit(final PipeSubscribeCommitReq req) {
    try {
      return handlePipeSubscribeCommitInternal(req);
    } catch (final SubscriptionException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when committing: %s, req: %s", e, req);
      LOGGER.warn(exceptionMessage);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
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

    // commit
    final List<SubscriptionCommitContext> commitContexts = req.getCommitContexts();
    final List<SubscriptionCommitContext> successfulCommitContexts =
        SubscriptionAgent.broker().commit(consumerConfig, commitContexts);

    if (Objects.equals(successfulCommitContexts.size(), commitContexts.size())) {
      LOGGER.info(
          "Subscription: consumer {} commit successfully, commit contexts: {}",
          consumerConfig,
          commitContexts);
    } else {
      LOGGER.warn(
          "Subscription: consumer {} commit partially successful, commit contexts: {}, successful commit contexts: {}",
          consumerConfig,
          commitContexts,
          successfulCommitContexts);
    }

    return PipeSubscribeCommitResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeClose(final PipeSubscribeCloseReq req) {
    try {
      return handlePipeSubscribeCloseInternal(req);
    } catch (final SubscriptionException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: something unexpected happened when closing: %s, req: %s", e, req);
      LOGGER.warn(exceptionMessage);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
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

    // unsubscribe all subscribed topics
    final Set<String> topics =
        SubscriptionAgent.consumer()
            .getTopicsSubscribedByConsumer(
                consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId());
    if (!topics.isEmpty()) {
      LOGGER.info(
          "Subscription: unsubscribe all subscribed topics {} before close consumer {}",
          topics,
          consumerConfig);
      unsubscribe(consumerConfig, topics);
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
    return PipeSubscribeCloseResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
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
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to create consumer %s in config node, status is %s.",
                consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (final ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to create consumer %s in config node, exception is %s.",
              consumerConfig, e);
      LOGGER.warn(exceptionMessage);
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
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to close consumer %s in config node, status is %s.",
                consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (final ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to close consumer %s in config node, exception is %s.",
              consumerConfig, e);
      LOGGER.warn(exceptionMessage);
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
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to subscribe topics %s for consumer %s in config node, status is %s.",
                topicNames, consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (final ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to subscribe topics %s for consumer %s in config node, exception is %s.",
              topicNames, consumerConfig, e);
      LOGGER.warn(exceptionMessage);
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
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to unsubscribe topics %s for consumer %s in config node, status is %s.",
                topicNames, consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (final ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to unsubscribe topics %s for consumer %s in config node, exception is %s.",
              topicNames, consumerConfig, e);
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
    }
  }
}
