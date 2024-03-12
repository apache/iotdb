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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCloseReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCommitReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHeartbeatReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribePollReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeUnsubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.request.TopicConfig;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
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
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

public class SubscriptionReceiverV1 implements SubscriptionReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionReceiverV1.class);

  private final ThreadLocal<ConsumerConfig> consumerConfigThreadLocal = new ThreadLocal<>();

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private static final TPipeSubscribeResp SUBSCRIPTION_MISSING_CUSTOMER_RESP =
      new TPipeSubscribeResp(
          RpcUtils.getStatus(
              TSStatusCode.SUBSCRIPTION_MISSING_CUSTOMER,
              "Missing consumer config, please handshake first."),
          PipeSubscribeResponseVersion.VERSION_1.getVersion(),
          PipeSubscribeResponseType.ACK.getType());

  @Override
  public PipeSubscribeRequestVersion getVersion() {
    return PipeSubscribeRequestVersion.VERSION_1;
  }

  @Override
  public final TPipeSubscribeResp handle(TPipeSubscribeReq req) {
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

  private TPipeSubscribeResp handlePipeSubscribeHandshake(PipeSubscribeHandshakeReq req) {
    // set consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    final boolean isConsumerExisted;
    if (Objects.isNull(consumerConfig)) {
      consumerConfigThreadLocal.set(req.getConsumerConfig());
      isConsumerExisted = false;
    } else {
      if (!consumerConfig.equals(req.getConsumerConfig())) {
        LOGGER.warn(
            "Subscription: Detect stale consumer config when handshaking, stale consumer config {} will be cleared, consumer config will set to the incoming consumer config {}.",
            consumerConfig,
            req.getConsumerConfig());
        // clear stale consumer
        SubscriptionAgent.consumer().dropConsumer(consumerConfig);
        consumerConfigThreadLocal.set(req.getConsumerConfig());
        isConsumerExisted = false;
      } else {
        LOGGER.info(
            "Subscription: Detect the same consumer config {} when handshaking, skip the creation of consumer.",
            consumerConfig);
        isConsumerExisted = true;
      }
    }

    // create consumer (group)
    if (!isConsumerExisted) {
      SubscriptionAgent.consumer().createConsumer(req.getConsumerConfig());
    }

    // TODO: REMOVE ME BEFORE MERGING
    SubscriptionAgent.topic().createTopic(new TopicConfig("demo", "root.**"));

    // fetch DN endPoints by CN
    // TODO: cache result and listen changes
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TDataNodeConfigurationResp dataNodeConfigurationResp =
          configNodeClient.getDataNodeConfiguration(-1);
      Map<Integer, TEndPoint> endPoints =
          dataNodeConfigurationResp.dataNodeConfigurationMap.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Entry::getKey, entry -> entry.getValue().location.clientRpcEndPoint));

      LOGGER.info(
          "Subscription: consumer handshake successfully, consumer config: {}, DN endPoints: {}",
          req.getConsumerConfig(),
          endPoints);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS, endPoints);
    } catch (ClientManagerException | TException e) {
      LOGGER.warn("Subscription: something unexpected happened when handshaking", e);
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HANDSHAKE_ERROR, e.getMessage()));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeHeartbeat(PipeSubscribeHeartbeatReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeHeartbeatReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // TODO: do something

    LOGGER.info(
        "Subscription: consumer heartbeat successfully, consumer config: {}", consumerConfig);
    return PipeSubscribeHeartbeatResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeSubscribe(PipeSubscribeSubscribeReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeSubscribeReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // subscribe topics
    List<String> topicNames = req.getTopicNames();
    SubscriptionAgent.consumer().subscribe(consumerConfig, topicNames);

    LOGGER.info(
        "Subscription: consumer subscribe {} successfully, consumer config: {}",
        topicNames,
        consumerConfig);
    return PipeSubscribeSubscribeResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeUnsubscribe(PipeSubscribeUnsubscribeReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeUnsubscribeReq: {}",
          req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // unsubscribe topics
    List<String> topicNames = req.getTopicNames();
    SubscriptionAgent.consumer().unsubscribe(consumerConfig, topicNames);

    LOGGER.info(
        "Subscription: consumer unsubscribe {} successfully, consumer config: {}",
        topicNames,
        consumerConfig);
    return PipeSubscribeUnsubscribeResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribePoll(PipeSubscribePollReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribePollReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // poll
    List<Pair<ByteBuffer, EnrichedTablets>> enrichedTabletsList =
        SubscriptionAgent.broker().poll(consumerConfig);
    List<ByteBuffer> serializedEnrichedTabletsList =
        enrichedTabletsList.stream().map((pair) -> pair.left).collect(Collectors.toList());
    List<List<String>> subscriptionCommitIdsList =
        enrichedTabletsList.stream()
            .map((pair) -> pair.right.getSubscriptionCommitIds())
            .collect(Collectors.toList());

    LOGGER.info(
        "Subscription: consumer poll successfully, consumer config: {}, commit ids: {}",
        consumerConfig,
        subscriptionCommitIdsList);
    return PipeSubscribePollResp.directToTPipeSubscribeResp(
        RpcUtils.SUCCESS_STATUS, serializedEnrichedTabletsList);
  }

  private TPipeSubscribeResp handlePipeSubscribeCommit(PipeSubscribeCommitReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeCommitReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // commit
    Map<String, List<String>> topicNameToSubscriptionCommitIds =
        req.getTopicNameToSubscriptionCommitIds();
    SubscriptionAgent.broker().commit(consumerConfig, topicNameToSubscriptionCommitIds);

    LOGGER.info(
        "Subscription: consumer commit {} successfully, consumer config: {}",
        topicNameToSubscriptionCommitIds,
        consumerConfig);
    return PipeSubscribeCommitResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeClose(PipeSubscribeCloseReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn(
          "Subscription: missing consumer config when handling PipeSubscribeCloseReq: {}", req);
      return SUBSCRIPTION_MISSING_CUSTOMER_RESP;
    }

    // drop consumer (group)
    SubscriptionAgent.consumer().dropConsumer(consumerConfig);

    LOGGER.info("Subscription: consumer close successfully, consumer config: {}", consumerConfig);
    return PipeSubscribeCloseResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }
}
