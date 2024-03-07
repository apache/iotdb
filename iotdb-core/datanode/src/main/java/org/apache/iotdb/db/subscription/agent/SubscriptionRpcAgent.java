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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCloseReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCommitReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHeartbeatReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribePollReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

public class SubscriptionRpcAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionRpcAgent.class);

  private final ThreadLocal<ConsumerConfig> consumerConfigThreadLocal = new ThreadLocal<>();

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  public final TPipeSubscribeResp handle(TPipeSubscribeReq req) {
    // TODO: handle request version
    final byte reqVersion = req.getVersion();
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
    LOGGER.warn("Unknown PipeSubscribeRequestType, response status = {}.", status);
    return new TPipeSubscribeResp(
        status,
        PipeSubscribeResponseVersion.VERSION_1.getVersion(),
        PipeSubscribeResponseType.ACK.getType());
  }

  private TPipeSubscribeResp handlePipeSubscribeHandshake(PipeSubscribeHandshakeReq req) {
    // set consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      consumerConfigThreadLocal.set(req.getConsumerConfig());
      LOGGER.info(
          "Subscription: consumer handshake successfully, consumer config: {}",
          req.getConsumerConfig());
    } else if (!consumerConfig.equals(req.getConsumerConfig())) {
      // TODO: corner case
      LOGGER.warn(
          "Subscription: inconsistent consumer config, old consumer config: {}, new consumer config: {}",
          consumerConfig,
          req.getConsumerConfig());
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(
              TSStatusCode.SUBSCRIPTION_HANDSHAKE_ERROR, "inconsistent consumer config"));
    }

    // create consumer (group)
    SubscriptionAgent.consumer().createConsumer(req.getConsumerConfig());

    // tmp
    SubscriptionAgent.topic().createTopic(new TopicConfig("demo", "root.**"));

    // get DN configs
    // TODO: cached and listen changes
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TDataNodeConfigurationResp dataNodeConfigurationResp =
          configNodeClient.getDataNodeConfiguration(-1);
      Map<Integer, TEndPoint> endPoints =
          dataNodeConfigurationResp.dataNodeConfigurationMap.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Entry::getKey, entry -> entry.getValue().location.clientRpcEndPoint));
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS, endPoints);
    } catch (ClientManagerException | TException e) {
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HANDSHAKE_ERROR, e.getMessage()));
    }
  }

  private TPipeSubscribeResp handlePipeSubscribeHeartbeat(PipeSubscribeHeartbeatReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn("Subscription: unknown consumer config");
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HEARTBEAT_ERROR, "unknown consumer config"));
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
      LOGGER.warn("Subscription: unknown consumer config");
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_SUBSCRIBE_ERROR, "unknown consumer config"));
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
      LOGGER.warn("Subscription: unknown consumer config");
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(
              TSStatusCode.SUBSCRIPTION_UNSUBSCRIBE_ERROR, "unknown consumer config"));
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
      LOGGER.warn("Subscription: unknown consumer config");
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_POLL_ERROR, "unknown consumer config"));
    }

    // poll
    List<EnrichedTablets> enrichedTabletsList = new ArrayList<>();
    for (EnrichedTablets enrichedTablets : SubscriptionAgent.broker().poll(consumerConfig)) {
      // TODO: limit
      enrichedTabletsList.add(enrichedTablets);
    }

    LOGGER.info("Subscription: consumer poll successfully, consumer config: {}", consumerConfig);
    return PipeSubscribePollResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS, enrichedTabletsList);
  }

  private TPipeSubscribeResp handlePipeSubscribeCommit(PipeSubscribeCommitReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn("Subscription: unknown consumer config");
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_COMMIT_ERROR, "unknown consumer config"));
    }

    // commit
    List<Pair<String, Long>> committerKeyAndCommitIds = req.getCommitterKeyAndCommitIds();
    SubscriptionAgent.broker().commit(consumerConfig, committerKeyAndCommitIds);

    LOGGER.info(
        "Subscription: consumer commit {} successfully, consumer config: {}",
        committerKeyAndCommitIds,
        consumerConfig);
    return PipeSubscribeCommitResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }

  private TPipeSubscribeResp handlePipeSubscribeClose(PipeSubscribeCloseReq req) {
    // check consumer config thread local
    ConsumerConfig consumerConfig = consumerConfigThreadLocal.get();
    if (Objects.isNull(consumerConfig)) {
      LOGGER.warn("Subscription: unknown consumer config");
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_CLOSE_ERROR, "unknown consumer config"));
    }

    // drop consumer (group)
    SubscriptionAgent.consumer().dropConsumer(consumerConfig);

    LOGGER.info("Subscription: consumer close successfully, consumer config: {}", consumerConfig);
    return PipeSubscribeCloseResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
  }
}
