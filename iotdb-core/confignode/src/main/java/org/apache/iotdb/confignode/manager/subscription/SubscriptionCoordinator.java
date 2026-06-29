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

package org.apache.iotdb.confignode.manager.subscription;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.read.subscription.ShowSubscriptionPlan;
import org.apache.iotdb.confignode.consensus.request.read.subscription.ShowTopicPlan;
import org.apache.iotdb.confignode.consensus.response.subscription.SubscriptionTableResp;
import org.apache.iotdb.confignode.consensus.response.subscription.TopicTableResp;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinatorLock;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicOwnerLeaseReq;
import org.apache.iotdb.mpp.rpc.thrift.TTopicOwnerLeaseEntry;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionCoordinator.class);

  private final ConfigManager configManager;

  // NEVER EXPOSE THIS DIRECTLY TO THE OUTSIDE
  private final SubscriptionInfo subscriptionInfo;

  private final PipeTaskCoordinatorLock coordinatorLock;
  private AtomicReference<SubscriptionInfo> subscriptionInfoHolder;

  private final SubscriptionMetaSyncer subscriptionMetaSyncer;
  private final SubscriptionOwnerLeaseSyncer subscriptionOwnerLeaseSyncer;
  // topicName -> blockSinceMs (ConfigNode local clock when owner-lease renewal was stopped for an
  // in-flight owner transfer). Used to skip renewal and to bound the admission wait.
  private final Map<String, Long> blockedOwnerLeaseRenewalTopics =
      Collections.synchronizedMap(new HashMap<>());

  public SubscriptionCoordinator(ConfigManager configManager, SubscriptionInfo subscriptionInfo) {
    this.configManager = configManager;
    this.subscriptionInfo = subscriptionInfo;
    this.coordinatorLock = new PipeTaskCoordinatorLock();
    this.subscriptionMetaSyncer = new SubscriptionMetaSyncer(configManager);
    this.subscriptionOwnerLeaseSyncer = new SubscriptionOwnerLeaseSyncer(configManager);
  }

  public SubscriptionInfo getSubscriptionInfo() {
    return subscriptionInfo;
  }

  /////////////////////////////// Lock ///////////////////////////////

  public AtomicReference<SubscriptionInfo> tryLock() {
    if (coordinatorLock.tryLock()) {
      subscriptionInfoHolder = new AtomicReference<>(subscriptionInfo);
      return subscriptionInfoHolder;
    }

    return null;
  }

  /**
   * Lock the {@link SubscriptionInfo} coordinator.
   *
   * @return the {@link SubscriptionInfo} holder, which can be used to get the {@link
   *     SubscriptionInfo}. Wait until lock is acquired
   */
  public AtomicReference<SubscriptionInfo> lock() {
    coordinatorLock.lock();
    subscriptionInfoHolder = new AtomicReference<>(subscriptionInfo);
    return subscriptionInfoHolder;
  }

  public boolean unlock() {
    if (subscriptionInfoHolder != null) {
      subscriptionInfoHolder.set(null);
      subscriptionInfoHolder = null;
    }

    coordinatorLock.unlock();
    return true;
  }

  public boolean isLocked() {
    return coordinatorLock.isLocked();
  }

  /////////////////////////////// Meta sync ///////////////////////////////

  public void startSubscriptionMetaSync() {
    subscriptionMetaSyncer.start();
    subscriptionOwnerLeaseSyncer.start();
  }

  public void stopSubscriptionMetaSync() {
    subscriptionMetaSyncer.stop();
    subscriptionOwnerLeaseSyncer.stop();
  }

  /**
   * Caller should ensure that the method is called in the write lock of {@link #subscriptionInfo}.
   */
  public void updateLastSyncedVersion() {
    subscriptionInfo.updateLastSyncedVersion();
  }

  public boolean canSkipNextSync() {
    return subscriptionInfo.canSkipNextSync();
  }

  /////////////////////////////// Operate ///////////////////////////////

  public TSStatus createTopic(TCreateTopicReq req) {
    final TSStatus status = configManager.getProcedureManager().createTopic(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ManagerMessages.FAILED_TO_CREATE_TOPIC_WITH_ATTRIBUTES_RESULT_STATUS,
          req.getTopicName(),
          req.getTopicAttributes(),
          status);
    }
    return status;
  }

  public TSStatus alterTopic(TAlterTopicReq req) {
    final TSStatus status = configManager.getProcedureManager().alterTopic(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ManagerMessages.FAILED_TO_ALTER_TOPIC_WITH_ATTRIBUTES_RESULT_STATUS,
          req.getTopicName(),
          req.getTopicAttributes(),
          status);
    }
    return status;
  }

  public boolean blockOwnerLeaseRenewalIfOwnerTransfer(TAlterTopicReq req) {
    final TopicMeta currentTopicMeta = subscriptionInfo.deepCopyTopicMeta(req.getTopicName());
    final TopicMeta updatedTopicMeta = buildAlteredTopicMeta(req);
    if (Objects.isNull(currentTopicMeta)
        || Objects.isNull(updatedTopicMeta)
        || Objects.equals(currentTopicMeta.getOwnerId(), updatedTopicMeta.getOwnerId())) {
      return false;
    }

    blockedOwnerLeaseRenewalTopics.put(req.getTopicName(), System.currentTimeMillis());
    return true;
  }

  public void unblockOwnerLeaseRenewal(String topicName) {
    blockedOwnerLeaseRenewalTopics.remove(topicName);
  }

  /**
   * Block until it is safe to install the new owner: every DataNode must have let the old owner's
   * lease expire. Since renewal was stopped at {@code blockSince}, a DataNode's lease can expire as
   * late as {@code blockSince + H + L} (H covers propagation of the last renewal sent just before
   * the block; L is the lease duration). We wait that long measured purely on this ConfigNode's own
   * clock from {@code blockSince} — no cross-node timestamp comparison, so clock skew cannot shrink
   * the window. This replaces any absolute-expire-timestamp comparison.
   */
  public TopicMeta buildAlteredTopicMetaAfterOwnerLeaseExpired(TAlterTopicReq req)
      throws InterruptedException {
    final TopicMeta currentTopicMeta = subscriptionInfo.deepCopyTopicMeta(req.getTopicName());
    final TopicMeta updatedTopicMeta = buildAlteredTopicMeta(req);
    if (Objects.isNull(currentTopicMeta)
        || Objects.isNull(updatedTopicMeta)
        || Objects.equals(currentTopicMeta.getOwnerId(), updatedTopicMeta.getOwnerId())) {
      // Not an owner change: nothing to drain.
      return updatedTopicMeta;
    }

    final Long leaseDurationMs = currentTopicMeta.getOwnerLeaseDurationMs();
    final Long blockSinceMs = blockedOwnerLeaseRenewalTopics.get(req.getTopicName());
    if (Objects.isNull(leaseDurationMs) || Objects.isNull(blockSinceMs)) {
      // No lease configured (no drain to wait for) or renewal not blocked: epoch fencing applies on
      // reachable DataNodes; nothing further to wait on here.
      return updatedTopicMeta;
    }

    final long drainDeadlineMs =
        blockSinceMs + leaseDurationMs + SubscriptionOwnerLeaseSyncer.getHeartbeatIntervalMs();
    long remainingMs;
    while ((remainingMs = drainDeadlineMs - System.currentTimeMillis()) > 0) {
      Thread.sleep(Math.min(remainingMs, 1000L));
    }
    return updatedTopicMeta;
  }

  public TopicMeta buildAlteredTopicMeta(TAlterTopicReq req) {
    return subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(
        req.getTopicName(), req.getTopicAttributes());
  }

  /**
   * Build and push owner-lease renewals to all DataNodes via the dedicated subscription owner
   * heartbeat (independent from the node heartbeat). Topics undergoing an owner transfer are
   * skipped so their lease drains on the DataNodes. Best-effort: a DataNode that misses pushes will
   * let its local lease expire and fence the owner (fail-closed).
   */
  public void pushTopicOwnerLeasesToDataNodes() {
    final Set<String> blockedTopicNames;
    synchronized (blockedOwnerLeaseRenewalTopics) {
      blockedTopicNames = new HashSet<>(blockedOwnerLeaseRenewalTopics.keySet());
    }

    final List<TTopicOwnerLeaseEntry> ownerLeases =
        subscriptionInfo.collectTopicOwnerLeaseEntries(blockedTopicNames);
    if (ownerLeases.isEmpty()) {
      return;
    }

    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    if (dataNodeLocationMap.isEmpty()) {
      return;
    }

    final TPushTopicOwnerLeaseReq request = new TPushTopicOwnerLeaseReq(ownerLeases);
    final DataNodeAsyncRequestContext<TPushTopicOwnerLeaseReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.SUBSCRIPTION_PUSH_OWNER_LEASE, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
  }

  public TSStatus dropTopic(TDropTopicReq req) {
    final String topicName = req.getTopicName();
    final boolean isSetIfExistsCondition =
        req.isSetIfExistsCondition() && req.isIfExistsCondition();
    if (!subscriptionInfo.isTopicExisted(topicName, req.isTableModel)) {
      return isSetIfExistsCondition
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
          : RpcUtils.getStatus(
              TSStatusCode.TOPIC_NOT_EXIST_ERROR,
              String.format(
                  "Failed to drop topic %s. Failures: %s does not exist.", topicName, topicName));
    }
    return configManager.getProcedureManager().dropTopic(topicName);
  }

  public TShowTopicResp showTopic(TShowTopicReq req) {
    try {
      return ((TopicTableResp) configManager.getConsensusManager().read(new ShowTopicPlan()))
          .filter(req.topicName, req.isTableModel)
          .convertToTShowTopicResp();
    } catch (Exception e) {
      LOGGER.warn(ManagerMessages.FAILED_TO_SHOW_TOPIC_INFO, e);
      return new TopicTableResp(
              new TSStatus(TSStatusCode.SHOW_TOPIC_ERROR.getStatusCode())
                  .setMessage(e.getMessage()),
              Collections.emptyList())
          .convertToTShowTopicResp();
    }
  }

  public TGetAllTopicInfoResp getAllTopicInfo() {
    try {
      return ((TopicTableResp) configManager.getConsensusManager().read(new ShowTopicPlan()))
          .convertToTGetAllTopicInfoResp();
    } catch (Exception e) {
      LOGGER.warn(ManagerMessages.FAILED_TO_GET_ALL_TOPIC_INFO, e);
      return new TGetAllTopicInfoResp(
          new TSStatus(TSStatusCode.SHOW_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TSStatus createConsumer(TCreateConsumerReq req) {
    final TSStatus status = configManager.getProcedureManager().createConsumer(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ManagerMessages.FAILED_TO_CREATE_CONSUMER_IN_CONSUMER_GROUP_RESULT_STATUS,
          req.getConsumerId(),
          req.getConsumerGroupId(),
          status);
    }
    return status;
  }

  public TSStatus dropConsumer(TCloseConsumerReq req) {
    final TSStatus status = configManager.getProcedureManager().dropConsumer(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ManagerMessages.FAILED_TO_CLOSE_CONSUMER_IN_CONSUMER_GROUP_RESULT_STATUS,
          req.getConsumerId(),
          req.getConsumerGroupId(),
          status);
    }
    return status;
  }

  public TSStatus createSubscription(TSubscribeReq req) {
    final TSStatus status = configManager.getProcedureManager().createSubscription(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ManagerMessages.CONSUMER_IN_CONSUMER_GROUP_FAILED_TO_SUBSCRIBE_TOPICS_RESULT_STATUS,
          req.getConsumerId(),
          req.getConsumerGroupId(),
          req.getTopicNames(),
          status);
    }
    return status;
  }

  public TSStatus dropSubscription(TUnsubscribeReq req) {
    final TSStatus status = configManager.getProcedureManager().dropSubscription(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ManagerMessages.CONSUMER_IN_CONSUMER_GROUP_FAILED_TO_UNSUBSCRIBE_TOPICS_RESULT_STATUS,
          req.getConsumerId(),
          req.getConsumerGroupId(),
          req.getTopicNames(),
          status);
    }
    return status;
  }

  public TSStatus dropSubscription(TDropSubscriptionReq req) {
    final String subscriptionId = req.getSubsciptionId();
    final boolean isSetIfExistsCondition =
        req.isSetIfExistsCondition() && req.isIfExistsCondition();
    final Optional<Pair<String, String>> topicNameWithConsumerGroupName =
        subscriptionInfo.parseSubscriptionId(subscriptionId, req.isTableModel);
    if (!topicNameWithConsumerGroupName.isPresent()) {
      return isSetIfExistsCondition
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
          : RpcUtils.getStatus(
              TSStatusCode.TOPIC_NOT_EXIST_ERROR,
              String.format(
                  "Failed to drop subscription %s. Failures: %s does not exist.",
                  subscriptionId, subscriptionId));
    }
    return configManager
        .getProcedureManager()
        .dropSubscription(
            new TUnsubscribeReq()
                .setConsumerId(null)
                .setConsumerGroupId(topicNameWithConsumerGroupName.get().right)
                .setTopicNames(Collections.singleton(topicNameWithConsumerGroupName.get().left))
                .setIsTableModel(req.isTableModel));
  }

  public TShowSubscriptionResp showSubscription(TShowSubscriptionReq req) {
    try {
      return ((SubscriptionTableResp)
              configManager.getConsensusManager().read(new ShowSubscriptionPlan()))
          .filter(req.getTopicName(), req.isTableModel)
          .convertToTShowSubscriptionResp();
    } catch (Exception e) {
      LOGGER.warn(ManagerMessages.FAILED_TO_SHOW_SUBSCRIPTION_INFO, e);
      return new SubscriptionTableResp(
              new TSStatus(TSStatusCode.SHOW_SUBSCRIPTION_ERROR.getStatusCode())
                  .setMessage(e.getMessage()),
              Collections.emptyList(),
              Collections.emptyList())
          .convertToTShowSubscriptionResp();
    }
  }

  public TGetAllSubscriptionInfoResp getAllSubscriptionInfo() {
    try {
      return ((SubscriptionTableResp)
              configManager.getConsensusManager().read(new ShowSubscriptionPlan()))
          .convertToTGetAllSubscriptionInfoResp();
    } catch (Exception e) {
      LOGGER.warn(ManagerMessages.FAILED_TO_GET_ALL_SUBSCRIPTION_INFO, e);
      return new TGetAllSubscriptionInfoResp(
          new TSStatus(TSStatusCode.SHOW_SUBSCRIPTION_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
