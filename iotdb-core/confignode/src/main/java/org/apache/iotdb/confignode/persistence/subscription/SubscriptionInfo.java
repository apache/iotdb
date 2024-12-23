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

package org.apache.iotdb.confignode.persistence.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMetaKeeper;
import org.apache.iotdb.commons.subscription.meta.subscription.SubscriptionMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMetaKeeper;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime.ConsumerGroupHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterMultipleTopicsPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.runtime.TopicHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.response.subscription.SubscriptionTableResp;
import org.apache.iotdb.confignode.consensus.response.subscription.TopicTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SubscriptionInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionInfo.class);

  private static final String SNAPSHOT_FILE_NAME = "subscription_info.bin";

  private final TopicMetaKeeper topicMetaKeeper;
  private final ConsumerGroupMetaKeeper consumerGroupMetaKeeper;

  private final ReentrantReadWriteLock subscriptionInfoLock = new ReentrantReadWriteLock(true);

  // Pure in-memory object, not involved in snapshot serialization and deserialization.
  private final SubscriptionInfoVersion subscriptionInfoVersion;

  public SubscriptionInfo() {
    this.topicMetaKeeper = new TopicMetaKeeper();
    this.consumerGroupMetaKeeper = new ConsumerGroupMetaKeeper();
    this.subscriptionInfoVersion = new SubscriptionInfoVersion();
  }

  /////////////////////////////// Lock ///////////////////////////////

  private void acquireReadLock() {
    subscriptionInfoLock.readLock().lock();
  }

  private void releaseReadLock() {
    subscriptionInfoLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    subscriptionInfoLock.writeLock().lock();
    subscriptionInfoVersion.increaseLatestVersion();
  }

  public void releaseWriteLock() {
    subscriptionInfoLock.writeLock().unlock();
  }

  /////////////////////////////// Version ///////////////////////////////

  private class SubscriptionInfoVersion {

    private final AtomicLong latestVersion;
    private long lastSyncedVersion;
    private boolean isLastSyncedPipeTaskInfoEmpty;

    public SubscriptionInfoVersion() {
      this.latestVersion = new AtomicLong(0);
      this.lastSyncedVersion = 0;
      this.isLastSyncedPipeTaskInfoEmpty = false;
    }

    public void increaseLatestVersion() {
      latestVersion.incrementAndGet();
    }

    public void updateLastSyncedVersion() {
      lastSyncedVersion = latestVersion.get();
      isLastSyncedPipeTaskInfoEmpty =
          topicMetaKeeper.isEmpty() && consumerGroupMetaKeeper.isEmpty();
    }

    public boolean canSkipNextSync() {
      return isLastSyncedPipeTaskInfoEmpty
          && topicMetaKeeper.isEmpty()
          && consumerGroupMetaKeeper.isEmpty()
          && lastSyncedVersion == latestVersion.get();
    }
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock}. */
  public void updateLastSyncedVersion() {
    subscriptionInfoVersion.updateLastSyncedVersion();
  }

  public boolean canSkipNextSync() {
    return subscriptionInfoVersion.canSkipNextSync();
  }

  /////////////////////////////// Topic ///////////////////////////////

  public boolean validateBeforeCreatingTopic(TCreateTopicReq createTopicReq)
      throws SubscriptionException {
    acquireReadLock();
    try {
      return checkBeforeCreateTopicInternal(createTopicReq);
    } finally {
      releaseReadLock();
    }
  }

  private boolean checkBeforeCreateTopicInternal(TCreateTopicReq createTopicReq)
      throws SubscriptionException {
    if (!isTopicExisted(createTopicReq.getTopicName())) {
      return true;
    }

    if (createTopicReq.isSetIfNotExistsCondition() && createTopicReq.isIfNotExistsCondition()) {
      return false;
    }

    final String exceptionMessage =
        String.format(
            "Failed to create topic %s, the topic with the same name has been created",
            createTopicReq.getTopicName());
    LOGGER.warn(exceptionMessage);
    throw new SubscriptionException(exceptionMessage);
  }

  public void validateBeforeDroppingTopic(String topicName) throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeDropTopicInternal(topicName);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeDropTopicInternal(String topicName) throws SubscriptionException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Check before dropping topic: {}, topic exists: {}",
          topicName,
          isTopicExisted(topicName));
    }

    TopicMeta topicMeta = topicMetaKeeper.getTopicMeta(topicName);
    if (Objects.isNull(topicMeta)) {
      // DO NOTHING HERE!
      // No matter whether the topic exists, we allow the drop operation
      // executed on all nodes to ensure the consistency.
      return;
    } else {
      if (!consumerGroupMetaKeeper.isTopicSubscribedByConsumerGroup(topicName)) {
        return;
      }
    }

    final String exceptionMessage =
        String.format(
            "Failed to drop topic %s, the topic is subscribed by some consumers",
            topicMeta.getTopicName());
    LOGGER.warn(exceptionMessage);
    throw new SubscriptionException(exceptionMessage);
  }

  public void validatePipePluginUsageByTopic(String pipePluginName) throws SubscriptionException {
    acquireReadLock();
    try {
      validatePipePluginUsageByTopicInternal(pipePluginName);
    } finally {
      releaseReadLock();
    }
  }

  public void validatePipePluginUsageByTopicInternal(String pipePluginName)
      throws SubscriptionException {
    acquireReadLock();
    try {
      topicMetaKeeper
          .getAllTopicMeta()
          .forEach(
              meta -> {
                if (pipePluginName.equals(meta.getConfig().getAttribute().get("processor"))) {
                  final String exceptionMessage =
                      String.format(
                          "PipePlugin '%s' is already used by Topic '%s' as a processor.",
                          pipePluginName, meta.getTopicName());
                  LOGGER.warn(exceptionMessage);
                  throw new SubscriptionException(exceptionMessage);
                }
              });
    } finally {
      releaseReadLock();
    }
  }

  public void validateBeforeAlteringTopic(TopicMeta topicMeta) throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeAlteringTopicInternal(topicMeta);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeAlteringTopicInternal(TopicMeta topicMeta) throws SubscriptionException {
    if (isTopicExisted(topicMeta.getTopicName())) {
      return;
    }

    final String exceptionMessage =
        String.format(
            "Failed to alter topic %s, the topic is not existed", topicMeta.getTopicName());
    LOGGER.warn(exceptionMessage);
    throw new SubscriptionException(exceptionMessage);
  }

  public boolean isTopicExisted(String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName);
    } finally {
      releaseReadLock();
    }
  }

  public TopicMeta getTopicMeta(String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.getTopicMeta(topicName);
    } finally {
      releaseReadLock();
    }
  }

  public Iterable<TopicMeta> getAllTopicMeta() {
    acquireReadLock();
    try {
      return topicMetaKeeper.getAllTopicMeta();
    } finally {
      releaseReadLock();
    }
  }

  public TopicMeta deepCopyTopicMeta(String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName)
          ? topicMetaKeeper.getTopicMeta(topicName).deepCopy()
          : null;
    } finally {
      releaseReadLock();
    }
  }

  public DataSet showTopics() {
    acquireReadLock();
    try {
      return new TopicTableResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          StreamSupport.stream(topicMetaKeeper.getAllTopicMeta().spliterator(), false)
              .collect(Collectors.toList()));
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus createTopic(CreateTopicPlan plan) {
    acquireWriteLock();
    try {
      topicMetaKeeper.addTopicMeta(plan.getTopicMeta().getTopicName(), plan.getTopicMeta());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus alterTopic(AlterTopicPlan plan) {
    acquireWriteLock();
    try {
      topicMetaKeeper.removeTopicMeta(plan.getTopicMeta().getTopicName());
      topicMetaKeeper.addTopicMeta(plan.getTopicMeta().getTopicName(), plan.getTopicMeta());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus alterMultipleTopics(AlterMultipleTopicsPlan plan) {
    acquireWriteLock();
    try {
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setSubStatus(new ArrayList<>());
      for (AlterTopicPlan subPlan : plan.getSubPlans()) {
        TSStatus innerStatus = alterTopic(subPlan);
        status.getSubStatus().add(innerStatus);
        if (innerStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          status.setCode(TSStatusCode.ALTER_TOPIC_ERROR.getStatusCode());
          break;
        }
      }

      // If all sub-plans are successful, set the sub-status to null
      // Otherwise, keep the sub-status to indicate the failing plan's index
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        status.setSubStatus(null);
      }
      return status;
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus dropTopic(DropTopicPlan plan) {
    acquireWriteLock();
    try {
      topicMetaKeeper.removeTopicMeta(plan.getTopicName());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus handleTopicMetaChanges(TopicHandleMetaChangePlan plan) {
    acquireWriteLock();
    try {
      LOGGER.info("Handling topic meta changes ...");

      topicMetaKeeper.clear();

      plan.getTopicMetaList()
          .forEach(
              topicMeta -> {
                topicMetaKeeper.addTopicMeta(topicMeta.getTopicName(), topicMeta);
                LOGGER.info("Recording topic meta: {}", topicMeta);
              });

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  /////////////////////////////////  Consumer  /////////////////////////////////

  public void validateBeforeCreatingConsumer(TCreateConsumerReq createConsumerReq)
      throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeCreateConsumerInternal(createConsumerReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeCreateConsumerInternal(TCreateConsumerReq createConsumerReq)
      throws SubscriptionException {
    if (!isConsumerGroupExisted(createConsumerReq.getConsumerGroupId())
        || !isConsumerExisted(
            createConsumerReq.getConsumerGroupId(), createConsumerReq.getConsumerId())) {
      return;
    }

    // A consumer with same consumerId and consumerGroupId has already existed,
    // we should end the procedure
    final String exceptionMessage =
        String.format(
            "Failed to create pipe consumer %s in consumer group %s, the consumer with the same name has been created",
            createConsumerReq.getConsumerId(), createConsumerReq.getConsumerGroupId());
    LOGGER.warn(exceptionMessage);
    throw new SubscriptionException(exceptionMessage);
  }

  public void validateBeforeDroppingConsumer(TCloseConsumerReq dropConsumerReq)
      throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeDropConsumerInternal(dropConsumerReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeDropConsumerInternal(TCloseConsumerReq dropConsumerReq)
      throws SubscriptionException {
    if (isConsumerExisted(dropConsumerReq.getConsumerGroupId(), dropConsumerReq.getConsumerId())) {
      return;
    }

    // There is no consumer with the same consumerId and consumerGroupId, we should end the
    // procedure
    final String exceptionMessage =
        String.format(
            "Failed to drop pipe consumer %s in consumer group %s, the consumer does not exist",
            dropConsumerReq.getConsumerId(), dropConsumerReq.getConsumerGroupId());
    LOGGER.warn(exceptionMessage);
    throw new SubscriptionException(exceptionMessage);
  }

  public void validateBeforeAlterConsumerGroup(ConsumerGroupMeta consumerGroupMeta)
      throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeAlterConsumerGroupInternal(consumerGroupMeta);
    } finally {
      releaseReadLock();
    }
  }

  public void checkBeforeAlterConsumerGroupInternal(ConsumerGroupMeta consumerGroupMeta)
      throws SubscriptionException {
    if (isConsumerGroupExisted(consumerGroupMeta.getConsumerGroupId())) {
      return;
    }

    // Consumer group not exist, not allowed to alter
    final String exceptionMessage =
        String.format(
            "Failed to alter consumer group because the consumer group %s does not exist",
            consumerGroupMeta.getConsumerGroupId());
    LOGGER.warn(exceptionMessage);
    throw new SubscriptionException(exceptionMessage);
  }

  public boolean isConsumerGroupExisted(String consumerGroupName) {
    acquireReadLock();
    try {
      return consumerGroupMetaKeeper.containsConsumerGroupMeta(consumerGroupName);
    } finally {
      releaseReadLock();
    }
  }

  public boolean isConsumerExisted(String consumerGroupName, String consumerId) {
    acquireReadLock();
    try {
      final ConsumerGroupMeta consumerGroupMeta =
          consumerGroupMetaKeeper.getConsumerGroupMeta(consumerGroupName);
      return consumerGroupMeta != null && consumerGroupMeta.containsConsumer(consumerId);
    } finally {
      releaseReadLock();
    }
  }

  public ConsumerGroupMeta getConsumerGroupMeta(String consumerGroupName) {
    acquireReadLock();
    try {
      return consumerGroupMetaKeeper.getConsumerGroupMeta(consumerGroupName);
    } finally {
      releaseReadLock();
    }
  }

  public ConsumerGroupMeta deepCopyConsumerGroupMeta(String consumerGroupName) {
    acquireReadLock();
    try {
      return consumerGroupMetaKeeper.containsConsumerGroupMeta(consumerGroupName)
          ? consumerGroupMetaKeeper.getConsumerGroupMeta(consumerGroupName).deepCopy()
          : null;
    } finally {
      releaseReadLock();
    }
  }

  public boolean isTopicSubscribedByConsumerGroup(
      final String topicName, final String consumerGroupId) {
    acquireReadLock();
    try {
      return consumerGroupMetaKeeper.isTopicSubscribedByConsumerGroup(topicName, consumerGroupId);
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus alterConsumerGroup(AlterConsumerGroupPlan plan) {
    acquireWriteLock();
    try {
      ConsumerGroupMeta consumerGroupMeta = plan.getConsumerGroupMeta();
      if (Objects.nonNull(consumerGroupMeta)) {
        String consumerGroupId = consumerGroupMeta.getConsumerGroupId();
        consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
        if (!consumerGroupMeta.isEmpty()) {
          consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, consumerGroupMeta);
        }
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus handleConsumerGroupMetaChanges(ConsumerGroupHandleMetaChangePlan plan) {
    acquireWriteLock();
    try {
      LOGGER.info("Handling consumer group meta changes ...");

      consumerGroupMetaKeeper.clear();

      plan.getConsumerGroupMetaList()
          .forEach(
              consumerGroupMeta -> {
                consumerGroupMetaKeeper.addConsumerGroupMeta(
                    consumerGroupMeta.getConsumerGroupId(), consumerGroupMeta);
                LOGGER.info("Recording consumer group meta: {}", consumerGroupMeta);
              });

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  /////////////////////////////////  Subscription  /////////////////////////////////

  public void validateBeforeSubscribe(TSubscribeReq subscribeReq) throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeSubscribeInternal(subscribeReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeSubscribeInternal(TSubscribeReq subscribeReq)
      throws SubscriptionException {
    // 1. Check if the consumer exists
    if (!isConsumerExisted(subscribeReq.getConsumerGroupId(), subscribeReq.getConsumerId())) {
      // There is no consumer with the same consumerId and consumerGroupId,
      // we should end the procedure
      final String exceptionMessage =
          String.format(
              "Failed to subscribe because the consumer %s in consumer group %s does not exist",
              subscribeReq.getConsumerId(), subscribeReq.getConsumerGroupId());
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
    }

    // 2. Check if all topics exist. No need to check if already subscribed.
    for (String topic : subscribeReq.getTopicNames()) {
      if (!isTopicExisted(topic)) {
        final String exceptionMessage =
            String.format("Failed to subscribe because the topic %s does not exist", topic);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    }
  }

  public void validateBeforeUnsubscribe(TUnsubscribeReq unsubscribeReq)
      throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeUnsubscribeInternal(unsubscribeReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeUnsubscribeInternal(TUnsubscribeReq unsubscribeReq)
      throws SubscriptionException {
    // 1. Check if the consumer exists
    if (!isConsumerExisted(unsubscribeReq.getConsumerGroupId(), unsubscribeReq.getConsumerId())) {
      // There is no consumer with the same consumerId and consumerGroupId,
      // we should end the procedure
      final String exceptionMessage =
          String.format(
              "Failed to unsubscribe because the consumer %s in consumer group %s does not exist",
              unsubscribeReq.getConsumerId(), unsubscribeReq.getConsumerGroupId());
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
    }

    // 2. Check if all topics exist. No need to check if already subscribed.
    for (String topic : unsubscribeReq.getTopicNames()) {
      if (!isTopicExisted(topic)) {
        final String exceptionMessage =
            String.format("Failed to unsubscribe because the topic %s does not exist", topic);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    }
  }

  public DataSet showSubscriptions() {
    acquireReadLock();
    try {
      return new SubscriptionTableResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          getAllSubscriptionMeta(),
          getAllConsumerGroupMeta());
    } finally {
      releaseReadLock();
    }
  }

  private List<SubscriptionMeta> getAllSubscriptionMeta() {
    List<SubscriptionMeta> allSubscriptions = new ArrayList<>();
    for (TopicMeta topicMeta : topicMetaKeeper.getAllTopicMeta()) {
      for (String consumerGroupId :
          consumerGroupMetaKeeper.getSubscribedConsumerGroupIds(topicMeta.getTopicName())) {
        Set<String> subscribedConsumerIDs =
            consumerGroupMetaKeeper.getConsumersSubscribingTopic(
                consumerGroupId, topicMeta.getTopicName());
        if (!subscribedConsumerIDs.isEmpty()) {
          allSubscriptions.add(
              new SubscriptionMeta(
                  topicMeta.getTopicName(), consumerGroupId, subscribedConsumerIDs));
        }
      }
    }
    return allSubscriptions;
  }

  public List<ConsumerGroupMeta> getAllConsumerGroupMeta() {
    return StreamSupport.stream(
            consumerGroupMetaKeeper.getAllConsumerGroupMeta().spliterator(), false)
        .collect(Collectors.toList());
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    acquireReadLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (snapshotFile.exists() && snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to take subscription snapshot, because snapshot file {} is already exist.",
            snapshotFile.getAbsolutePath());
        return false;
      }

      try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
        topicMetaKeeper.processTakeSnapshot(fileOutputStream);
        consumerGroupMetaKeeper.processTakeSnapshot(fileOutputStream);
        fileOutputStream.getFD().sync();
      }

      return true;
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    acquireWriteLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (!snapshotFile.exists() || !snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to load subscription snapshot, snapshot file {} is not exist.",
            snapshotFile.getAbsolutePath());
        return;
      }

      try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
        topicMetaKeeper.processLoadSnapshot(fileInputStream);
        consumerGroupMetaKeeper.processLoadSnapshot(fileInputStream);
      }
    } finally {
      releaseWriteLock();
    }
  }
}
