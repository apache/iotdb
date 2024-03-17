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
import org.apache.iotdb.commons.exception.SubscriptionException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMetaKeeper;
import org.apache.iotdb.commons.subscription.meta.subscription.SubscriptionMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMetaKeeper;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
import org.apache.iotdb.confignode.consensus.response.subscription.SubscriptionTableResp;
import org.apache.iotdb.confignode.consensus.response.subscription.TopicTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SubscriptionInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionInfo.class);

  private static final String SNAPSHOT_FILE_NAME = "subscription_info.bin";

  private final TopicMetaKeeper topicMetaKeeper = new TopicMetaKeeper();
  private final ConsumerGroupMetaKeeper consumerGroupMetaKeeper = new ConsumerGroupMetaKeeper();

  private final ReentrantReadWriteLock subscriptionInfoLock = new ReentrantReadWriteLock(true);

  /////////////////////////////// Lock ///////////////////////////////

  private void acquireReadLock() {
    subscriptionInfoLock.readLock().lock();
  }

  private void releaseReadLock() {
    subscriptionInfoLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    subscriptionInfoLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    subscriptionInfoLock.writeLock().unlock();
  }

  /////////////////////////////// Topic ///////////////////////////////

  public void validateBeforeCreatingTopic(TCreateTopicReq createTopicReq)
      throws SubscriptionException {
    acquireReadLock();
    try {
      checkBeforeCreateTopicInternal(createTopicReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeCreateTopicInternal(TCreateTopicReq createTopicReq)
      throws SubscriptionException {
    if (!isTopicExisted(createTopicReq.getTopicName())) {
      return;
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

    // DO NOTHING HERE!
    // No matter whether the topic exists, we allow the drop operation
    // executed on all nodes to ensure the consistency.
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

  public TSStatus dropTopic(DropTopicPlan plan) {
    acquireWriteLock();
    try {
      topicMetaKeeper.removeTopicMeta(plan.getTopicName());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TopicMeta getTopicMetaByTopicName(String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.getTopicMeta(topicName);
    } finally {
      releaseReadLock();
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

  public TSStatus alterConsumerGroup(AlterConsumerGroupPlan plan) {
    acquireWriteLock();
    try {
      if (plan.getConsumerGroupMeta() != null) {
        String consumerGroupId = plan.getConsumerGroupMeta().getConsumerGroupId();
        consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
        consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, plan.getConsumerGroupMeta());
      }
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
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), getAllSubscriptionMeta());
    } finally {
      releaseReadLock();
    }
  }

  private List<SubscriptionMeta> getAllSubscriptionMeta() {
    List<SubscriptionMeta> allSubscriptions = new ArrayList<>();
    for (TopicMeta topicMeta : topicMetaKeeper.getAllTopicMeta()) {
      for (String consumerGroupId : topicMeta.getSubscribedConsumerGroupIDs()) {
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
