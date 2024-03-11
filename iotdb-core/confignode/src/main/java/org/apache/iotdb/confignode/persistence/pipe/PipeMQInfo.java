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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.subscription.meta.PipeMQConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.PipeMQConsumerGroupMetaKeeper;
import org.apache.iotdb.commons.subscription.meta.PipeMQSubscriptionMeta;
import org.apache.iotdb.commons.subscription.meta.PipeMQTopicMeta;
import org.apache.iotdb.commons.subscription.meta.PipeMQTopicMetaKeeper;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.consumer.AlterPipeMQConsumerGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.AlterPipeMQTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.CreatePipeMQTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.DropPipeMQTopicPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.mq.PipeMQSubscriptionTableResp;
import org.apache.iotdb.confignode.consensus.response.pipe.mq.PipeMQTopicTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.pipe.api.exception.PipeException;
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

public class PipeMQInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMQInfo.class);

  private static final String SNAPSHOT_FILE_NAME = "pipe_mq_info.bin";

  private final PipeMQTopicMetaKeeper pipeMQTopicMetaKeeper;
  private final PipeMQConsumerGroupMetaKeeper pipeMQConsumerGroupMetaKeeper;
  private final ReentrantReadWriteLock pipeMQInfoLock = new ReentrantReadWriteLock(true);

  public PipeMQInfo() {
    this.pipeMQTopicMetaKeeper = new PipeMQTopicMetaKeeper();
    this.pipeMQConsumerGroupMetaKeeper = new PipeMQConsumerGroupMetaKeeper();
  }

  /////////////////////////////// Lock ///////////////////////////////

  private void acquireReadLock() {
    pipeMQInfoLock.readLock().lock();
  }

  private void releaseReadLock() {
    pipeMQInfoLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    pipeMQInfoLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    pipeMQInfoLock.writeLock().unlock();
  }

  /////////////////////////////// Validator ///////////////////////////////
  public void validateBeforeCreatingTopic(TCreateTopicReq createTopicReq) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeCreateTopicInternal(createTopicReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeCreateTopicInternal(TCreateTopicReq createTopicReq) throws PipeException {
    if (!isTopicExisted(createTopicReq.getTopicName())) {
      return;
    }

    final String exceptionMessage =
        String.format(
            "Failed to create pipe topic %s, the topic with the same name has been created",
            createTopicReq.getTopicName());
    LOGGER.warn(exceptionMessage);
    throw new PipeException(exceptionMessage);
  }

  public void validateBeforeDroppingTopic(String topicName) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeDropTopicInternal(topicName);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeDropTopicInternal(String topicName) throws PipeException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Check before dropping topic: {}, topic exists:{}", topicName, isTopicExisted(topicName));
    }

    // No matter whether the topic exists, we allow the drop operation executed on all nodes to
    // ensure the consistency.
    // DO NOTHING HERE!
  }

  public void validateBeforeAlteringTopic(PipeMQTopicMeta pipeMQTopicMeta) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeAlteringTopicInternal(pipeMQTopicMeta);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeAlteringTopicInternal(PipeMQTopicMeta pipeMQTopicMeta)
      throws PipeException {
    if (isTopicExisted(pipeMQTopicMeta.getTopicName())) {
      return;
    }

    final String exceptionMessage =
        String.format(
            "Failed to alter pipe topic %s, the pipe topic with the same name is not existed",
            pipeMQTopicMeta.getTopicName());
    LOGGER.warn(exceptionMessage);
    throw new PipeException(exceptionMessage);
  }

  public boolean isTopicExisted(String topicName) {
    acquireReadLock();
    try {
      return pipeMQTopicMetaKeeper.containsPipeMQTopicMeta(topicName);
    } finally {
      releaseReadLock();
    }
  }

  public PipeMQTopicMeta getPipeMQTopicMeta(String topicName) {
    acquireReadLock();
    try {
      return pipeMQTopicMetaKeeper.getPipeMQTopicMeta(topicName);
    } finally {
      releaseReadLock();
    }
  }

  private Iterable<PipeMQTopicMeta> getAllPipeMQTopicMeta() {
    return pipeMQTopicMetaKeeper.getAllPipeMQTopicMeta();
  }

  public DataSet showTopics() {
    acquireReadLock();
    try {
      return new PipeMQTopicTableResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          StreamSupport.stream(getAllPipeMQTopicMeta().spliterator(), false)
              .collect(Collectors.toList()));
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus createTopic(CreatePipeMQTopicPlan plan) {
    acquireWriteLock();
    try {
      pipeMQTopicMetaKeeper.addPipeMQTopicMeta(
          plan.getTopicMeta().getTopicName(), plan.getTopicMeta());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus alterTopic(AlterPipeMQTopicPlan plan) {
    acquireWriteLock();
    try {
      pipeMQTopicMetaKeeper.removePipeMQTopicMeta(plan.getTopicMeta().getTopicName());
      pipeMQTopicMetaKeeper.addPipeMQTopicMeta(
          plan.getTopicMeta().getTopicName(), plan.getTopicMeta());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus dropTopic(DropPipeMQTopicPlan plan) {
    acquireWriteLock();
    try {
      pipeMQTopicMetaKeeper.removePipeMQTopicMeta(plan.getTopicName());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  /////////////////////////////////  Consumer  /////////////////////////////////

  public void validateBeforeCreatingConsumer(TCreateConsumerReq createConsumerReq)
      throws PipeException {
    acquireReadLock();
    try {
      checkBeforeCreateConsumerInternal(createConsumerReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeCreateConsumerInternal(TCreateConsumerReq createConsumerReq)
      throws PipeException {
    if (!isConsumerGroupExisted(createConsumerReq.getConsumerGroupId())
        || !isConsumerExisted(
            createConsumerReq.getConsumerGroupId(), createConsumerReq.getConsumerId())) {
      return;
    }

    // A consumer with same consumerId and consumerGroupId has already existed, we should end the
    // procedure
    final String exceptionMessage =
        String.format(
            "Failed to create pipe consumer %s in consumer group %s, the consumer with the same name has been created",
            createConsumerReq.getConsumerId(), createConsumerReq.getConsumerGroupId());
    LOGGER.warn(exceptionMessage);
    throw new PipeException(exceptionMessage);
  }

  public void validateBeforeDroppingConsumer(TCloseConsumerReq dropConsumerReq)
      throws PipeException {
    acquireReadLock();
    try {
      checkBeforeDropConsumerInternal(dropConsumerReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeDropConsumerInternal(TCloseConsumerReq dropConsumerReq)
      throws PipeException {
    if (isConsumerExisted(dropConsumerReq.getConsumerGroupId(), dropConsumerReq.getConsumerId())) {
      return;
    }

    // There is no consumer with the same consumerId and consumerGroupId, we should end the
    // procedure
    final String exceptionMessage =
        String.format(
            "Failed to drop pipe consumer %s in consumer group %s, the consumer with the same name does not exist",
            dropConsumerReq.getConsumerId(), dropConsumerReq.getConsumerGroupId());
    LOGGER.warn(exceptionMessage);
    throw new PipeException(exceptionMessage);
  }

  public void validateBeforeAlterConsumerGroup(PipeMQConsumerGroupMeta consumerGroupMeta)
      throws PipeException {
    acquireReadLock();
    try {
      checkBeforeAlterConsumerGroupInternal(consumerGroupMeta);
    } finally {
      releaseReadLock();
    }
  }

  public void checkBeforeAlterConsumerGroupInternal(PipeMQConsumerGroupMeta consumerGroupMeta)
      throws PipeException {
    if (!isConsumerGroupExisted(consumerGroupMeta.getConsumerGroupId())) {
      // Consumer group not exist, not allowed to alter
      final String exceptionMessage =
          String.format(
              "Failed to alter consumer group because the consumer group %s does not exist",
              consumerGroupMeta.getConsumerGroupId());
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  public boolean isConsumerGroupExisted(String consumerGroupName) {
    acquireReadLock();
    try {
      return pipeMQConsumerGroupMetaKeeper.containsPipeMQConsumerGroupMeta(consumerGroupName);
    } finally {
      releaseReadLock();
    }
  }

  public boolean isConsumerExisted(String consumerGroupName, String consumerId) {
    acquireReadLock();
    try {
      PipeMQConsumerGroupMeta consumerGroupMeta =
          pipeMQConsumerGroupMetaKeeper.getPipeMQConsumerGroupMeta(consumerGroupName);
      return consumerGroupMeta == null || consumerGroupMeta.containsConsumer(consumerId);
    } finally {
      releaseReadLock();
    }
  }

  public PipeMQConsumerGroupMeta getPipeMQConsumerGroupMeta(String consumerGroupName) {
    acquireReadLock();
    try {
      return pipeMQConsumerGroupMetaKeeper.getPipeMQConsumerGroupMeta(consumerGroupName);
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus alterConsumerGroup(AlterPipeMQConsumerGroupPlan plan) {
    acquireWriteLock();
    try {
      pipeMQConsumerGroupMetaKeeper.removePipeMQConsumerGroupMeta(
          plan.getConsumerGroupMeta().getConsumerGroupId());
      if (plan.getConsumerGroupMeta() != null) {
        pipeMQConsumerGroupMetaKeeper.addPipeMQConsumerGroupMeta(
            plan.getConsumerGroupMeta().getConsumerGroupId(), plan.getConsumerGroupMeta());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  /////////////////////////////////  Subscription  /////////////////////////////////

  public void validateBeforeSubscribe(TSubscribeReq subscribeReq) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeSubscribeInternal(subscribeReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeSubscribeInternal(TSubscribeReq subscribeReq) throws PipeException {
    // 1. Check if the consumer exists
    if (!isConsumerExisted(subscribeReq.getConsumerGroupId(), subscribeReq.getConsumerId())) {
      // There is no consumer with the same consumerId and consumerGroupId, we should end the
      // procedure
      final String exceptionMessage =
          String.format(
              "Failed to subscribe because the consumer %s in consumer group %s does not exist",
              subscribeReq.getConsumerId(), subscribeReq.getConsumerGroupId());
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    // 2. Check if all topics exist. No need to check if already subscribed.
    for (String topic : subscribeReq.getTopicNames()) {
      if (!isTopicExisted(topic)) {
        final String exceptionMessage =
            String.format("Failed to subscribe because the topic %s does not exist", topic);
        LOGGER.warn(exceptionMessage);
        throw new PipeException(exceptionMessage);
      }
    }
  }

  /**
   * @return A set of topics from the TSubscribeReq that will have no consumer in this group after
   *     this unsubscribe.
   */
  public void validateBeforeUnsubscribe(TUnsubscribeReq unsubscribeReq) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeUnsubscribeInternal(unsubscribeReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeUnsubscribeInternal(TUnsubscribeReq unsubscribeReq) throws PipeException {
    // 1. Check if the consumer exists
    if (!isConsumerExisted(unsubscribeReq.getConsumerGroupId(), unsubscribeReq.getConsumerId())) {
      // There is no consumer with the same consumerId and consumerGroupId, we should end the
      // procedure
      final String exceptionMessage =
          String.format(
              "Failed to unsubscribe because the consumer %s in consumer group %s does not exist",
              unsubscribeReq.getConsumerId(), unsubscribeReq.getConsumerGroupId());
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    // 2. Check if all topics exist. No need to check if already subscribed.
    for (String topic : unsubscribeReq.getTopicNames()) {
      if (!isTopicExisted(topic)) {
        final String exceptionMessage =
            String.format("Failed to unsubscribe because the topic %s does not exist", topic);
        LOGGER.warn(exceptionMessage);
        throw new PipeException(exceptionMessage);
      }
    }
  }

  public DataSet showSubscriptions() {
    acquireReadLock();
    try {
      return new PipeMQSubscriptionTableResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          getAllPipeMQSubscriptionMeta());
    } finally {
      releaseReadLock();
    }
  }

  private List<PipeMQSubscriptionMeta> getAllPipeMQSubscriptionMeta() {
    List<PipeMQSubscriptionMeta> allSubscriptions = new ArrayList<>();
    for (PipeMQTopicMeta topicMeta : pipeMQTopicMetaKeeper.getAllPipeMQTopicMeta()) {
      for (String consumerGroupId : topicMeta.getSubscribedConsumerGroupIDs()) {
        Set<String> subscribedConsumerIDs =
            pipeMQConsumerGroupMetaKeeper.getConsumersSubscribingTopic(
                consumerGroupId, topicMeta.getTopicName());
        if (!subscribedConsumerIDs.isEmpty()) {
          allSubscriptions.add(
              new PipeMQSubscriptionMeta(
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
            "Failed to take snapshot, because snapshot file [{}] is already exist.",
            snapshotFile.getAbsolutePath());
        return false;
      }

      try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
        pipeMQTopicMetaKeeper.processTakeSnapshot(fileOutputStream);
        pipeMQConsumerGroupMetaKeeper.processTakeSnapshot(fileOutputStream);
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
            "Failed to load snapshot,snapshot file [{}] is not exist.",
            snapshotFile.getAbsolutePath());
        return;
      }

      try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
        pipeMQTopicMetaKeeper.processLoadSnapshot(fileInputStream);
        pipeMQConsumerGroupMetaKeeper.processLoadSnapshot(fileInputStream);
      }
    } finally {
      releaseWriteLock();
    }
  }
}
