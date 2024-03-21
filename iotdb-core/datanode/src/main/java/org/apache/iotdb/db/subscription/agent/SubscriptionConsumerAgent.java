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

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMetaKeeper;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupRespExceptionMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class SubscriptionConsumerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumerAgent.class);

  private final ConsumerGroupMetaKeeper consumerGroupMetaKeeper;

  public SubscriptionConsumerAgent() {
    this.consumerGroupMetaKeeper = new ConsumerGroupMetaKeeper();
  }

  ////////////////////////// ConsumerGroupMeta Lock Control //////////////////////////

  protected void acquireReadLock() {
    consumerGroupMetaKeeper.acquireReadLock();
  }

  protected void releaseReadLock() {
    consumerGroupMetaKeeper.releaseReadLock();
  }

  protected void acquireWriteLock() {
    consumerGroupMetaKeeper.acquireWriteLock();
  }

  protected void releaseWriteLock() {
    consumerGroupMetaKeeper.releaseWriteLock();
  }

  ////////////////////////// ConsumerGroupMeta Management Entry //////////////////////////

  public TPushConsumerGroupRespExceptionMessage handleSingleConsumerGroupMetaChanges(
      ConsumerGroupMeta consumerGroupMetaFromCoordinator) {
    acquireWriteLock();
    try {
      handleSingleConsumerGroupMetaChangesInternal(consumerGroupMetaFromCoordinator);
      return null;
    } catch (Exception e) {
      final String consumerGroupId = consumerGroupMetaFromCoordinator.getConsumerGroupId();
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to handle single consumer group meta changes for consumer group %s, because %s",
              consumerGroupId, e.getMessage());
      LOGGER.warn(exceptionMessage);
      return new TPushConsumerGroupRespExceptionMessage(
          consumerGroupId, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleSingleConsumerGroupMetaChangesInternal(
      final ConsumerGroupMeta metaFromCoordinator) {
    final String consumerGroupId = metaFromCoordinator.getConsumerGroupId();
    if (!SubscriptionAgent.broker().isBrokerExist(consumerGroupId)) {
      SubscriptionAgent.broker().createBroker(consumerGroupId);
    }
    consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
    consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
  }

  public TPushConsumerGroupRespExceptionMessage handleConsumerGroupMetaChanges(
      List<ConsumerGroupMeta> consumerGroupMetasFromCoordinator) {
    acquireWriteLock();
    try {
      for (ConsumerGroupMeta consumerGroupMetaFromCoordinator : consumerGroupMetasFromCoordinator) {
        try {
          handleSingleConsumerGroupMetaChangesInternal(consumerGroupMetaFromCoordinator);
          return null;
        } catch (Exception e) {
          final String consumerGroupId = consumerGroupMetaFromCoordinator.getConsumerGroupId();
          final String exceptionMessage =
              String.format(
                  "Subscription: Failed to handle single consumer group meta changes for consumer group %s, because %s",
                  consumerGroupId, e.getMessage());
          LOGGER.warn(exceptionMessage);
          return new TPushConsumerGroupRespExceptionMessage(
              consumerGroupId, exceptionMessage, System.currentTimeMillis());
        }
      }
      return null;
    } finally {
      releaseWriteLock();
    }
  }

  public TPushConsumerGroupRespExceptionMessage handleDropConsumerGroup(String consumerGroupId) {
    acquireWriteLock();
    try {
      handleDropConsumerGroupInternal(consumerGroupId);
      return null;
    } catch (Exception e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to drop consumer group %s, because %s",
              consumerGroupId, e.getMessage());
      LOGGER.warn(exceptionMessage);
      return new TPushConsumerGroupRespExceptionMessage(
          consumerGroupId, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleDropConsumerGroupInternal(String consumerGroupId) {
    consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
  }

  public boolean isConsumerExisted(String consumerId, String consumerGroupId) {
    acquireReadLock();
    try {
      final ConsumerGroupMeta consumerGroupMeta =
          consumerGroupMetaKeeper.getConsumerGroupMeta(consumerGroupId);
      return Objects.nonNull(consumerGroupMeta) && consumerGroupMeta.containsConsumer(consumerId);
    } finally {
      releaseReadLock();
    }
  }
}
