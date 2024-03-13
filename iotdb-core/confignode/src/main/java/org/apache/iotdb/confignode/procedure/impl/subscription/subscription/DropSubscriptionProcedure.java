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

package org.apache.iotdb.confignode.procedure.impl.subscription.subscription;

import org.apache.iotdb.commons.subscription.meta.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.TopicMeta;
import org.apache.iotdb.confignode.manager.subscription.coordinator.SubscriptionCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StopPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DropSubscriptionProcedure extends AbstractOperateSubscriptionProcedure {
  private static final Logger LOGGER = LoggerFactory.getLogger(DropSubscriptionProcedure.class);

  private TUnsubscribeReq unsubscribeReq;
  private AlterConsumerGroupProcedure consumerGroupProcedure;
  private List<AlterTopicProcedure> topicProcedures = new ArrayList<>();
  private List<AbstractOperatePipeProcedureV2> pipeProcedures = new ArrayList<>();

  public DropSubscriptionProcedure() {
    super();
  }

  public DropSubscriptionProcedure(TUnsubscribeReq unsubscribeReq) {
    this.unsubscribeReq = unsubscribeReq;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.DROP_SUBSCRIPTION;
  }

  @Override
  protected void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropSubscriptionProcedure: executeFromLock, try to acquire subscription lock");

    final SubscriptionCoordinator subscriptionCoordinator =
        env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator();

    subscriptionCoordinator.lock();

    // check if the consumer and all topics exists
    try {
      subscriptionCoordinator.getSubscriptionInfo().validateBeforeUnsubscribe(unsubscribeReq);
    } catch (PipeException e) {
      LOGGER.error(
          "DropSubscriptionProcedure: executeFromLock, validateBeforeUnsubscribe failed", e);
      throw e;
    }

    // Construct AlterConsumerGroupProcedure
    ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionCoordinator
            .getSubscriptionInfo()
            .getConsumerGroupMeta(unsubscribeReq.getConsumerGroupId())
            .copy();

    // Get topics subscribed by no consumers in this group after this un-subscription
    Set<String> topicsUnsubByGroup =
        updatedConsumerGroupMeta.removeSubscription(
            unsubscribeReq.getConsumerId(), unsubscribeReq.getTopicNames());
    consumerGroupProcedure = new AlterConsumerGroupProcedure(updatedConsumerGroupMeta);

    for (String topic : unsubscribeReq.getTopicNames()) {
      if (topicsUnsubByGroup.contains(topic)) {
        // Topic will be subscribed by no consumers in this group

        TopicMeta updatedTopicMeta =
            subscriptionCoordinator.getSubscriptionInfo().getTopicMeta(topic).copy();
        updatedTopicMeta.removeSubscribedConsumerGroup(unsubscribeReq.getConsumerGroupId());

        topicProcedures.add(new AlterTopicProcedure(updatedTopicMeta));
        pipeProcedures.add(
            new StopPipeProcedureV2(topic + "_" + unsubscribeReq.getConsumerGroupId()));
      }
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnConfigNodes");

    int topicCount = topicProcedures.size();
    for (int i = 0; i < topicCount; ++i) {
      pipeProcedures.get(i).executeFromValidateTask(env);
    }

    for (int i = 0; i < topicCount; ++i) {
      pipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }

    consumerGroupProcedure.executeFromOperateOnConfigNodes(env);

    for (int i = 0; i < topicCount; ++i) {
      topicProcedures.get(i).executeFromOperateOnConfigNodes(env);
      pipeProcedures.get(i).executeFromWriteConfigNodeConsensus(env);
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnDataNodes");

    consumerGroupProcedure.executeFromOperateOnDataNodes(env);

    int topicCount = topicProcedures.size();
    for (int i = 0; i < topicCount; ++i) {
      topicProcedures.get(i).executeFromOperateOnDataNodes(env);
      try {
        pipeProcedures.get(i).executeFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to stop pipe task for subscription on datanode, because {}", e.getMessage());
        throw new PipeException(e.getMessage());
      }
    }
  }

  @Override
  protected void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropSubscriptionProcedure: executeFromUnlock");
  }

  @Override
  protected void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromLock");
    env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator().unlock();
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

    int topicCount = topicProcedures.size();
    for (int i = topicCount - 1; i >= 0; --i) {
      pipeProcedures.get(i).rollbackFromWriteConfigNodeConsensus(env);
      topicProcedures.get(i).rollbackFromOperateOnConfigNodes(env);
    }

    consumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);

    for (int i = topicCount - 1; i >= 0; --i) {
      pipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }

    for (int i = topicCount - 1; i >= 0; --i) {
      pipeProcedures.get(i).executeFromValidateTask(env);
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    int topicCount = topicProcedures.size();
    for (int i = topicCount - 1; i >= 0; --i) {
      try {
        pipeProcedures.get(i).rollbackFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to roll back stop pipe task for subscription on datanode, because {}",
            e.getMessage());
        throw new PipeException(e.getMessage());
      }
      topicProcedures.get(i).rollbackFromOperateOnDataNodes(env);
    }

    consumerGroupProcedure.rollbackFromOperateOnDataNodes(env);
  }
}
