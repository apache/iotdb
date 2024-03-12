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
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateSubscriptionProcedure extends AbstractOperateSubscriptionProcedure {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSubscriptionProcedure.class);

  private TSubscribeReq subscribeReq;

  private AlterConsumerGroupProcedure consumerGroupProcedure;
  private List<AlterTopicProcedure> topicProcedures = new ArrayList<>();
  private List<AbstractOperatePipeProcedureV2> pipeProcedures = new ArrayList<>();

  public CreateSubscriptionProcedure(TSubscribeReq subscribeReq) {
    this.subscribeReq = subscribeReq;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_SUBSCRIPTION;
  }

  @Override
  protected void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromLock, try to acquire subscription lock");

    final SubscriptionCoordinator subscriptionCoordinator =
        env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator();

    subscriptionCoordinator.lock();

    // check if the consumer and all topics exists
    try {
      subscriptionCoordinator.getSubscriptionInfo().validateBeforeSubscribe(subscribeReq);
    } catch (PipeException e) {
      LOGGER.error(
          "CreateSubscriptionProcedure: executeFromLock, validateBeforeSubscribe failed", e);
      throw e;
    }

    // Construct AlterConsumerGroupProcedure
    ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionCoordinator
            .getSubscriptionInfo()
            .getConsumerGroupMeta(subscribeReq.getConsumerGroupId())
            .copy();
    updatedConsumerGroupMeta.addSubscription(
        subscribeReq.getConsumerId(), subscribeReq.getTopicNames());
    consumerGroupProcedure = new AlterConsumerGroupProcedure(updatedConsumerGroupMeta);

    for (String topic : subscribeReq.getTopicNames()) {
      // Construct AlterTopicProcedures
      TopicMeta updatedTopicMeta =
          subscriptionCoordinator.getSubscriptionInfo().getTopicMeta(topic).copy();

      // Construct pipe procedures
      if (updatedTopicMeta.addSubscribedConsumerGroup(subscribeReq.getConsumerGroupId())) {
        // Consumer group not subscribed this topic
        // TODO: now all configs are put to extractor attributes. need to put to processor and
        // connector attributes correctly.
        pipeProcedures.add(
            new CreatePipeProcedureV2(
                new TCreatePipeReq()
                    .setPipeName(topic + "_" + subscribeReq.getConsumerGroupId())
                    .setExtractorAttributes(updatedTopicMeta.getConfig().getAttribute())));
      } else {
        // Consumer group already subscribed this topic
        pipeProcedures.add(
            new StartPipeProcedureV2(topic + "_" + subscribeReq.getConsumerGroupId()));
      }

      topicProcedures.add(new AlterTopicProcedure(updatedTopicMeta));
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnConfigNodes");

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
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnDataNodes");

    consumerGroupProcedure.executeFromOperateOnDataNodes(env);

    int topicCount = topicProcedures.size();
    for (int i = 0; i < topicCount; ++i) {
      topicProcedures.get(i).executeFromOperateOnDataNodes(env);
      try {
        pipeProcedures.get(i).executeFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to create or start pipe task for subscription on datanode, because {}",
            e.getMessage());
        throw new PipeException(e.getMessage());
      }
    }
  }

  @Override
  protected void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromUnlock");
  }

  @Override
  protected void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromLock");
    env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator().unlock();
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

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
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    int topicCount = topicProcedures.size();
    for (int i = topicCount - 1; i >= 0; --i) {
      try {
        pipeProcedures.get(i).rollbackFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to roll back create or start pipe task for subscription on datanode, because {}",
            e.getMessage());
        throw new PipeException(e.getMessage());
      }
      topicProcedures.get(i).rollbackFromOperateOnDataNodes(env);
    }

    consumerGroupProcedure.rollbackFromOperateOnDataNodes(env);
  }
}
