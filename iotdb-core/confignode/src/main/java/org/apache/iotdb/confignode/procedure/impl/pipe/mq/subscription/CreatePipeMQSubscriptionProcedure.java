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

package org.apache.iotdb.confignode.procedure.impl.pipe.mq.subscription;

import org.apache.iotdb.commons.pipe.mq.meta.PipeMQConsumerGroupMeta;
import org.apache.iotdb.commons.pipe.mq.meta.PipeMQTopicMeta;
import org.apache.iotdb.confignode.manager.pipe.mq.coordinator.PipeMQCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.mq.AbstractOperatePipeMQProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.mq.consumer.AlterPipeMQConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.mq.topic.AlterPipeMQTopicProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreatePipeMQSubscriptionProcedure extends AbstractOperatePipeMQProcedure {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(CreatePipeMQSubscriptionProcedure.class);

  private TSubscribeReq subscribeReq;

  private AlterPipeMQConsumerGroupProcedure consumerGroupProcedure;
  private List<AlterPipeMQTopicProcedure> topicProcedures = new ArrayList<>();
  private List<AbstractOperatePipeProcedureV2> pipeProcedures = new ArrayList<>();

  public CreatePipeMQSubscriptionProcedure(TSubscribeReq subscribeReq) {
    this.subscribeReq = subscribeReq;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_SUBSCRIPTION;
  }

  @Override
  protected void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreatePipeMQSubscriptionProcedure: executeFromLock, try to acquire pipeMQ lock");

    final PipeMQCoordinator pipeMQCoordinator =
        env.getConfigManager().getMQManager().getPipeMQCoordinator();

    pipeMQCoordinator.lock();

    // check if the consumer and all topics exists
    try {
      pipeMQCoordinator.getPipeMQInfo().validateBeforeSubscribe(subscribeReq);
    } catch (PipeException e) {
      LOGGER.error(
          "CreatePipeMQSubscriptionProcedure: executeFromLock, validateBeforeSubscribe failed", e);
      throw e;
    }

    // Construct AlterPipeMQConsumerGroupProcedure
    PipeMQConsumerGroupMeta updatedConsumerGroupMeta =
        pipeMQCoordinator
            .getPipeMQInfo()
            .getPipeMQConsumerGroupMeta(subscribeReq.getConsumerGroupId())
            .copy();
    updatedConsumerGroupMeta.addSubscription(
        subscribeReq.getConsumerId(), subscribeReq.getTopicNames());
    consumerGroupProcedure = new AlterPipeMQConsumerGroupProcedure(updatedConsumerGroupMeta);

    for (String topic : subscribeReq.getTopicNames()) {
      // Construct AlterPipeMQTopicProcedures
      PipeMQTopicMeta updatedTopicMeta =
          pipeMQCoordinator.getPipeMQInfo().getPipeMQTopicMeta(topic).copy();

      // Construct pipe procedures
      if (updatedTopicMeta.addSubscribedConsumerGroup(subscribeReq.getConsumerGroupId())) {
        // Consumer group not subscribed this topic
        // TODO: now all configs are put to extractor attributes. need to put to processor and
        // connector attributes correctly.
        pipeProcedures.add(
            new CreatePipeProcedureV2(
                new TCreatePipeReq()
                    .setPipeName(topic + "_" + subscribeReq.getConsumerGroupId())
                    .setExtractorAttributes(updatedTopicMeta.getConfig().getAttributes())));
      } else {
        // Consumer group already subscribed this topic
        pipeProcedures.add(
            new StartPipeProcedureV2(topic + "_" + subscribeReq.getConsumerGroupId()));
      }

      topicProcedures.add(new AlterPipeMQTopicProcedure(updatedTopicMeta));
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreatePipeMQSubscriptionProcedure: executeFromOperateOnConfigNodes");

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
    LOGGER.info("CreatePipeMQSubscriptionProcedure: executeFromOperateOnDataNodes");

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
    LOGGER.info("CreatePipeMQSubscriptionProcedure: executeFromUnlock");
  }

  @Override
  protected void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreatePipeMQSubscriptionProcedure: rollbackFromLock");
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreatePipeMQSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

    int topicCount = topicProcedures.size();
    for (int i = topicCount; i >= 0; --i) {
      pipeProcedures.get(i).rollbackFromWriteConfigNodeConsensus(env);
      topicProcedures.get(i).rollbackFromOperateOnConfigNodes(env);
    }

    consumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);

    for (int i = topicCount; i >= 0; --i) {
      pipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }

    for (int i = topicCount; i >= 0; --i) {
      pipeProcedures.get(i).executeFromValidateTask(env);
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreatePipeMQSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    int topicCount = topicProcedures.size();
    for (int i = topicCount; i >= 0; --i) {
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
