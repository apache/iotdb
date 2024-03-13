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

package org.apache.iotdb.confignode.procedure.impl.subscription.topic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
import org.apache.iotdb.confignode.manager.subscription.coordinator.SubscriptionCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateTopicProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTopicProcedure.class);

  private TCreateTopicReq createTopicReq;
  private TopicMeta topicMeta;

  public CreateTopicProcedure() {
    super();
  }

  public CreateTopicProcedure(TCreateTopicReq createTopicReq) throws PipeException {
    super();
    this.createTopicReq = createTopicReq;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_TOPIC;
  }

  @Override
  protected void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateTopicProcedure: executeFromLock, try to acquire subscription lock");

    final SubscriptionCoordinator subscriptionCoordinator =
        env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator();

    subscriptionCoordinator.lock();

    // 1. check if the topic exists
    try {
      subscriptionCoordinator.getSubscriptionInfo().validateBeforeCreatingTopic(createTopicReq);
    } catch (PipeException e) {
      // The topic has already created, we should end the procedure
      LOGGER.warn(
          "Topic {} is already created, end the CreateTopicProcedure({})",
          createTopicReq.getTopicName(),
          createTopicReq.getTopicName());
      setFailure(new ProcedureException(e.getMessage()));
      subscriptionCoordinator.unlock();
      throw e;
    }

    // 2. create the topic meta
    topicMeta =
        new TopicMeta(
            createTopicReq.getTopicName(),
            System.currentTimeMillis(),
            createTopicReq.getTopicAttributes());
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreateTopicProcedure: executeFromOperateOnConfigNodes({})", topicMeta.getTopicName());

    TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(new CreateTopicPlan(topicMeta));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreateTopicProcedure: executeFromOperateOnDataNodes({})", topicMeta.getTopicName());

    try {
      if (RpcUtils.squashResponseStatusList(env.pushSingleTopicOnDataNode(topicMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the topic meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to create topic instance [%s] on data nodes", topicMeta.getTopicName()));
  }

  @Override
  protected void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateTopicProcedure: executeFromUnlock({})", topicMeta.getTopicName());
    env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator().unlock();
  }

  @Override
  protected void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateTopicProcedure: rollbackFromLock({})", topicMeta.getTopicName());
    env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator().unlock();
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreateTopicProcedure: rollbackFromCreateOnConfigNodes({})", topicMeta.getTopicName());

    try {
      env.getConfigManager()
          .getConsensusManager()
          .write(new DropTopicPlan(topicMeta.getTopicName()));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreateTopicProcedure: rollbackFromCreateOnDataNodes({})", topicMeta.getTopicName());

    if (RpcUtils.squashResponseStatusList(env.dropSingleTopicOnDataNode(topicMeta.getTopicName()))
            .getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format("Failed to rollback topic [%s] on data nodes", topicMeta.getTopicName()));
    }
  }
}
