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
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.manager.subscription.coordinator.SubscriptionCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AlterTopicProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterTopicProcedure.class);

  private TopicMeta updatedTopicMeta;

  private TopicMeta existedTopicMeta;

  public AlterTopicProcedure(TopicMeta updatedTopicMeta) {
    super();
    this.updatedTopicMeta = updatedTopicMeta;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.ALTER_TOPIC;
  }

  @Override
  public void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterTopicProcedure: executeFromLock, try to acquire subcription lock");

    final SubscriptionCoordinator subscriptionCoordinator =
        env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator();

    subscriptionCoordinator.lock();

    validateOldAndNewTopicMeta(subscriptionCoordinator);
  }

  public void validateOldAndNewTopicMeta(SubscriptionCoordinator subscriptionCoordinator) {
    try {
      subscriptionCoordinator.getSubscriptionInfo().validateBeforeAlteringTopic(updatedTopicMeta);
    } catch (PipeException e) {
      LOGGER.error("AlterTopicProcedure: executeFromLock, validateBeforeAlteringTopic failed", e);
      setFailure(new ProcedureException(e.getMessage()));
      subscriptionCoordinator.unlock();
      throw e;
    }

    this.existedTopicMeta =
        subscriptionCoordinator.getSubscriptionInfo().getTopicMeta(updatedTopicMeta.getTopicName());
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterTopicProcedure: executeFromOperateOnConfigNodes, try to alter topic");

    TSStatus response;
    try {
      response =
          env.getConfigManager().getConsensusManager().write(new AlterTopicPlan(updatedTopicMeta));
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
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterTopicProcedure: executeFromOperateOnDataNodes({})", updatedTopicMeta.getTopicName());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSingleTopicOnDataNode(updatedTopicMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the topic meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to push the topic meta to data nodes, topic name: %s",
            updatedTopicMeta.getTopicName()));
  }

  @Override
  public void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterTopicProcedure: executeFromUnlock({})", updatedTopicMeta.getTopicName());
    env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator().unlock();
  }

  @Override
  public void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterTopicProcedure: rollbackFromLock({})", updatedTopicMeta.getTopicName());
    env.getConfigManager().getSubscriptionManager().getSubscriptionCoordinator().unlock();
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterTopicProcedure: rollbackFromOperateOnConfigNodes({})",
        updatedTopicMeta.getTopicName());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSingleTopicOnDataNode(existedTopicMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the topic meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to push the topic meta to data nodes, topic name: %s",
            updatedTopicMeta.getTopicName()));
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterTopicProcedure: rollbackFromOperateOnDataNodes({})", updatedTopicMeta.getTopicName());
    // do nothing
  }
}
