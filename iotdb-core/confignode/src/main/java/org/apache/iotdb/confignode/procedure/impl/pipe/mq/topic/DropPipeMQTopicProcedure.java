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

package org.apache.iotdb.confignode.procedure.impl.pipe.mq.topic;

import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.DropPipeMQTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.manager.pipe.mq.coordinator.PipeMQCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.mq.AbstractOperatePipeMQProcedure;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropPipeMQTopicProcedure extends AbstractOperatePipeMQProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropPipeMQTopicProcedure.class);

  private String topicName;

  public DropPipeMQTopicProcedure(String topicName) {
    super();
    this.topicName = topicName;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.DROP_TOPIC;
  }

  @Override
  protected void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeMQTopicProcedure: executeFromLock({})", topicName);
    final PipeMQCoordinator pipeMQCoordinator =
        env.getConfigManager().getMQManager().getPipeMQCoordinator();

    pipeMQCoordinator.lock();

    try {
      pipeMQCoordinator.getPipeMQInfo().validateBeforeDroppingTopic(topicName);
    } catch (PipeException e) {
      LOGGER.warn(e.getMessage());
      setFailure(new ProcedureException(e.getMessage()));
      pipeMQCoordinator.unlock();
      throw e;
    }

    try {
      env.getConfigManager().getConsensusManager().write(new DropPipePluginPlan(topicName));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeMQTopicProcedure: executeFromOperateOnDataNodes({})", topicName);
    if (RpcUtils.squashResponseStatusList(env.dropSinglePipeMQTopicOnDataNode(topicName)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format("Failed to drop pipe mq topic %s on data nodes", topicName));
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeMQTopicProcedure: executeFromOperateOnConfigNodes({})", topicName);

    try {
      env.getConfigManager().getConsensusManager().write(new DropPipeMQTopicPlan(topicName));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }
  }

  @Override
  protected void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeMQTopicProcedure: executeFromUnlock({})", topicName);

    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  protected void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeMQTopicProcedure: rollbackFromLock({})", topicName);
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeMQTopicProcedure: rollbackFromCreateOnConfigNodes({})", topicName);
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeMQTopicProcedure: rollbackFromCreateOnDataNodes({})", topicName);
  }
}
