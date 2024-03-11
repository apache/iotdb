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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.PipeMQTopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.AlterPipeMQTopicPlan;
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

import java.io.IOException;

public class AlterPipeMQTopicProcedure extends AbstractOperatePipeMQProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterPipeMQTopicProcedure.class);

  private PipeMQTopicMeta updatedPipeMQTopicMeta;

  private PipeMQTopicMeta existedPipeMQTopicMeta;

  public AlterPipeMQTopicProcedure(PipeMQTopicMeta updatedTopicMeta) {
    super();
    this.updatedPipeMQTopicMeta = updatedTopicMeta;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.ALTER_TOPIC;
  }

  @Override
  public void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterPipeMQTopicProcedure: executeFromLock, try to acquire pipeMQ lock");

    final PipeMQCoordinator pipeMQCoordinator =
        env.getConfigManager().getMQManager().getPipeMQCoordinator();

    pipeMQCoordinator.lock();

    validateOldAndNewPipeMQTopicMeta(pipeMQCoordinator);
  }

  public void validateOldAndNewPipeMQTopicMeta(PipeMQCoordinator pipeMQCoordinator) {
    try {
      pipeMQCoordinator.getPipeMQInfo().validateBeforeAlteringTopic(updatedPipeMQTopicMeta);
    } catch (PipeException e) {
      LOGGER.error(
          "AlterPipeMQTopicProcedure: executeFromLock, validateBeforeAlteringTopic failed", e);
      setFailure(new ProcedureException(e.getMessage()));
      pipeMQCoordinator.unlock();
      throw e;
    }

    this.existedPipeMQTopicMeta =
        pipeMQCoordinator.getPipeMQInfo().getPipeMQTopicMeta(updatedPipeMQTopicMeta.getTopicName());
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterPipeMQTopicProcedure: executeFromOperateOnConfigNodes, try to alter topic");

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterPipeMQTopicPlan(updatedPipeMQTopicMeta));
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
        "AlterPipeMQTopicProcedure: executeFromOperateOnDataNodes({})",
        updatedPipeMQTopicMeta.getTopicName());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSinglePipeMQTopicOnDataNode(updatedPipeMQTopicMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the pipe mq topic meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to push the pipe mq topic meta to data nodes, topic name: %s",
            updatedPipeMQTopicMeta.getTopicName()));
  }

  @Override
  public void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterPipeMQTopicProcedure: executeFromUnlock({})", updatedPipeMQTopicMeta.getTopicName());
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  public void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeMQTopicProcedure: rollbackFromLock({})", updatedPipeMQTopicMeta.getTopicName());
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeMQTopicProcedure: rollbackFromOperateOnConfigNodes({})",
        updatedPipeMQTopicMeta.getTopicName());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSinglePipeMQTopicOnDataNode(existedPipeMQTopicMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the pipe mq topic meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to push the pipe mq topic meta to data nodes, topic name: %s",
            updatedPipeMQTopicMeta.getTopicName()));
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeMQTopicProcedure: rollbackFromOperateOnDataNodes({})",
        updatedPipeMQTopicMeta.getTopicName());
    // do nothing
  }
}
