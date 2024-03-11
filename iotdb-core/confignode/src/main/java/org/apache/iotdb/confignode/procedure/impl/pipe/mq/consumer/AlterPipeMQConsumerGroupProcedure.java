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

package org.apache.iotdb.confignode.procedure.impl.pipe.mq.consumer;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.PipeMQConsumerGroupMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.consumer.AlterPipeMQConsumerGroupPlan;
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

public class AlterPipeMQConsumerGroupProcedure extends AbstractOperatePipeMQProcedure {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(AlterPipeMQConsumerGroupProcedure.class);
  protected PipeMQConsumerGroupMeta existingPipeMQConsumerGroupMeta;
  protected PipeMQConsumerGroupMeta updatedPipeMQConsumerGroupMeta;

  protected AlterPipeMQConsumerGroupProcedure() {
    super();
  }

  public AlterPipeMQConsumerGroupProcedure(PipeMQConsumerGroupMeta updatedPipeMQConsumerGroupMeta) {
    super();
    this.updatedPipeMQConsumerGroupMeta = updatedPipeMQConsumerGroupMeta;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.ALTER_CONSUMER_GROUP;
  }

  public void validateAndGetOldAndNewMeta(
      ConfigNodeProcedureEnv env, PipeMQCoordinator pipeMQCoordinator) {
    try {
      pipeMQCoordinator
          .getPipeMQInfo()
          .validateBeforeAlterConsumerGroup(updatedPipeMQConsumerGroupMeta);
    } catch (PipeException e) {
      // Consumer group not exist, we should end the procedure
      LOGGER.warn(
          "Consumer group {} does not exist, end the AlterPipeMQConsumerGroupProcedure",
          updatedPipeMQConsumerGroupMeta.getConsumerGroupId());
      setFailure(new ProcedureException(e.getMessage()));
      pipeMQCoordinator.unlock();
      throw e;
    }

    this.existingPipeMQConsumerGroupMeta =
        pipeMQCoordinator
            .getPipeMQInfo()
            .getPipeMQConsumerGroupMeta(updatedPipeMQConsumerGroupMeta.getConsumerGroupId());
  }

  @Override
  public void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterPipeMQConsumerGroupProcedure: executeFromLock, try to acquire pipeMQ lock");

    env.getConfigManager().getMQManager().getPipeMQCoordinator().lock();

    validateAndGetOldAndNewMeta(env, env.getConfigManager().getMQManager().getPipeMQCoordinator());
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterPipeMQConsumerGroupProcedure: executeFromOperateOnConfigNodes, try to alter consumer group");

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterPipeMQConsumerGroupPlan(updatedPipeMQConsumerGroupMeta));
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
        "AlterPipeMQConsumerGroupProcedure: executeFromOperateOnDataNodes({})",
        updatedPipeMQConsumerGroupMeta.getConsumerGroupId());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSinglePipeMQConsumerGroupOnDataNode(
                      updatedPipeMQConsumerGroupMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the pipe mq consumer group meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to create consumer group instance [%s] on data nodes",
            updatedPipeMQConsumerGroupMeta.getConsumerGroupId()));
  }

  @Override
  public void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterPipeMQConsumerGroupProcedure: executeFromUnlock");
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  public void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterPipeMQConsumerGroupProcedure: rollbackFromLock");
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterPipeMQConsumerGroupProcedure: rollbackFromOperateOnConfigNodes");

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSinglePipeMQConsumerGroupOnDataNode(
                      existingPipeMQConsumerGroupMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the pipe mq consumer group meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to create consumer group instance [%s] on data nodes",
            updatedPipeMQConsumerGroupMeta.getConsumerGroupId()));
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterPipeMQConsumerGroupProcedure: rollbackFromOperateOnDataNodes");
    // Do nothing
  }
}
