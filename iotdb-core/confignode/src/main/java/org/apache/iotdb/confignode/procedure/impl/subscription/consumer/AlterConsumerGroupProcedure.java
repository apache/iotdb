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

package org.apache.iotdb.confignode.procedure.impl.subscription.consumer;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.ConsumerGroupMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AlterConsumerGroupProcedure extends AbstractOperateSubscriptionProcedure {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AlterConsumerGroupProcedure.class);
  protected ConsumerGroupMeta existingConsumerGroupMeta;
  protected ConsumerGroupMeta updatedConsumerGroupMeta;

  protected AlterConsumerGroupProcedure() {
    super();
  }

  public AlterConsumerGroupProcedure(ConsumerGroupMeta updatedConsumerGroupMeta) {
    super();
    this.updatedConsumerGroupMeta = updatedConsumerGroupMeta;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.ALTER_CONSUMER_GROUP;
  }

  public void validateAndGetOldAndNewMeta(ConfigNodeProcedureEnv env) {
    try {
      subscriptionInfo.get().validateBeforeAlterConsumerGroup(updatedConsumerGroupMeta);
    } catch (PipeException e) {
      // Consumer group not exist, we should end the procedure
      LOGGER.warn(
          "Consumer group {} does not exist, end the AlterConsumerGroupProcedure",
          updatedConsumerGroupMeta.getConsumerGroupId());
      setFailure(new ProcedureException(e.getMessage()));
      throw e;
    }

    this.existingConsumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(updatedConsumerGroupMeta.getConsumerGroupId());
  }

  @Override
  public void executeFromValidate(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterConsumerGroupProcedure: executeFromValidate, try to validate");

    validateAndGetOldAndNewMeta(env);
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterConsumerGroupProcedure: executeFromOperateOnConfigNodes, try to alter consumer group");

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterConsumerGroupPlan(updatedConsumerGroupMeta));
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
        "AlterConsumerGroupProcedure: executeFromOperateOnDataNodes({})",
        updatedConsumerGroupMeta.getConsumerGroupId());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSingleConsumerGroupOnDataNode(updatedConsumerGroupMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the consumer group meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to create consumer group instance [%s] on data nodes",
            updatedConsumerGroupMeta.getConsumerGroupId()));
  }

  @Override
  public void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterConsumerGroupProcedure: rollbackFromValidate");
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterConsumerGroupProcedure: rollbackFromOperateOnConfigNodes");

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSingleConsumerGroupOnDataNode(existingConsumerGroupMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the consumer group meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to create consumer group instance [%s] on data nodes",
            updatedConsumerGroupMeta.getConsumerGroupId()));
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterConsumerGroupProcedure: rollbackFromOperateOnDataNodes");
    // Do nothing
  }
}
