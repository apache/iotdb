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

import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
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

public class DropTopicProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTopicProcedure.class);

  private String topicName;

  public DropTopicProcedure() {
    super();
  }

  public DropTopicProcedure(String topicName) {
    super();
    this.topicName = topicName;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.DROP_TOPIC;
  }

  @Override
  protected void executeFromValidate(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropTopicProcedure: executeFromValidate({})", topicName);

    try {
      subscriptionInfo.get().validateBeforeDroppingTopic(topicName);
    } catch (PipeException e) {
      LOGGER.warn(e.getMessage());
      setFailure(new ProcedureException(e.getMessage()));
      throw e;
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropTopicProcedure: executeFromOperateOnDataNodes({})", topicName);
    if (RpcUtils.squashResponseStatusList(env.dropSingleTopicOnDataNode(topicName)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(String.format("Failed to drop topic %s on data nodes", topicName));
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropTopicProcedure: executeFromOperateOnConfigNodes({})", topicName);

    try {
      env.getConfigManager().getConsensusManager().write(new DropTopicPlan(topicName));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropTopicProcedure: rollbackFromValidate({})", topicName);
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropTopicProcedure: rollbackFromCreateOnConfigNodes({})", topicName);
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropTopicProcedure: rollbackFromCreateOnDataNodes({})", topicName);
  }
}
