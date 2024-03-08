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
import org.apache.iotdb.commons.pipe.mq.meta.PipeMQTopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.AlterPipeMQTopicPlan;
import org.apache.iotdb.confignode.manager.pipe.mq.coordinator.PipeMQCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.mq.AbstractOperatePipeMQProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTopicReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class AlterPipeMQTopicProcedure extends AbstractOperatePipeMQProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterPipeMQTopicProcedure.class);

  private TAlterTopicReq alterTopicReq;

  private PipeMQTopicMeta updatedPipeMQTopicMeta;

  private PipeMQTopicMeta pipeMQTopicMeta;

  public AlterPipeMQTopicProcedure() {
    super();
  }

  public AlterPipeMQTopicProcedure(TAlterTopicReq alterTopicReq) {
    super();
    this.alterTopicReq = alterTopicReq;
  }

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

    // check if the topic exists
    // todo
    try {
      pipeMQCoordinator.getPipeMQInfo().validateBeforeAlteringTopic(alterTopicReq);
    } catch (PipeException e) {
      LOGGER.error(
          "AlterPipeMQTopicProcedure: executeFromLock, validateBeforeAlteringTopic failed", e);
      throw e;
    }
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterPipeMQTopicProcedure: executeFromOperateOnConfigNodes, try to alter topic");

    if (updatedPipeMQTopicMeta == null) {
      // updatedPipeMQTopicMeta == null means this procedure is constructed from TAlterTopicReq.
      updatedPipeMQTopicMeta =
          new PipeMQTopicMeta(
              alterTopicReq.getTopicName(),
              System.currentTimeMillis(),
              new HashMap<>(alterTopicReq.getTopicAttributes()));
    }

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
        alterTopicReq.getTopicName());

    // do nothing
  }

  @Override
  public void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterPipeMQTopicProcedure: executeFromUnlock({})", alterTopicReq.getTopicName());
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  public void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterPipeMQTopicProcedure: rollbackFromLock({})", alterTopicReq.getTopicName());
    env.getConfigManager().getMQManager().getPipeMQCoordinator().unlock();
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    // do nothing
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    // do nothing
  }
}
