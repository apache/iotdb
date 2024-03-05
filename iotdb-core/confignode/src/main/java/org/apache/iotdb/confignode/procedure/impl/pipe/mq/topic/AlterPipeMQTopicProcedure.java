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

import org.apache.iotdb.confignode.manager.pipe.mq.coordinator.topic.PipeMQTopicCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.mq.AbstractOperatePipeMQProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.AlterPipeProcedureV2;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterPipeMQTopicProcedure extends AbstractOperatePipeMQProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterPipeMQTopicProcedure.class);

  private TAlterTopicReq alterTopicReq;


  public AlterPipeMQTopicProcedure() {
    super();
  }

  public AlterPipeMQTopicProcedure(TAlterTopicReq alterTopicReq) {
    super();
    this.alterTopicReq = alterTopicReq;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.ALTER_TOPIC;
  }

  @Override
  protected void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterPipeMQTopicProcedure: executeFromLock, try to acquire pipeMQ lock");

    final PipeMQTopicCoordinator pipeMQTopicCoordinator =
        env.getConfigManager().getMQManager().getPipeMQTopicCoordinator();

    pipeMQTopicCoordinator.lock();

    // check if the topic exists
// todo
    try {
      pipeMQTopicCoordinator.getPipeMQInfo().validateBeforeAlteringTopic(alterTopicReq);
    } catch (PipeException e) {
      // if the pipe  is a built-in plugin, we should not drop it
      LOGGER.error("AlterPipeMQTopicProcedure: executeFromLock, validateBeforeAlteringTopic failed", e);
      throw e;
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {

  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {

  }

  @Override
  protected void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {

  }

  @Override
  protected void rollbackFromLock(ConfigNodeProcedureEnv env) {

  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {

  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {

  }
}
