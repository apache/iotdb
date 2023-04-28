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
package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.task.OperatePipeTaskState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This procedure manage 4 kinds of PIPE operations: CREATE, START, STOP and DROP.
 *
 * <p>This class extends AbstractNodeProcedure to make sure that pipe task procedures can be
 * executed in sequence and node procedures can be locked when a pipe task procedure is running.
 */
abstract class AbstractOperatePipeProcedureV2 extends AbstractNodeProcedure<OperatePipeTaskState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractOperatePipeProcedureV2.class);

  private static final int RETRY_THRESHOLD = 3;

  abstract PipeTaskOperation getOperation();

  /**
   * Execute at state VALIDATE_TASK
   *
   * @return true if procedure can finish directly
   */
  abstract boolean executeFromValidateTask(ConfigNodeProcedureEnv env)
      throws PipeException, PipeSinkException;

  /** Execute at state CALCULATE_INFO_FOR_TASK */
  abstract void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state WRITE_CONFIG_NODE_CONSENSUS */
  abstract void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException;

  /** Execute at state OPERATE_ON_DATA_NODES */
  abstract void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperatePipeTaskState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case VALIDATE_TASK:
          env.getConfigManager().getPipeManager().getPipeTaskCoordinator().lock();
          if (!executeFromValidateTask(env)) {
            env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
            return Flow.NO_MORE_STATE;
          }
          setNextState(OperatePipeTaskState.CALCULATE_INFO_FOR_TASK);
          break;
        case CALCULATE_INFO_FOR_TASK:
          executeFromCalculateInfoForTask(env);
          setNextState(OperatePipeTaskState.WRITE_CONFIG_NODE_CONSENSUS);
          break;
        case WRITE_CONFIG_NODE_CONSENSUS:
          executeFromWriteConfigNodeConsensus(env);
          setNextState(OperatePipeTaskState.OPERATE_ON_DATA_NODES);
          break;
        case OPERATE_ON_DATA_NODES:
          executeFromOperateOnDataNodes(env);
          env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
          return Flow.NO_MORE_STATE;
      }
    } catch (PipeException | PipeSinkException | IOException e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in OperatePipeProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error("Retrievable error trying to {} at state [{}]", getOperation(), state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format("Fail to %s because %s", getOperation().name(), e.getMessage())));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean isRollbackSupported(OperatePipeTaskState state) {
    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeTaskState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case VALIDATE_TASK:
        rollbackFromValidateTask(env);
        env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
        break;
      case CALCULATE_INFO_FOR_TASK:
        rollbackFromCalculateInfoForTask(env);
        break;
      case WRITE_CONFIG_NODE_CONSENSUS:
        rollbackFromWriteConfigNodeConsensus(env);
        break;
      case OPERATE_ON_DATA_NODES:
        rollbackFromOperateOnDataNodes(env);
        break;
      default:
        LOGGER.error("Unsupported roll back STATE [{}]", state);
    }
  }

  protected abstract void rollbackFromValidateTask(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws IOException;

  @Override
  protected OperatePipeTaskState getState(int stateId) {
    return OperatePipeTaskState.values()[stateId];
  }

  @Override
  protected int getStateId(OperatePipeTaskState state) {
    return state.ordinal();
  }

  @Override
  protected OperatePipeTaskState getInitialState() {
    return OperatePipeTaskState.VALIDATE_TASK;
  }
}
