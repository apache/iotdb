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
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.sync.OperatePipeState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** This procedure manage three kinds of PIPE operations: CREATE, START and STOP */
abstract class AbstractOperatePipeProcedureV2
    extends StateMachineProcedure<ConfigNodeProcedureEnv, OperatePipeState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractOperatePipeProcedureV2.class);

  private static final int RETRY_THRESHOLD = 3;

  /**
   * Execute at state VALIDATE_TASK
   *
   * @return true if procedure can finish directly
   */
  abstract boolean validateTask(ConfigNodeProcedureEnv env) throws PipeException, PipeSinkException;

  /** Execute at state PRE_OPERATE_PIPE_CONFIGNODE */
  abstract void calculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state WRITE_CONFIG_NODE_CONSENSUS */
  abstract void writeConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state OPERATE_ON_DATA_NODES */
  abstract void operateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException, IOException;

  abstract PipeTaskOperation getOperation();

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case VALIDATE_TASK:
          env.getConfigManager().getPipeManager().getPipeTaskCoordinator().lock();
          if (!validateTask(env)) {
            env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
            return Flow.NO_MORE_STATE;
          }
          setNextState(OperatePipeState.CALCULATE_INFO_FOR_TASK);
          break;
        case CALCULATE_INFO_FOR_TASK:
          calculateInfoForTask(env);
          setNextState(OperatePipeState.WRITE_CONFIG_NODE_CONSENSUS);
          break;
        case WRITE_CONFIG_NODE_CONSENSUS:
          writeConfigNodeConsensus(env);
          setNextState(OperatePipeState.OPERATE_ON_DATA_NODES);
          break;
        case OPERATE_ON_DATA_NODES:
          operateOnDataNodes(env);
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
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv env) {
    env.getSchedulerLock().lock();
    try {
      if (env.getPipeLock().tryLock(this)) {
        LOGGER.info("procedureId {} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      env.getPipeLock().waitProcedure(this);
      LOGGER.info("procedureId {} wait for lock.", getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      env.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv env) {
    env.getSchedulerLock().lock();
    try {
      LOGGER.info("procedureId {} release lock.", getProcId());
      if (env.getPipeLock().releaseLock(this)) {
        env.getPipeLock().wakeWaitingProcedures(env.getScheduler());
      }
    } finally {
      env.getSchedulerLock().unlock();
    }
  }

  @Override
  protected OperatePipeState getState(int stateId) {
    return OperatePipeState.values()[stateId];
  }

  @Override
  protected int getStateId(OperatePipeState state) {
    return state.ordinal();
  }

  @Override
  protected OperatePipeState getInitialState() {
    return OperatePipeState.VALIDATE_TASK;
  }
}
