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
package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.sync.OperatePipeState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This procedure manage three kinds of PIPE operations: CREATE, START and STOP */
abstract class AbstractOperatePipeProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, OperatePipeState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOperatePipeProcedure.class);

  private static final int RETRY_THRESHOLD = 3;

  /**
   * Execute at state OPERATE_CHECK
   *
   * @return true if procedure can finish directly
   */
  abstract boolean executeCheckCanSkip(ConfigNodeProcedureEnv env)
      throws PipeException, PipeSinkException;

  /** Execute at state PRE_OPERATE_PIPE_CONFIGNODE */
  abstract void executePreOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state OPERATE_PIPE_DATANODE */
  abstract void executeOperatePipeOnDataNode(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state OPERATE_PIPE_CONFIGNODE */
  abstract void executeOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException;

  abstract SyncOperation getOperation();

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case OPERATE_CHECK:
          env.getConfigManager().getSyncManager().lockSyncMetadata();
          if (executeCheckCanSkip(env)) {
            env.getConfigManager().getSyncManager().unlockSyncMetadata();
            return Flow.NO_MORE_STATE;
          }
          setNextState(OperatePipeState.PRE_OPERATE_PIPE_CONFIGNODE);
          break;
        case PRE_OPERATE_PIPE_CONFIGNODE:
          executePreOperatePipeOnConfigNode(env);
          setNextState(OperatePipeState.OPERATE_PIPE_DATANODE);
          break;
        case OPERATE_PIPE_DATANODE:
          executeOperatePipeOnDataNode(env);
          setNextState(OperatePipeState.OPERATE_PIPE_CONFIGNODE);
          break;
        case OPERATE_PIPE_CONFIGNODE:
          executeOperatePipeOnConfigNode(env);
          env.getConfigManager().getSyncManager().unlockSyncMetadata();
          return Flow.NO_MORE_STATE;
      }
    } catch (PipeException | PipeSinkException e) {
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
    return OperatePipeState.OPERATE_CHECK;
  }
}
