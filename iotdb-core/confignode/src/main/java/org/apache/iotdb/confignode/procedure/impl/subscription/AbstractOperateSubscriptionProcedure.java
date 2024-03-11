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

package org.apache.iotdb.confignode.procedure.impl.subscription;

import org.apache.iotdb.confignode.persistence.pipe.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.state.subscription.OperateSubscriptionState;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractOperateSubscriptionProcedure
    extends AbstractNodeProcedure<OperateSubscriptionState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractOperateSubscriptionProcedure.class);
  protected AtomicReference<SubscriptionInfo> subscriptionInfo;
  private static final int RETRY_THRESHOLD = 1;

  protected abstract PipeTaskOperation getOperation();

  protected abstract void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException;

  protected abstract void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws PipeException;

  protected abstract void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException;

  protected abstract void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperateSubscriptionState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {

    try {
      switch (state) {
        case LOCK:
          executeFromLock(env);
          setNextState(OperateSubscriptionState.OPERATE_ON_CONFIG_NODES);
          break;
        case OPERATE_ON_CONFIG_NODES:
          executeFromOperateOnConfigNodes(env);
          setNextState(OperateSubscriptionState.OPERATE_ON_DATA_NODES);
          break;
        case OPERATE_ON_DATA_NODES:
          executeFromOperateOnDataNodes(env);
          setNextState(OperateSubscriptionState.UNLOCK);
          break;
        case UNLOCK:
          executeFromUnlock(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Unknown state during executing operateSubscriptionProcedure, %s", state));
      }
    } catch (Exception e) {
      // Retry before rollback
      if (getCycles() < RETRY_THRESHOLD) {
        LOGGER.warn(
            "ProcedureId {}: Encountered error when trying to {} at state [{}], retry [{}/{}]",
            getProcId(),
            getOperation(),
            state,
            getCycles() + 1,
            RETRY_THRESHOLD,
            e);
        // Wait 3s for next retry
        TimeUnit.MILLISECONDS.sleep(3000L);
      } else {
        LOGGER.warn(
            "ProcedureId {}: All {} retries failed when trying to {} at state [{}], will rollback...",
            getProcId(),
            RETRY_THRESHOLD,
            getOperation(),
            state,
            e);
        setFailure(
            new ProcedureException(
                String.format(
                    "ProcedureId %s: Fail to %s because %s",
                    getProcId(), getOperation().name(), e.getMessage())));
      }
    }

    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperateSubscriptionState state)
      throws IOException, InterruptedException, ProcedureException {
    if (subscriptionInfo == null) {
      LOGGER.warn(
          "ProcedureId {}: Subscription lock is not acquired, rollbackState({})'s execution will be skipped.",
          getProcId(),
          state);
      return;
    }

    switch (state) {
      case LOCK:
        try {
          rollbackFromLock(env);
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from state [{}], because {}",
              getProcId(),
              state,
              e.getMessage());
        }
        break;
      case OPERATE_ON_CONFIG_NODES:
        try {
          rollbackFromOperateOnConfigNodes(env);
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from state [{}], because {}",
              getProcId(),
              state,
              e.getMessage());
        }
        break;
      case OPERATE_ON_DATA_NODES:
        try {
          rollbackFromOperateOnDataNodes(env);
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from state [{}], because {}",
              getProcId(),
              state,
              e.getMessage());
        }
        break;
    }
  }

  protected abstract void rollbackFromLock(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env);

  @Override
  protected OperateSubscriptionState getState(int stateId) {
    return OperateSubscriptionState.values()[stateId];
  }

  @Override
  protected int getStateId(OperateSubscriptionState state) {
    return state.ordinal();
  }

  @Override
  protected OperateSubscriptionState getInitialState() {
    return OperateSubscriptionState.LOCK;
  }
}
