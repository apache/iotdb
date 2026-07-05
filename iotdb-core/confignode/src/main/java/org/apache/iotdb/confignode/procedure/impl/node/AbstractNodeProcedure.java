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

package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Node procedure. */
public abstract class AbstractNodeProcedure<TState>
    extends StateMachineProcedure<ConfigNodeProcedureEnv, TState> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNodeProcedure.class);

  protected AbstractNodeProcedure() {
    super();
  }

  protected AbstractNodeProcedure(boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      if (configNodeProcedureEnv.getNodeLock().tryLock(this)) {
        LOG.info(ProcedureMessages.LOG_PROCEDUREID_ARG_ACQUIRE_LOCK_3FBF9987, getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      LOG.info(
          ProcedureMessages
              .LOG_PROCEDUREID_ARG_ACQUIRE_LOCK_FAILED_WILL_WAIT_LOCK_AFTER_FINISHING_3B27278E,
          getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void waitForLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      configNodeProcedureEnv
          .getNodeLock()
          .waitProcedure(this, configNodeProcedureEnv.getScheduler());
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOG.info(ProcedureMessages.LOG_PROCEDUREID_ARG_RELEASE_LOCK_FF860D6B, getProcId());
      if (configNodeProcedureEnv.getNodeLock().releaseLock(this)) {
        configNodeProcedureEnv
            .getNodeLock()
            .wakeWaitingProcedures(configNodeProcedureEnv.getScheduler());
      }
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }
}
