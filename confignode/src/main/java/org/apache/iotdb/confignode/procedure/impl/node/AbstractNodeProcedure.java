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

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Node procedure */
public abstract class AbstractNodeProcedure<TState>
    extends StateMachineProcedure<ConfigNodeProcedureEnv, TState> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNodeProcedure.class);

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      if (configNodeProcedureEnv.getNodeLock().tryLock(this)) {
        LOG.info("procedureId {} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      configNodeProcedureEnv.getNodeLock().waitProcedure(this);
      LOG.info("procedureId {} wait for lock.", getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOG.info("procedureId {} release lock.", getProcId());
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
