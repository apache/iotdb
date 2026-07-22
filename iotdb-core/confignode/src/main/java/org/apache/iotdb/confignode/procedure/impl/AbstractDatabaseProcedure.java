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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;

import java.util.Set;

/** A procedure that holds exclusive lifecycle locks for its databases until it finishes. */
public abstract class AbstractDatabaseProcedure<T>
    extends StateMachineProcedure<ConfigNodeProcedureEnv, T> {

  private String waitingDatabase;

  protected AbstractDatabaseProcedure() {
    super();
  }

  protected AbstractDatabaseProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  protected abstract Set<String> getDatabaseNames();

  @Override
  protected ProcedureLockState acquireLock(final ConfigNodeProcedureEnv env) {
    waitingDatabase = env.tryLockDatabases(this, getDatabaseNames());
    return waitingDatabase == null
        ? ProcedureLockState.LOCK_ACQUIRED
        : ProcedureLockState.LOCK_EVENT_WAIT;
  }

  @Override
  protected void waitForLock(final ConfigNodeProcedureEnv env) {
    if (waitingDatabase != null) {
      env.waitDatabaseLock(this, waitingDatabase);
    }
  }

  @Override
  protected void releaseLock(final ConfigNodeProcedureEnv env) {
    env.releaseDatabaseLocks(this, getDatabaseNames());
  }

  @Override
  protected boolean holdLock(final ConfigNodeProcedureEnv env) {
    return true;
  }
}
