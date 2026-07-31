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

package org.apache.iotdb.confignode.procedure.scheduler;

import org.apache.iotdb.confignode.procedure.Procedure;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicReference;

/** Lock Queue for procedure of the same type */
public class LockQueue<Env> {
  private final ArrayDeque<Procedure<Env>> deque = new ArrayDeque<>();

  private final AtomicReference<Procedure<Env>> lockOwnerProcedure = new AtomicReference<>();

  public boolean tryLock(Procedure<Env> procedure) {
    final Procedure<Env> currentLockOwnerProcedure = lockOwnerProcedure.get();
    if (currentLockOwnerProcedure == null) {
      lockOwnerProcedure.set(procedure);
      return true;
    }
    return procedure.getProcId() == currentLockOwnerProcedure.getProcId();
  }

  public boolean releaseLock(Procedure<Env> procedure) {
    final Procedure<Env> currentLockOwnerProcedure = lockOwnerProcedure.get();
    if (currentLockOwnerProcedure == null
        || currentLockOwnerProcedure.getProcId() != procedure.getProcId()) {
      return false;
    }
    lockOwnerProcedure.set(null);
    return true;
  }

  public Procedure<Env> getLockOwnerProcedure() {
    return lockOwnerProcedure.get();
  }

  public void waitProcedure(Procedure<Env> procedure, ProcedureScheduler procedureScheduler) {
    if (lockOwnerProcedure.get() == null) {
      procedureScheduler.addFront(procedure);
      return;
    }
    if (deque.stream()
        .anyMatch(waitingProcedure -> waitingProcedure.getProcId() == procedure.getProcId())) {
      return;
    }
    deque.addLast(procedure);
  }

  public int wakeWaitingProcedures(ProcedureScheduler procedureScheduler) {
    int count = deque.size();
    while (!deque.isEmpty()) {
      procedureScheduler.addFront(deque.pollFirst());
    }
    return count;
  }
}
