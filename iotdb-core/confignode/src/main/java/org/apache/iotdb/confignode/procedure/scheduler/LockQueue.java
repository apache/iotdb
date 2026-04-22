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
import java.util.HashSet;
import java.util.Set;

/**
 * Lock queue for procedures. Supports both exclusive mode (maxPermits=1, the default) and semaphore
 * mode (maxPermits>1) for concurrent lock holders.
 *
 * <p>When maxPermits <= 0, no limit is enforced and {@link #tryLock} always returns true.
 */
public class LockQueue {
  private final ArrayDeque<Procedure<?>> waitingQueue = new ArrayDeque<>();
  private final Set<Long> lockOwnerProcedureIds = new HashSet<>();
  private volatile int maxPermits;

  /** Creates an exclusive lock queue (maxPermits = 1). */
  public LockQueue() {
    this(1);
  }

  /**
   * Creates a lock queue allowing up to {@code maxPermits} concurrent holders.
   *
   * @param maxPermits maximum concurrent lock holders; <= 0 means no limit
   */
  public LockQueue(int maxPermits) {
    this.maxPermits = maxPermits;
  }

  public synchronized boolean tryLock(Procedure<?> procedure) {
    // No limit mode
    if (maxPermits <= 0) {
      return true;
    }
    // Reentrant check
    if (lockOwnerProcedureIds.contains(procedure.getProcId())) {
      return true;
    }
    if (lockOwnerProcedureIds.size() < maxPermits) {
      lockOwnerProcedureIds.add(procedure.getProcId());
      return true;
    }
    return false;
  }

  public synchronized boolean releaseLock(Procedure<?> procedure) {
    return lockOwnerProcedureIds.remove(procedure.getProcId());
  }

  public synchronized void waitProcedure(Procedure<?> procedure) {
    waitingQueue.addLast(procedure);
  }

  public synchronized int wakeWaitingProcedures(ProcedureScheduler procedureScheduler) {
    int count = waitingQueue.size();
    while (!waitingQueue.isEmpty()) {
      procedureScheduler.addFront(waitingQueue.pollFirst());
    }
    return count;
  }

  public void setMaxPermits(int maxPermits) {
    this.maxPermits = maxPermits;
  }

  public int getMaxPermits() {
    return maxPermits;
  }
}
