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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The single source of database lifecycle locks in a ConfigNode.
 *
 * <p>A lock is keyed by the exact database name. Both short-lived manager requests and procedures
 * use the same ownership table, so database creation, Region creation, maintenance retries, and
 * database deletion cannot bypass one another. Procedure ownership is identified by procedure id
 * rather than worker thread because a procedure can resume on a different executor thread.
 */
public class DatabaseLockQueue {

  private final ProcedureScheduler scheduler;
  private final ReentrantLock stateLock = new ReentrantLock(true);
  private final Condition lockReleased = stateLock.newCondition();
  private final Map<String, DatabaseLockState> lockStateMap = new HashMap<>();

  public DatabaseLockQueue(final ProcedureScheduler scheduler) {
    this.scheduler = scheduler;
  }

  /** Acquire database locks for a synchronous manager request. */
  public DatabaseLock acquireLocks(final Set<String> databaseNames) {
    final List<String> orderedDatabases = orderedDatabases(databaseNames);
    final Thread owner = Thread.currentThread();
    stateLock.lock();
    try {
      while (!canAcquireRequestLocks(owner, orderedDatabases)) {
        lockReleased.awaitUninterruptibly();
      }
      return acquireRequestLocks(orderedDatabases, owner);
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * Tries to acquire database locks for a synchronous manager request within the given timeout.
   *
   * @return the acquired lock, or null if the timeout elapsed before every lock became available
   * @throws InterruptedException if interrupted while waiting for the locks
   */
  public DatabaseLock tryAcquireLocks(
      final Set<String> databaseNames, final long timeout, final TimeUnit timeUnit)
      throws InterruptedException {
    final List<String> orderedDatabases = orderedDatabases(databaseNames);
    final Thread owner = Thread.currentThread();
    long remainingNanos = timeUnit.toNanos(timeout);
    if (!stateLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS)) {
      return null;
    }
    try {
      while (!canAcquireRequestLocks(owner, orderedDatabases)) {
        if (remainingNanos <= 0) {
          return null;
        }
        remainingNanos = lockReleased.awaitNanos(remainingNanos);
      }
      return acquireRequestLocks(orderedDatabases, owner);
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * Atomically tries to lock all databases for a procedure.
   *
   * @return the first database whose lock is unavailable, or null when all locks are acquired
   */
  public String tryLock(final Procedure<?> procedure, final Set<String> databaseNames) {
    stateLock.lock();
    try {
      final List<String> orderedDatabases = orderedDatabases(databaseNames);
      final List<String> acquiredDatabases = new ArrayList<>();
      for (final String database : orderedDatabases) {
        final DatabaseLockState lockState =
            lockStateMap.computeIfAbsent(database, ignored -> new DatabaseLockState());
        final boolean hasWaiterPriority = lockState.isHeadWaiter(procedure);
        if (!lockState.canAcquireProcedureLock(procedure, hasWaiterPriority)) {
          acquiredDatabases.forEach(
              acquiredDatabase -> releaseProcedureLock(procedure, acquiredDatabase));
          return database;
        }
        if (lockState.acquireProcedureLock(procedure)) {
          acquiredDatabases.add(database);
        }
        if (hasWaiterPriority) {
          lockState.removeHeadWaiter(procedure);
        }
      }
      return null;
    } finally {
      stateLock.unlock();
    }
  }

  public void waitProcedure(final Procedure<?> procedure, final String databaseName) {
    stateLock.lock();
    try {
      final DatabaseLockState lockState =
          lockStateMap.computeIfAbsent(databaseName, ignored -> new DatabaseLockState());
      if (lockState.isUnlocked() && !lockState.hasWaitingProcedures()) {
        scheduler.addFront(procedure);
        removeIfIdle(databaseName, lockState);
      } else {
        lockState.waitProcedure(procedure);
      }
    } finally {
      stateLock.unlock();
    }
  }

  public void releaseLocks(final Procedure<?> procedure, final Set<String> databaseNames) {
    stateLock.lock();
    try {
      orderedDatabases(databaseNames)
          .forEach(database -> releaseProcedureLock(procedure, database));
    } finally {
      stateLock.unlock();
    }
  }

  private boolean canAcquireRequestLocks(final Thread owner, final List<String> orderedDatabases) {
    for (final String database : orderedDatabases) {
      final DatabaseLockState lockState = lockStateMap.get(database);
      if (lockState != null && !lockState.canAcquireRequestLock(owner)) {
        return false;
      }
    }
    return true;
  }

  private DatabaseLock acquireRequestLocks(
      final List<String> orderedDatabases, final Thread owner) {
    orderedDatabases.forEach(
        database ->
            lockStateMap
                .computeIfAbsent(database, ignored -> new DatabaseLockState())
                .acquireRequestLock(owner));
    return new DatabaseLock(this, orderedDatabases, owner);
  }

  private void releaseRequestLocks(final List<String> orderedDatabases, final Thread requestOwner) {
    stateLock.lock();
    try {
      for (final String database : orderedDatabases) {
        final DatabaseLockState lockState = lockStateMap.get(database);
        if (lockState != null && lockState.releaseRequestLock(requestOwner)) {
          wakeWaiters(lockState);
          removeIfIdle(database, lockState);
        }
      }
    } finally {
      stateLock.unlock();
    }
  }

  private void releaseProcedureLock(final Procedure<?> procedure, final String database) {
    final DatabaseLockState lockState = lockStateMap.get(database);
    if (lockState != null && lockState.releaseProcedureLock(procedure)) {
      wakeWaiters(lockState);
      removeIfIdle(database, lockState);
    }
  }

  private void wakeWaiters(final DatabaseLockState lockState) {
    if (!lockState.wakeNextWaitingProcedure(scheduler)) {
      lockReleased.signalAll();
    }
  }

  private void removeIfIdle(final String database, final DatabaseLockState lockState) {
    if (lockState.isIdle()) {
      lockStateMap.remove(database, lockState);
    }
  }

  private static List<String> orderedDatabases(final Set<String> databaseNames) {
    return new ArrayList<>(new TreeSet<>(databaseNames));
  }

  public static final class DatabaseLock implements AutoCloseable {
    private final DatabaseLockQueue lockQueue;
    private final List<String> databaseNames;
    private final Thread owner;
    private boolean closed;

    private DatabaseLock(
        final DatabaseLockQueue lockQueue, final List<String> databaseNames, final Thread owner) {
      this.lockQueue = lockQueue;
      this.databaseNames = databaseNames;
      this.owner = owner;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        lockQueue.releaseRequestLocks(databaseNames, owner);
      }
    }
  }

  private static final class DatabaseLockState {
    private final ArrayDeque<Procedure<?>> waitingProcedures = new ArrayDeque<>();
    private Procedure<?> procedureOwner;
    private Thread requestOwner;
    private int requestHoldCount;

    private boolean canAcquireRequestLock(final Thread owner) {
      if (requestOwner == owner) {
        return true;
      }
      return requestOwner == null && procedureOwner == null && waitingProcedures.isEmpty();
    }

    private void acquireRequestLock(final Thread owner) {
      requestOwner = owner;
      requestHoldCount++;
    }

    /**
     * Releases one request lock hold.
     *
     * @return true when the lock became fully released
     */
    private boolean releaseRequestLock(final Thread owner) {
      if (requestOwner != owner) {
        return false;
      }
      requestHoldCount--;
      if (requestHoldCount == 0) {
        requestOwner = null;
        return true;
      }
      return false;
    }

    private boolean canAcquireProcedureLock(
        final Procedure<?> procedure, final boolean hasWaiterPriority) {
      return requestOwner == null
          && (procedureOwner == null || procedureOwner.getProcId() == procedure.getProcId())
          && (hasWaiterPriority || waitingProcedures.isEmpty());
    }

    /**
     * Acquires the procedure lock when it is not already held by the same procedure.
     *
     * @return true when this invocation newly acquired the lock
     */
    private boolean acquireProcedureLock(final Procedure<?> procedure) {
      if (procedureOwner == null) {
        procedureOwner = procedure;
        return true;
      }
      return false;
    }

    /**
     * Releases the procedure lock.
     *
     * @return true when the lock was released
     */
    private boolean releaseProcedureLock(final Procedure<?> procedure) {
      if (procedureOwner == null || procedureOwner.getProcId() != procedure.getProcId()) {
        return false;
      }
      procedureOwner = null;
      return true;
    }

    private void waitProcedure(final Procedure<?> procedure) {
      if (waitingProcedures.stream()
          .noneMatch(waitingProcedure -> waitingProcedure.getProcId() == procedure.getProcId())) {
        waitingProcedures.addLast(procedure);
      }
    }

    private boolean removeHeadWaiter(final Procedure<?> procedure) {
      if (isHeadWaiter(procedure)) {
        waitingProcedures.pollFirst();
        return true;
      }
      return false;
    }

    private boolean isHeadWaiter(final Procedure<?> procedure) {
      return !waitingProcedures.isEmpty()
          && waitingProcedures.peekFirst().getProcId() == procedure.getProcId();
    }

    private boolean wakeNextWaitingProcedure(final ProcedureScheduler procedureScheduler) {
      final Procedure<?> waitingProcedure = waitingProcedures.peekFirst();
      if (waitingProcedure == null) {
        return false;
      }
      procedureScheduler.addFront(waitingProcedure);
      return true;
    }

    private boolean hasWaitingProcedures() {
      return !waitingProcedures.isEmpty();
    }

    private boolean isUnlocked() {
      return procedureOwner == null && requestOwner == null;
    }

    private boolean isIdle() {
      return isUnlocked() && waitingProcedures.isEmpty();
    }
  }
}
