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

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;

/** Simple scheduler for procedures */
public class SimpleProcedureScheduler extends AbstractProcedureScheduler {
  // Use "LinkedBlockingDeque" to ensure thread-safety
  private final LinkedBlockingDeque<Procedure> runnables = new LinkedBlockingDeque<>();
  private final LinkedBlockingDeque<Procedure> waitings = new LinkedBlockingDeque<>();

  @Override
  protected void enqueue(final Procedure procedure, final boolean addFront) {
    if (addFront) {
      runnables.addFirst(procedure);
    } else {
      runnables.addLast(procedure);
    }
  }

  @Override
  protected Procedure dequeue() {
    return runnables.poll();
  }

  @Override
  public void clear() {
    schedLock();
    try {
      runnables.clear();
      waitings.clear();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void yield(final Procedure proc) {
    addBack(proc);
  }

  @Override
  public boolean queueHasRunnables() {
    return !runnables.isEmpty();
  }

  @Override
  public int queueSize() {
    return runnables.size();
  }

  public void waitProcedure(Procedure proc, ReentrantLock lock) {
    boolean signal = false;
    schedLock();
    try {
      if (lock.isLocked()) {
        waitings.add(proc);
      } else {
        runnables.addFirst(proc);
        signal = true;
      }
    } finally {
      schedUnlock();
    }
    if (signal) {
      signalAll();
    }
  }

  public void releaseWaiting(ReentrantLock lock) {
    boolean signal;
    schedLock();
    try {
      lock.unlock();
      signal = !waitings.isEmpty();
      runnables.addAll(waitings);
      waitings.clear();
    } finally {
      schedUnlock();
    }
    if (signal) {
      signalAll();
    }
  }
}
