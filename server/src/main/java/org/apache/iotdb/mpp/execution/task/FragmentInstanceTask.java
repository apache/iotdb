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
package org.apache.iotdb.mpp.execution.task;

import org.apache.iotdb.mpp.execution.ExecutionContext;
import org.apache.iotdb.mpp.execution.FragmentInstanceTaskExecutor;
import org.apache.iotdb.mpp.execution.queue.ID;
import org.apache.iotdb.mpp.execution.queue.IDIndexedAccessible;

import java.util.Comparator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * the scheduling element of {@link FragmentInstanceTaskExecutor}. It wraps a single
 * FragmentInstance.
 */
public class FragmentInstanceTask implements IDIndexedAccessible {

  private FragmentInstanceID id;
  private FragmentInstanceTaskStatus status;
  private final ExecutionContext executionContext;

  // the higher this field is, the higher probability it will be scheduled.
  private long schedulePriority;
  private final long ddl;
  private final Lock lock;

  /** Initialize a dummy instance for queryHolder */
  public FragmentInstanceTask() {
    this(null, 0L, null);
  }

  public FragmentInstanceTask(
      FragmentInstanceID id, long timeoutMs, FragmentInstanceTaskStatus status) {
    this.id = id;
    this.setStatus(status);
    this.executionContext = new ExecutionContext();
    this.schedulePriority = 0L;
    this.ddl = System.currentTimeMillis() + timeoutMs;
    this.lock = new ReentrantLock();
  }

  public FragmentInstanceID getId() {
    return id;
  }

  @Override
  public void setId(ID id) {
    this.id = (FragmentInstanceID) id;
  }

  public FragmentInstanceTaskStatus getStatus() {
    return status;
  }

  public void setStatus(FragmentInstanceTaskStatus status) {
    this.status = status;
  }

  public ExecutionContext getExecutionContext() {
    return executionContext;
  }

  /** Update the schedule priority according to the execution context. */
  public void updateSchedulePriority() {
    // TODO: need to implement here
    this.schedulePriority = System.currentTimeMillis() - ddl;
  }

  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }

  public double getSchedulePriority() {
    return schedulePriority;
  }

  public long getDDL() {
    return ddl;
  }

  /** a comparator of ddl, the less the ddl is, the low order it has. */
  public static class TimeoutComparator implements Comparator<FragmentInstanceTask> {

    @Override
    public int compare(FragmentInstanceTask o1, FragmentInstanceTask o2) {
      if (o1.getId().equals(o2.getId())) {
        return 0;
      }
      if (o1.getDDL() < o2.getDDL()) {
        return -1;
      }
      if (o1.getDDL() > o2.getDDL()) {
        return 1;
      }
      return o1.getId().compareTo(o2);
    }
  }

  /** a comparator of ddl, the higher the schedulePriority is, the low order it has. */
  public static class SchedulePriorityComparator implements Comparator<FragmentInstanceTask> {

    @Override
    public int compare(FragmentInstanceTask o1, FragmentInstanceTask o2) {
      if (o1.getId().equals(o2.getId())) {
        return 0;
      }
      if (o1.getSchedulePriority() > o2.getSchedulePriority()) {
        return -1;
      }
      if (o1.getSchedulePriority() < o2.getSchedulePriority()) {
        return 1;
      }
      return o1.getId().compareTo(o2);
    }
  }
}
