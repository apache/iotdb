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
package org.apache.iotdb.db.mpp.schedule.task;

import org.apache.iotdb.db.mpp.common.FragmentId;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.ExecFragmentInstance;
import org.apache.iotdb.db.mpp.schedule.ExecutionContext;
import org.apache.iotdb.db.mpp.schedule.FragmentInstanceTaskExecutor;
import org.apache.iotdb.db.mpp.schedule.queue.ID;
import org.apache.iotdb.db.mpp.schedule.queue.IDIndexedAccessible;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * the scheduling element of {@link FragmentInstanceTaskExecutor}. It wraps a single
 * FragmentInstance.
 */
public class FragmentInstanceTask implements IDIndexedAccessible {

  private FragmentInstanceTaskID id;
  private FragmentInstanceTaskStatus status;
  private final ExecFragmentInstance fragmentInstance;

  // the higher this field is, the higher probability it will be scheduled.
  private double schedulePriority;
  private final long ddl;
  private final Lock lock;

  // Running stats
  private long cpuWallNano;

  /** Initialize a dummy instance for queryHolder */
  public FragmentInstanceTask() {
    this(new StubFragmentInstance(), 0L, null);
  }

  public FragmentInstanceTask(
      ExecFragmentInstance instance, long timeoutMs, FragmentInstanceTaskStatus status) {
    this.fragmentInstance = instance;
    this.id = new FragmentInstanceTaskID(instance.getInfo());
    this.setStatus(status);
    this.schedulePriority = 0L;
    this.ddl = System.currentTimeMillis() + timeoutMs;
    this.lock = new ReentrantLock();
  }

  public FragmentInstanceTaskID getId() {
    return id;
  }

  @Override
  public void setId(ID id) {
    this.id = (FragmentInstanceTaskID) id;
  }

  public FragmentInstanceTaskStatus getStatus() {
    return status;
  }

  public boolean isEndState() {
    return status == FragmentInstanceTaskStatus.ABORTED
        || status == FragmentInstanceTaskStatus.FINISHED;
  }

  public ExecFragmentInstance getFragmentInstance() {
    return fragmentInstance;
  }

  public void setStatus(FragmentInstanceTaskStatus status) {
    this.status = status;
  }

  /**
   * Update the schedule priority according to the execution context.
   *
   * @param context the last execution context.
   */
  public void updateSchedulePriority(ExecutionContext context) {
    // TODO: need to implement more complex here

    // 1. The penalty factor means that if a task executes less time in one schedule, it will have a
    // high schedule priority
    double penaltyFactor =
        context.getCpuDuration().getWall().getValue(TimeUnit.NANOSECONDS)
            / context.getTimeSlice().getValue(TimeUnit.NANOSECONDS);
    // 2. If a task is nearly timeout, it should be scheduled as soon as possible.
    long base = System.currentTimeMillis() - ddl;

    // 3. Now the final schedulePriority is out, this may not be so reasonable.
    this.schedulePriority = base * penaltyFactor;
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

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FragmentInstanceTask && ((FragmentInstanceTask) o).getId().equals(id);
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
      return o1.getId().compareTo(o2.getId());
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
      return o1.getId().compareTo(o2.getId());
    }
  }

  private static class StubFragmentInstance implements ExecFragmentInstance {

    private static final QueryId stubQueryId = new QueryId("stub-query");
    private static final FragmentInstanceId stubInstance =
        new FragmentInstanceId(stubQueryId, new FragmentId(stubQueryId, 0), "stub-instance");

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public ListenableFuture<Void> processFor(Duration duration) {
      return null;
    }

    @Override
    public FragmentInstanceId getInfo() {
      return stubInstance;
    }

    @Override
    public void close() {}
  }
}
