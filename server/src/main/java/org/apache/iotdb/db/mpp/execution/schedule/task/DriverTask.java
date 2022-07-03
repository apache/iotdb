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
package org.apache.iotdb.db.mpp.execution.schedule.task;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.exchange.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.schedule.DriverTaskThread;
import org.apache.iotdb.db.mpp.execution.schedule.ExecutionContext;
import org.apache.iotdb.db.mpp.execution.schedule.queue.ID;
import org.apache.iotdb.db.mpp.execution.schedule.queue.IDIndexedAccessible;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** the scheduling element of {@link DriverTaskThread}. It wraps a single Driver. */
public class DriverTask implements IDIndexedAccessible {

  private DriverTaskID id;
  private DriverTaskStatus status;
  private final IDriver fragmentInstance;

  // the higher this field is, the higher probability it will be scheduled.
  private volatile double schedulePriority;
  private final long ddl;
  private final Lock lock;

  // Running stats
  private long cpuWallNano;

  private String abortCause;

  /** Initialize a dummy instance for queryHolder */
  public DriverTask() {
    this(new StubFragmentInstance(), 0L, null);
  }

  public DriverTask(IDriver instance, long timeoutMs, DriverTaskStatus status) {
    this.fragmentInstance = instance;
    this.id = new DriverTaskID(instance.getInfo());
    this.setStatus(status);
    this.schedulePriority = 0.0D;
    this.ddl = System.currentTimeMillis() + timeoutMs;
    this.lock = new ReentrantLock();
  }

  public DriverTaskID getId() {
    return id;
  }

  @Override
  public void setId(ID id) {
    this.id = (DriverTaskID) id;
  }

  public DriverTaskStatus getStatus() {
    return status;
  }

  public boolean isEndState() {
    return status == DriverTaskStatus.ABORTED || status == DriverTaskStatus.FINISHED;
  }

  public IDriver getFragmentInstance() {
    return fragmentInstance;
  }

  public void setStatus(DriverTaskStatus status) {
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
    return o instanceof DriverTask && ((DriverTask) o).getId().equals(id);
  }

  public String getAbortCause() {
    return abortCause;
  }

  public void setAbortCause(String abortCause) {
    this.abortCause = abortCause;
  }

  /** a comparator of ddl, the less the ddl is, the low order it has. */
  public static class TimeoutComparator implements Comparator<DriverTask> {

    @Override
    public int compare(DriverTask o1, DriverTask o2) {
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
  public static class SchedulePriorityComparator implements Comparator<DriverTask> {

    @Override
    public int compare(DriverTask o1, DriverTask o2) {
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

  private static class StubFragmentInstance implements IDriver {

    private static final QueryId stubQueryId = new QueryId("stub_query");
    private static final FragmentInstanceId stubInstance =
        new FragmentInstanceId(new PlanFragmentId(stubQueryId, 0), "stub-instance");

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public ListenableFuture<?> processFor(Duration duration) {
      return null;
    }

    @Override
    public FragmentInstanceId getInfo() {
      return stubInstance;
    }

    @Override
    public void close() {}

    @Override
    public void failed(Throwable t) {}

    @Override
    public ISinkHandle getSinkHandle() {
      return null;
    }
  }
}
