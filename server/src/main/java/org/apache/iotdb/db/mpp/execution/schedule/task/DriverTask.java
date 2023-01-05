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
import org.apache.iotdb.db.mpp.execution.schedule.queue.multilevelqueue.DriverTaskHandle;
import org.apache.iotdb.db.mpp.execution.schedule.queue.multilevelqueue.Priority;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** the scheduling element of {@link DriverTaskThread}. It wraps a single Driver. */
public class DriverTask implements IDIndexedAccessible {

  private final IDriver driver;
  private DriverTaskStatus status;

  // the higher this field is, the higher probability it will be scheduled.
  private volatile double schedulePriority;
  private final long ddl;
  private final Lock lock;

  // Running stats
  private long cpuWallNano;

  private String abortCause;

  private final AtomicReference<Priority> priority = new AtomicReference<>(new Priority(0, 0));

  private DriverTaskHandle driverTaskHandle;
  private long lastEnterReadyQueueTime;
  private long lastEnterBlockQueueTime;

  /** Initialize a dummy instance for queryHolder */
  public DriverTask() {
    this(new StubFragmentInstance(), 0L, null);
  }

  public DriverTask(IDriver driver, long timeoutMs, DriverTaskStatus status) {
    this.driver = driver;
    this.setStatus(status);
    this.schedulePriority = 0.0D;
    this.ddl = System.currentTimeMillis() + timeoutMs;
    this.lock = new ReentrantLock();
  }

  public DriverTaskId getDriverTaskId() {
    return driver.getDriverTaskId();
  }

  @Override
  public void setId(ID id) {
    driver.setDriverTaskId((DriverTaskId) id);
  }

  public DriverTaskStatus getStatus() {
    return status;
  }

  public boolean isEndState() {
    return status == DriverTaskStatus.ABORTED || status == DriverTaskStatus.FINISHED;
  }

  public IDriver getDriver() {
    return driver;
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
    return driver.getDriverTaskId().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof DriverTask && ((DriverTask) o).getDriverTaskId().equals(getDriverTaskId());
  }

  public String getAbortCause() {
    return abortCause;
  }

  public void setAbortCause(String abortCause) {
    this.abortCause = abortCause;
  }

  public Priority getPriority() {
    return priority.get();
  }

  /**
   * Updates the (potentially stale) priority value cached in this object. This should be called
   * when this object is outside the queue.
   *
   * @return true if the level changed.
   */
  public boolean updateLevelPriority() {
    Priority newPriority = taskHandle.getPriority();
    Priority oldPriority = priority.getAndSet(newPriority);
    return newPriority.getLevel() != oldPriority.getLevel();
  }

  public long getLastEnterReadyQueueTime() {
    return lastEnterReadyQueueTime;
  }

  public void setLastEnterReadyQueueTime(long lastEnterReadyQueueTime) {
    this.lastEnterReadyQueueTime = lastEnterReadyQueueTime;
  }

  public long getLastEnterBlockQueueTime() {
    return lastEnterBlockQueueTime;
  }

  public void setLastEnterBlockQueueTime(long lastEnterBlockQueueTime) {
    this.lastEnterBlockQueueTime = lastEnterBlockQueueTime;
  }

  /** a comparator of ddl, the less the ddl is, the low order it has. */
  public static class TimeoutComparator implements Comparator<DriverTask> {

    @Override
    public int compare(DriverTask o1, DriverTask o2) {
      if (o1.getDriverTaskId().equals(o2.getDriverTaskId())) {
        return 0;
      }
      if (o1.getDDL() < o2.getDDL()) {
        return -1;
      }
      if (o1.getDDL() > o2.getDDL()) {
        return 1;
      }
      return o1.getDriverTaskId().compareTo(o2.getDriverTaskId());
    }
  }

  /** a comparator of ddl, the higher the schedulePriority is, the low order it has. */
  public static class SchedulePriorityComparator implements Comparator<DriverTask> {

    @Override
    public int compare(DriverTask o1, DriverTask o2) {
      if (o1.getDriverTaskId().equals(o2.getDriverTaskId())) {
        return 0;
      }
      if (o1.getSchedulePriority() > o2.getSchedulePriority()) {
        return -1;
      }
      if (o1.getSchedulePriority() < o2.getSchedulePriority()) {
        return 1;
      }
      return o1.getDriverTaskId().compareTo(o2.getDriverTaskId());
    }
  }

  private static class StubFragmentInstance implements IDriver {

    private static final QueryId stubQueryId = new QueryId("stub_query");
    private static DriverTaskId stubDriver =
        new DriverTaskId(
            new FragmentInstanceId(new PlanFragmentId(stubQueryId, 0), "stub-instance"), 0);

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public ListenableFuture<?> processFor(Duration duration) {
      return null;
    }

    @Override
    public DriverTaskId getDriverTaskId() {
      return stubDriver;
    }

    @Override
    public void setDriverTaskId(DriverTaskId driverTaskId) {
      stubDriver = driverTaskId;
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
