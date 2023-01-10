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

package org.apache.iotdb.db.mpp.execution.schedule.queue.multilevelqueue;

import javax.annotation.concurrent.GuardedBy;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class DriverTaskHandle {

  private final int driverTaskHandleId;

  @GuardedBy("this")
  private long scheduledTimeInNanos;

  private final MultilevelPriorityQueue driverTaskQueue;

  /** It is not used for now but can be used to limit the driverNum per Task in the future. */
  private final OptionalInt maxDriversPerTask;

  private final AtomicReference<Priority> priority = new AtomicReference<>(new Priority(0, 0));

  public DriverTaskHandle(
      int driverTaskHandleId,
      MultilevelPriorityQueue driverTaskQueue,
      OptionalInt maxDriversPerTask) {
    this.driverTaskHandleId = driverTaskHandleId;
    this.driverTaskQueue = requireNonNull(driverTaskQueue, "driverTaskQueue is null");
    this.maxDriversPerTask = requireNonNull(maxDriversPerTask, "maxDriversPerTask is null");
  }

  public synchronized Priority addScheduledTimeInNanos(long durationNanos) {
    scheduledTimeInNanos += durationNanos;
    Priority newPriority =
        driverTaskQueue.updatePriority(priority.get(), durationNanos, scheduledTimeInNanos);

    priority.set(newPriority);
    return newPriority;
  }

  public synchronized Priority resetLevelScheduledTime() {
    long levelMinScheduledTime =
        driverTaskQueue.getLevelMinScheduledTime(priority.get().getLevel(), scheduledTimeInNanos);
    if (priority.get().getLevelScheduledTime() < levelMinScheduledTime) {
      Priority newPriority = new Priority(priority.get().getLevel(), levelMinScheduledTime);
      priority.set(newPriority);
      return newPriority;
    }

    return priority.get();
  }

  public Priority getPriority() {
    return priority.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DriverTaskHandle that = (DriverTaskHandle) o;
    return driverTaskHandleId == that.driverTaskHandleId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(driverTaskHandleId);
  }
}
