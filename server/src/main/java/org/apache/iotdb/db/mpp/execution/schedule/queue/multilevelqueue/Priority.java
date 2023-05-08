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

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * This class is inspired by Trino <a
 * href="https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/executor/Priority.java">...</a>
 * Priority is composed of a level and a within-level priority. Level decides which queue the
 * DriverTask is placed in, while within-level priority decides which DriverTask is executed next in
 * that level.
 *
 * <p>Tasks move from a lower to higher level as they exceed level thresholds of total scheduled
 * time accrued to a task.
 *
 * <p>The priority within a level increases with the scheduled time accumulated in that level. This
 * is necessary to achieve fairness when tasks acquire scheduled time at varying rates.
 *
 * <p>However, this priority is <b>not</b> equal to the task total accrued scheduled time. When a
 * task graduates to a higher level, the level priority is set to the minimum current priority in
 * the new level. This allows us to maintain instantaneous fairness in terms of scheduled time.
 */
@Immutable
public final class Priority {
  private final int level;

  /**
   * Occupied time in particular level of this task. The higher this value is, the later the task
   * with this Priority will be polled out by a PriorityQueue.
   */
  private final long levelScheduledTime;

  public Priority(int level, long levelScheduledTime) {
    this.level = level;
    this.levelScheduledTime = levelScheduledTime;
  }

  public int getLevel() {
    return level;
  }

  public long getLevelScheduledTime() {
    return levelScheduledTime;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("level", level)
        .add("levelScheduledTime", levelScheduledTime)
        .toString();
  }
}
