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

import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingReserveQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;

import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class is inspired by Trino <a
 * href="https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/executor/MultilevelSplitQueue.java">...</a>
 */
public class MultilevelPriorityQueue extends IndexedBlockingReserveQueue<DriverTask> {
  /** Scheduled time threshold of TASK in each level */
  static final int[] LEVEL_THRESHOLD_SECONDS = {0, 1, 10, 60, 300};

  /** the upper limit one Task can contribute to its level in one scheduled time */
  static final long LEVEL_CONTRIBUTION_CAP = SECONDS.toNanos(30);

  private final PriorityQueue<DriverTask>[] levelWaitingSplits;

  /**
   * Total amount of time each LEVEL has occupied, which decides which level we will take task from.
   */
  private final AtomicLong[] levelScheduledTime;

  /** The minimum scheduled time which current TASK in each level has. */
  private final AtomicLong[] levelMinScheduledTime;

  /**
   * Expected schedule time of each LEVEL.
   *
   * <p>The proportion of level0-level4 is: levelTimeMultiplier^4 : levelTimeMultiplier^3 :
   * levelTimeMultiplier^2 : levelTimeMultiplier : 1
   */
  private final double levelTimeMultiplier;

  public MultilevelPriorityQueue(
      double levelTimeMultiplier, int maxCapacity, DriverTask queryHolder) {
    super(maxCapacity, queryHolder);
    this.levelScheduledTime = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
    this.levelMinScheduledTime = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
    this.levelWaitingSplits = new PriorityQueue[LEVEL_THRESHOLD_SECONDS.length];
    for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
      levelScheduledTime[level] = new AtomicLong();
      levelMinScheduledTime[level] = new AtomicLong(-1);
      levelWaitingSplits[level] = new PriorityQueue<>(new DriverTask.SchedulePriorityComparator());
    }
    this.levelTimeMultiplier = levelTimeMultiplier;
  }

  /**
   * During periods of time when a level has no waiting splits, it will not accumulate scheduled
   * time and will fall behind relative to other levels.
   *
   * <p>This can cause temporary starvation for other levels when splits do reach the
   * previously-empty level.
   *
   * <p>To prevent this we set the scheduled time for levels which were empty to the expected
   * scheduled time.
   */
  @Override
  public void pushToQueue(DriverTask task) {
    checkArgument(task != null, "DriverTask to be pushed is null");

    int level = task.getPriority().getLevel();
    if (levelWaitingSplits[level].isEmpty()) {
      // Accesses to levelScheduledTime are not synchronized, so we have a data race
      // here - our level time math will be off. However, the staleness is bounded by
      // the fact that only running splits that complete during this computation
      // can update the level time. Therefore, this is benign.
      long level0Time = getLevel0TargetTime();
      long levelExpectedTime = (long) (level0Time / Math.pow(levelTimeMultiplier, level));
      long delta = levelExpectedTime - levelScheduledTime[level].get();
      levelScheduledTime[level].addAndGet(delta);
    }
    levelWaitingSplits[level].offer(task);
  }

  protected DriverTask pollFirst() {
    DriverTask result;
    while (true) {
      result = chooseLevelAndTask();
      if (result.updatePriority()) {
        // result.updatePriority() returns true means that the Priority of DriverTaskHandle the
        // result belongs to has changed.
        // All the DriverTasks of one DriverTaskHandle should be in the same level.
        // We push the result into the queue and choose another DriverTask.
        pushToQueue(result);
        continue;
      }
      int selectedLevel = result.getPriority().getLevel();
      levelMinScheduledTime[selectedLevel].set(result.getPriority().getLevelScheduledTime());
      return result;
    }
  }

  /**
   * We attempt to give each level a target amount of scheduled time, which is configurable using
   * levelTimeMultiplier.
   *
   * <p>This function selects the level that has the lowest ratio of actual to the target time with
   * the objective of minimizing deviation from the target scheduled time. From this level, we pick
   * the DriverTask with the lowest scheduled time.
   */
  private DriverTask chooseLevelAndTask() {
    long targetScheduledTime = getLevel0TargetTime();
    double worstRatio = 1;
    int selectedLevel = -1;
    for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
      if (!levelWaitingSplits[level].isEmpty()) {
        long levelTime = levelScheduledTime[level].get();
        double ratio = levelTime == 0 ? 0 : targetScheduledTime / (1.0 * levelTime);
        if (selectedLevel == -1 || ratio > worstRatio) {
          worstRatio = ratio;
          selectedLevel = level;
        }
      }

      targetScheduledTime /= levelTimeMultiplier;
    }

    // selected level == -1 means that the queue is empty and this method is only called when the
    // queue is not empty.
    checkState(selectedLevel != -1, "selected level can not equal to -1");
    DriverTask result = levelWaitingSplits[selectedLevel].poll();
    checkState(result != null, "result driverTask cannot be null");
    return result;
  }

  @Override
  protected DriverTask remove(DriverTask driverTask) {
    checkArgument(driverTask != null, "driverTask is null");
    for (PriorityQueue<DriverTask> level : levelWaitingSplits) {
      if (level.remove(driverTask)) {
        break;
      }
    }
    return driverTask;
  }

  @Override
  protected boolean isEmpty() {
    for (PriorityQueue<DriverTask> level : levelWaitingSplits) {
      if (!level.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(DriverTask driverTask) {
    for (PriorityQueue<DriverTask> level : levelWaitingSplits) {
      if (level.contains(driverTask)) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected DriverTask get(DriverTask driverTask) {
    // We do not support get() for MultilevelPriorityQueue since it is inefficient and not
    // necessary.
    throw new UnsupportedOperationException(
        "MultilevelPriorityQueue does not support access element by get.");
  }

  @Override
  protected void clearAllElements() {
    for (PriorityQueue<DriverTask> level : levelWaitingSplits) {
      level.clear();
    }
  }

  /**
   * Get the expected scheduled time of LEVEL0 based on the maximum scheduled time of all levels
   * after normalization.
   *
   * <p>For example, the levelTimeMultiplier is 2, which means the expected proportion of level0-1
   * is 2 : 1. However, the actual proportion of levelScheduledTime of level0 and level1 is 3 : 2,
   * in this situation the expected time of level0 will be Math.max(3, 2 * 2) = 4.
   *
   * @return the expected scheduled time of LEVEL0
   */
  private synchronized long getLevel0TargetTime() {
    long level0TargetTime = levelScheduledTime[0].get();
    double currentMultiplier = levelTimeMultiplier;

    for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
      currentMultiplier /= levelTimeMultiplier;
      long levelTime = levelScheduledTime[level].get();
      level0TargetTime = Math.max(level0TargetTime, (long) (levelTime / currentMultiplier));
    }

    return level0TargetTime;
  }

  private void addLevelTime(int level, long nanos) {
    levelScheduledTime[level].addAndGet(nanos);
  }

  /**
   * MultilevelPriorityQueue charges the quanta run time to the task and the level it belongs to in
   * an effort to maintain the target thread utilization ratios between levels and to maintain
   * fairness within a level.
   *
   * <p>Consider an example DriverTask where a read hung for several minutes. This is either a bug
   * or a failing dependency. In either case we do not want to charge the task too much, and we
   * especially do not want to charge the level too much - i.e. cause other queries in this level to
   * starve.
   *
   * @return the new priority for the task
   */
  public Priority updatePriority(Priority oldPriority, long quantaNanos, long scheduledNanos) {
    int oldLevel = oldPriority.getLevel();
    int newLevel = computeLevel(scheduledNanos);

    long levelContribution = Math.min(quantaNanos, LEVEL_CONTRIBUTION_CAP);

    if (oldLevel == newLevel) {
      addLevelTime(oldLevel, levelContribution);
      return new Priority(oldLevel, oldPriority.getLevelScheduledTime() + quantaNanos);
    }

    long remainingLevelContribution = levelContribution;
    long remainingTaskTime = quantaNanos;

    // a task normally slowly accrues scheduled time in a level and then moves to the next, but
    // if the task had a particularly long quanta, accrue time to each level as if it had run
    // in that level up to the level limit.
    for (int currentLevel = oldLevel; currentLevel < newLevel; currentLevel++) {
      long timeAccruedToLevel =
          Math.min(
              SECONDS.toNanos(
                  LEVEL_THRESHOLD_SECONDS[currentLevel + 1]
                      - LEVEL_THRESHOLD_SECONDS[currentLevel]),
              remainingLevelContribution);
      addLevelTime(currentLevel, timeAccruedToLevel);
      remainingLevelContribution -= timeAccruedToLevel;
      remainingTaskTime -= timeAccruedToLevel;
    }

    addLevelTime(newLevel, remainingLevelContribution);
    // TODO figure out why add newLevelMinScheduledTime
    long newLevelMinScheduledTime = getLevelMinScheduledTime(newLevel, scheduledNanos);
    return new Priority(newLevel, newLevelMinScheduledTime + remainingTaskTime);
  }

  public long getLevelMinScheduledTime(int level, long taskThreadUsageNanos) {
    levelMinScheduledTime[level].compareAndSet(-1, taskThreadUsageNanos);
    return levelMinScheduledTime[level].get();
  }

  public static int computeLevel(long threadUsageNanos) {
    long seconds = NANOSECONDS.toSeconds(threadUsageNanos);
    for (int level = 0; level < (LEVEL_THRESHOLD_SECONDS.length - 1); level++) {
      if (seconds < LEVEL_THRESHOLD_SECONDS[level + 1]) {
        return level;
      }
    }

    return LEVEL_THRESHOLD_SECONDS.length - 1;
  }
}
