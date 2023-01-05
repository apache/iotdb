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

import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;

import javax.annotation.concurrent.GuardedBy;

import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class is inspired by Trino <a
 * href="https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/executor/MultilevelSplitQueue.java">...</a>
 */
public class MultilevelPriorityQueue {
  /** Scheduled time threshold of each level */
  static final int[] LEVEL_THRESHOLD_SECONDS = {0, 1, 10, 60, 300};

  static final long LEVEL_CONTRIBUTION_CAP = SECONDS.toNanos(30);

  @GuardedBy("lock")
  private final PriorityQueue<DriverTask>[] levelWaitingSplits;

  private final AtomicLong[] levelScheduledTime;

  private final AtomicLong[] levelMinPriority;
  private final ReentrantLock lock = new ReentrantLock();

  private final Condition notEmpty = lock.newCondition();

  /**
   * Expected scheduled time of level0-level4 is: levelTimeMultiplier^4 : levelTimeMultiplier^3 :
   * levelTimeMultiplier^2 : levelTimeMultiplier : 1
   */
  private final double levelTimeMultiplier;

  public MultilevelPriorityQueue(double levelTimeMultiplier) {
    this.levelScheduledTime = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
    this.levelMinPriority = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
    this.levelWaitingSplits = new PriorityQueue[LEVEL_THRESHOLD_SECONDS.length];
    for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
      levelScheduledTime[level] = new AtomicLong();
      levelMinPriority[level] = new AtomicLong(-1);
      levelWaitingSplits[level] = new PriorityQueue<>();
    }
    this.levelTimeMultiplier = levelTimeMultiplier;
  }

  private void addLevelTime(int level, long nanos) {
    levelScheduledTime[level].addAndGet(nanos);
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
  public void push(DriverTask task) {
    checkArgument(task != null, "DriverTask to be pushed is null");

    int level = task.getPriority().getLevel();
    lock.lock();
    try {
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
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  /** Return the element that should be scheduled first among all the levels */
  public DriverTask poll() throws InterruptedException {
    while (true) {
      lock.lockInterruptibly();
      try {
        DriverTask result;
        while ((result = pollFirst()) == null) {
          notEmpty.await();
        }

        if (result.updateLevelPriority()) {
          // todo: add annotation here
          push(result);
          continue;
        }

        int selectedLevel = result.getPriority().getLevel();
        levelMinPriority[selectedLevel].set(result.getPriority().getLevelPriority());
        return result;
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * This queue attempts to give each level a target amount of scheduled time, which is configurable
   * using levelTimeMultiplier.
   *
   * <p>This function selects the level that has the lowest ratio of actual to the target time with
   * the objective of minimizing deviation from the target scheduled time. From this level, we pick
   * the DriverTask with the lowest priority.
   */
  @GuardedBy("lock")
  private DriverTask pollFirst() {
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

    if (selectedLevel == -1) {
      return null;
    }

    DriverTask result = levelWaitingSplits[selectedLevel].poll();
    checkState(result != null, "pollFirst cannot return null");

    return result;
  }

  /**
   * level0TargetTime
   *
   * @return corrected value of level0TargetTime
   */
  @GuardedBy("lock")
  private long getLevel0TargetTime() {
    long level0TargetTime = levelScheduledTime[0].get();
    double currentMultiplier = levelTimeMultiplier;

    for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
      currentMultiplier /= levelTimeMultiplier;
      long levelTime = levelScheduledTime[level].get();
      level0TargetTime = Math.max(level0TargetTime, (long) (levelTime / currentMultiplier));
    }

    return level0TargetTime;
  }
}
