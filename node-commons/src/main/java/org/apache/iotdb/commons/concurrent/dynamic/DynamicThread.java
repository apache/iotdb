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
package org.apache.iotdb.commons.concurrent.dynamic;

/**
 * DynamicThread record the idle time and running time of the thread and trigger addThread() or
 * onThreadExit() in DynamicThreadGroup to change the number of threads in a thread group
 * dynamically. IMPORTANT: the implementation must call idleToRunning(), runningToIdle(), and
 * shouldExit() properly in runInternal().
 */
public abstract class DynamicThread implements Runnable {

  private DynamicThreadGroup threadGroup;
  private long idleStart;
  private long runningStart;
  private long idleTimeSum;
  private long runningTimeSum;
  // TODO: add configuration for the two values
  private double maximumIdleRatio = 0.5;
  private double minimumIdleRatio = 0.1;

  public DynamicThread(DynamicThreadGroup threadGroup) {
    this.threadGroup = threadGroup;
  }

  /**
   * The implementation must call idleToRunning() and runningToIdle() properly. E.g., {
   * {@code {while(! Thread.interrupted ()) { Object obj = blockingQueue.poll(); idleToRunning();
   * process(obj); RunningToIdle(); if (shouldExit()) { return; } }
   *
   * @<code>}
   */
  public abstract void runInternal();

  protected void idleToRunning() {
    if (idleStart != 0) {
      long idleTime = System.nanoTime() - idleStart;
      idleTimeSum += idleTime;
    }
    runningStart = System.nanoTime();
  }

  protected void runningToIdle() {
    if (runningStart != 0) {
      long runningTime = System.nanoTime() - runningStart;
      runningTimeSum += runningTime;
    }
    idleStart = System.nanoTime();
  }

  protected double idleRatio() {
    return idleTimeSum * 1.0 / (idleTimeSum + runningTimeSum);
  }

  protected boolean shouldExit() {
    if (threadGroup == null) {
      return false;
    }

    double idleRatio = idleRatio();
    if (idleRatio < minimumIdleRatio) {
      // Thread too busy, try adding a new thread
      threadGroup.addThread();
      return false;
    } else if (idleRatio > maximumIdleRatio) {
      // Thread too idle, exit if there is still enough threads
      int afterCnt = threadGroup.getThreadCnt().decrementAndGet();
      if (afterCnt >= threadGroup.getMinThreadCnt()) {
        // notice that onThreadExit() will also decrease the counter, so we add it back here to avoid
        // the counter being decreased twice
        threadGroup.getThreadCnt().incrementAndGet();
        return true;
      } else {
        threadGroup.getThreadCnt().incrementAndGet();
        return false;
      }
    }
    return false;
  }

  @Override
  public void run() {
    try {
      runInternal();
    } finally {
      if (threadGroup != null) {
        threadGroup.onThreadExit(this);
      }
    }
  }
}
