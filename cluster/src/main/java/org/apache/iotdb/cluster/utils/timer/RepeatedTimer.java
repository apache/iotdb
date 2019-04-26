/**
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
package org.apache.iotdb.cluster.utils.timer;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class RepeatedTimer {

  /**
   * Name of timerMs
   */
  private String timerName;

  /**
   * Lock resource
   */
  private Lock lock = new ReentrantLock();

  /**
   * Timer
   */
  private Timer timerMs;

  /**
   * Task to execute when timeout
   */
  private TimerTask timerTask;

  /**
   * The unit is millisecond.
   */
  private volatile int timeoutMs;

  /**
   * Mark whether the timerMs is stopped or not.
   */
  private volatile boolean stopped;

  /**
   * Mark whether the timer is running or not.
   */
  private volatile boolean running;

  public RepeatedTimer(String name, int timeoutMs) {
    this.timerName = name;
    this.timeoutMs = timeoutMs;
    this.stopped = true;
    this.timerMs = new Timer(timerName);
  }

  /**
   * Start repeated timer.
   */
  public void start() {
    lock.lock();
    try {
      if (stopped && !running) {
        stopped = false;
        running = true;
        schedule();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Subclass implement this method to do actual task.
   */
  public abstract void run();

  /**
   * Schedule timer task
   */
  private void schedule() {
    if (timerTask != null) {
      timerTask.cancel();
    }
    timerTask = new TimerTask() {
      @Override
      public void run() {
        RepeatedTimer.this.triggerAction();
      }
    };
    timerMs.schedule(timerTask, timeoutMs);
  }

  /**
   * Trigger action when timeout
   */
  public void triggerAction() {
    lock.lock();
    try {
      run();
      repeatTimerTask();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Repeat timer task.
   */
  private void repeatTimerTask(){
    if(!stopped){
      schedule();
    }
  }

  /**
   * Reset repeated timer with current timeout parameter.
   */
  public void reset() {
    lock.lock();
    try {
      reset(timeoutMs);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Reset repeated timer with new timeout parameter.
   *
   * @param timeoutMs timeout millisecond
   */
  public void reset(int timeoutMs) {
    lock.lock();
    this.timeoutMs = timeoutMs;
    try {
      if (!stopped) {
        schedule();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Stop the timer
   */
  public void stop() {
    lock.lock();
    try {
      if (!stopped) {
        stopped = true;
        if (timerTask != null) {
          timerTask.cancel();
          running = false;
          timerTask = null;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    return "RepeatedTimer{" +
        "lock=" + lock +
        ", timerMs=" + timerMs +
        ", timerTask=" + timerTask +
        ", stopped=" + stopped +
        ", running=" + running +
        ", timeoutMs=" + timeoutMs +
        ", timerName='" + timerName + '\'' +
        '}';
  }
}

