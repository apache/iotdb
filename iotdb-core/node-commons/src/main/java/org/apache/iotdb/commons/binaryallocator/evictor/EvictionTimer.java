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

package org.apache.iotdb.commons.binaryallocator.evictor;

import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This file is modified from {@link org.apache.commons.pool2.impl.EvictionTimer} to make some
 * adaptations for IoTDB.
 */
public class EvictionTimer {
  private static ScheduledThreadPoolExecutor executor;

  /**
   * Task that removes references to abandoned tasks and shuts down the executor if there are no
   * live tasks left.
   */
  private static class Reaper implements Runnable {
    @Override
    public void run() {
      synchronized (EvictionTimer.class) {
        for (final Map.Entry<WeakReference<Evictor>, WeakRunner<Evictor>> entry :
            TASK_MAP.entrySet()) {
          if (entry.getKey().get() == null) {
            executor.remove(entry.getValue());
            TASK_MAP.remove(entry.getKey());
          }
        }
        if (TASK_MAP.isEmpty() && executor != null) {
          executor.shutdown();
          executor.setCorePoolSize(0);
          executor = null;
        }
      }
    }
  }

  /**
   * Runnable that runs the referent of a weak reference. When the referent is no no longer
   * reachable, run is no-op.
   */
  private static class WeakRunner<R extends Runnable> implements Runnable {

    private final WeakReference<R> ref;

    /** Constructs a new instance to track the given reference. */
    private WeakRunner(final WeakReference<R> ref) {
      this.ref = ref;
    }

    @Override
    public void run() {
      final Runnable task = ref.get();
      if (task != null) {
        task.run();
      } else {
        executor.remove(this);
        TASK_MAP.remove(ref);
      }
    }
  }

  /** Keys are weak references to tasks, values are runners managed by executor. */
  private static final HashMap<WeakReference<Evictor>, WeakRunner<Evictor>> TASK_MAP =
      new HashMap<>();

  /**
   * Removes the specified eviction task from the timer.
   *
   * @param evictor Task to be cancelled.
   * @param timeout If the associated executor is no longer required, how long should this thread
   *     wait for the executor to terminate.
   * @param restarting The state of the evictor.
   */
  public static synchronized void cancel(
      final Evictor evictor, final Duration timeout, final boolean restarting) {
    if (evictor != null) {
      evictor.cancel();
      remove(evictor);
    }
    if (!restarting && executor != null && TASK_MAP.isEmpty()) {
      executor.shutdown();
      try {
        executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
      }
      executor.setCorePoolSize(0);
      executor = null;
    }
  }

  /** Removes evictor from the task set and executor. Only called when holding the class lock. */
  private static void remove(final Evictor evictor) {
    for (final Map.Entry<WeakReference<Evictor>, WeakRunner<Evictor>> entry : TASK_MAP.entrySet()) {
      if (entry.getKey().get() == evictor) {
        executor.remove(entry.getValue());
        TASK_MAP.remove(entry.getKey());
        break;
      }
    }
  }

  /**
   * Adds the specified eviction task to the timer. Tasks that are added with a call to this method
   * *must* call {@link #cancel(Evictor, Duration, boolean)} to cancel the task to prevent memory
   * and/or thread leaks in application server environments.
   *
   * @param task Task to be scheduled.
   * @param delay Delay in milliseconds before task is executed.
   * @param period Time in milliseconds between executions.
   * @param name Name of the thread.
   */
  public static synchronized void schedule(
      final Evictor task, final Duration delay, final Duration period, final String name) {
    if (null == executor) {
      executor = new ScheduledThreadPoolExecutor(1, new IoTThreadFactory(name));
      executor.setRemoveOnCancelPolicy(true);
      ScheduledExecutorUtil.safelyScheduleAtFixedRate(
          executor, new Reaper(), delay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    }
    final WeakReference<Evictor> ref = new WeakReference<>(task);
    final WeakRunner<Evictor> runner = new WeakRunner<>(ref);
    final ScheduledFuture<?> scheduledFuture =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            executor, runner, delay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    task.setScheduledFuture(scheduledFuture);
    TASK_MAP.put(ref, runner);
  }

  private EvictionTimer() {
    // Hide the default constructor
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("EvictionTimer []");
    return builder.toString();
  }
}
