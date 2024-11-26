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

package org.apache.iotdb.commons.utils.BinaryAllocator;

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import java.lang.ref.WeakReference;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class EvictionTimer {

  /** Executor instance */
  private static ScheduledThreadPoolExecutor executor;

  /** Thread factory that creates a daemon thread, with the context class loader from this class. */
  private static class EvictorThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(final Runnable runnable) {
      final Thread thread = new Thread(null, runnable, "arena-evictor");
      thread.setDaemon(true);
      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
              () -> {
                thread.setContextClassLoader(EvictorThreadFactory.class.getClassLoader());
                return null;
              });

      return thread;
    }
  }

  /**
   * Task that removes references to abandoned tasks and shuts down the executor if there are no
   * live tasks left.
   */
  private static class Reaper implements Runnable {
    @Override
    public void run() {
      synchronized (EvictionTimer.class) {
        for (final Map.Entry<WeakReference<Arena.Evictor>, WeakRunner<Arena.Evictor>> entry :
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
   *
   * @param <R> The kind of Runnable.
   */
  private static class WeakRunner<R extends Runnable> implements Runnable {

    private final WeakReference<R> ref;

    /**
     * Constructs a new instance to track the given reference.
     *
     * @param ref the reference to track.
     */
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
  private static final HashMap<WeakReference<Arena.Evictor>, WeakRunner<Arena.Evictor>> TASK_MAP =
      new HashMap<>(); // @GuardedBy("EvictionTimer.class")

  /**
   * Removes the specified eviction task from the timer.
   *
   * @param evictor Task to be cancelled.
   * @param timeout If the associated executor is no longer required, how long should this thread
   *     wait for the executor to terminate?
   * @param restarting The state of the evictor.
   */
  public static synchronized void cancel(
      final Arena.Evictor evictor, final Duration timeout, final boolean restarting) {
    if (evictor != null) {
      evictor.cancel();
      remove(evictor);
    }
    if (!restarting && executor != null && TASK_MAP.isEmpty()) {
      executor.shutdown();
      try {
        executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        // Swallow
        // Significant API changes would be required to propagate this
      }
      executor.setCorePoolSize(0);
      executor = null;
    }
  }

  /**
   * For testing only.
   *
   * @return The executor.
   */
  static ScheduledThreadPoolExecutor getExecutor() {
    return executor;
  }

  /**
   * @return the number of eviction tasks under management.
   */
  public static synchronized int getNumTasks() {
    return TASK_MAP.size();
  }

  /**
   * Gets the task map. Keys are weak references to tasks, values are runners managed by executor.
   *
   * @return the task map.
   */
  static HashMap<WeakReference<Arena.Evictor>, WeakRunner<Arena.Evictor>> getTaskMap() {
    return TASK_MAP;
  }

  /**
   * Removes evictor from the task set and executor. Only called when holding the class lock.
   *
   * @param evictor Eviction task to remove
   */
  private static void remove(final Arena.Evictor evictor) {
    for (final Map.Entry<WeakReference<Arena.Evictor>, WeakRunner<Arena.Evictor>> entry :
        TASK_MAP.entrySet()) {
      if (entry.getKey().get() == evictor) {
        executor.remove(entry.getValue());
        TASK_MAP.remove(entry.getKey());
        break;
      }
    }
  }

  /**
   * Adds the specified eviction task to the timer. Tasks that are added with a call to this method
   * *must* call {@link #cancel(Arena.Evictor, Duration, boolean)} to cancel the task to prevent
   * memory and/or thread leaks in application server environments.
   *
   * @param task Task to be scheduled.
   * @param delay Delay in milliseconds before task is executed.
   * @param period Time in milliseconds between executions.
   */
  public static synchronized void schedule(
      final Arena.Evictor task, final Duration delay, final Duration period) {
    if (null == executor) {
      executor = new ScheduledThreadPoolExecutor(1, new EvictorThreadFactory());
      executor.setRemoveOnCancelPolicy(true);
      ScheduledExecutorUtil.safelyScheduleAtFixedRate(
          executor, new Reaper(), delay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    }
    final WeakReference<Arena.Evictor> ref = new WeakReference<>(task);
    final WeakRunner<Arena.Evictor> runner = new WeakRunner<>(ref);
    final ScheduledFuture<?> scheduledFuture =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            executor, runner, delay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    task.setScheduledFuture(scheduledFuture);
    TASK_MAP.put(ref, runner);
  }

  /** Prevents instantiation */
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
