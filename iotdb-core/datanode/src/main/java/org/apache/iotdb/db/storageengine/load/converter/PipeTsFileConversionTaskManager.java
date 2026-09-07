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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Deduplicates pipe TsFile conversion tasks. The status table is bounded, and only a bounded number
 * of tasks retain an in-memory parser checkpoint. The active-load directory remains the durable
 * source of each receiver-owned file.
 */
public final class PipeTsFileConversionTaskManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTsFileConversionTaskManager.class);

  public enum State {
    PENDING,
    RUNNING,
    PAUSED,
    SUCCESS,
    FAILED
  }

  public static final class Task {
    private final String taskId;
    private final boolean asyncLoadOnTypeMismatch;
    private volatile State state = State.PENDING;
    private volatile TSStatus status;
    private volatile boolean typeMismatchDetected;
    private volatile boolean receiverOwned;
    private boolean retrySealAllowed;
    private Object conversionContext;

    private Task(final String taskId, final boolean asyncLoadOnTypeMismatch) {
      this.taskId = taskId;
      this.asyncLoadOnTypeMismatch = asyncLoadOnTypeMismatch;
    }

    public String getTaskId() {
      return taskId;
    }

    public boolean isAsyncLoadOnTypeMismatch() {
      return asyncLoadOnTypeMismatch;
    }

    public State getState() {
      return state;
    }

    public TSStatus getStatus() {
      return status;
    }

    public boolean isTypeMismatchDetected() {
      return typeMismatchDetected;
    }

    public boolean isReceiverOwned() {
      return receiverOwned;
    }
  }

  private static final class UnretainedContext {
    private final String taskId;
    private final Object context;

    private UnretainedContext(final String taskId, final Object context) {
      this.taskId = taskId;
      this.context = context;
    }
  }

  private static final int MAX_TASKS = 4096;
  private static final int MAX_CONTEXTS =
      LoadTsFileDataTypeConverter.getTabletConversionPermitCount();
  private static final Map<String, Task> TASKS = new LinkedHashMap<>(128, 0.75F, true);
  private static final AtomicInteger RETAINED_CONTEXT_COUNT = new AtomicInteger();
  private static final ThreadLocal<String> CURRENT_TASK_ID = new ThreadLocal<>();
  // Keeps legacy seal requests (which predate conversion task ids) eligible for receiver takeover.
  private static final ThreadLocal<Boolean> CURRENT_TYPE_MISMATCH = new ThreadLocal<>();
  private static final ThreadLocal<UnretainedContext> CURRENT_UNRETAINED_CONTEXT =
      new ThreadLocal<>();

  private PipeTsFileConversionTaskManager() {
    // utility class
  }

  public static Task registerIfAbsent(final String taskId) {
    return registerIfAbsent(taskId, true);
  }

  public static Task registerIfAbsent(final String taskId, final boolean asyncLoadOnTypeMismatch) {
    if (taskId == null || taskId.isEmpty()) {
      return null;
    }
    synchronized (TASKS) {
      Task task = TASKS.get(taskId);
      if (task == null) {
        if (!hasTaskCapacity()) {
          return null;
        }
        task = new Task(taskId, asyncLoadOnTypeMismatch);
        TASKS.put(taskId, task);
      }
      return task;
    }
  }

  public static Task get(final String taskId) {
    if (taskId == null || taskId.isEmpty()) {
      return null;
    }
    synchronized (TASKS) {
      return TASKS.get(taskId);
    }
  }

  /** Returns a response for a duplicate seal, or {@code null} when no task is known. */
  public static TSStatus getDuplicateStatus(
      final String taskId, final boolean asyncLoadOnTypeMismatch) {
    if (taskId == null || taskId.isEmpty()) {
      return null;
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      return task == null ? null : getDuplicateStatus(task, asyncLoadOnTypeMismatch);
    }
  }

  /** Atomically claims a new/retryable seal or returns the status of its existing task. */
  public static TSStatus registerAndGetDuplicateStatus(
      final String taskId, final boolean asyncLoadOnTypeMismatch) {
    if (taskId == null || taskId.isEmpty()) {
      return null;
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task == null) {
        if (!hasTaskCapacity()) {
          return createReceiverTemporaryUnavailableStatus(null);
        }
        TASKS.put(taskId, new Task(taskId, asyncLoadOnTypeMismatch));
        return null;
      }
      if (task.retrySealAllowed && !task.receiverOwned) {
        task.retrySealAllowed = false;
        task.status = null;
        task.state = State.PENDING;
        return null;
      }
      return getDuplicateStatus(task, asyncLoadOnTypeMismatch);
    }
  }

  private static TSStatus getDuplicateStatus(
      final Task task, final boolean asyncLoadOnTypeMismatch) {
    if (asyncLoadOnTypeMismatch || task.isAsyncLoadOnTypeMismatch()) {
      if (task.receiverOwned || task.state == State.SUCCESS) {
        // Once the receiver owns the file, the sender must not create a second conversion task.
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      if (task.state == State.FAILED && task.status != null) {
        return toReceiverStatus(task.status);
      }
      if (task.state == State.PAUSED && task.status != null) {
        return toReceiverStatus(task.status);
      }
      return createReceiverTemporaryUnavailableStatus(null);
    }
    if (task.state == State.SUCCESS) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    if ((task.state == State.PAUSED || task.state == State.FAILED) && task.status != null) {
      return toReceiverStatus(task.status);
    }
    return createReceiverTemporaryUnavailableStatus(null);
  }

  private static TSStatus toReceiverStatus(final TSStatus status) {
    if (status == null
        || status.getCode() != TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()) {
      return status;
    }
    return createReceiverTemporaryUnavailableStatus(status.getMessage());
  }

  private static TSStatus createReceiverTemporaryUnavailableStatus(final String message) {
    return new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
        .setMessage(message);
  }

  public static void enter(final String taskId) {
    CURRENT_TYPE_MISMATCH.set(false);
    if (taskId != null && !taskId.isEmpty()) {
      final String previousTaskId = CURRENT_TASK_ID.get();
      if (previousTaskId != null && !previousTaskId.equals(taskId)) {
        clearCurrentUnretainedContext(previousTaskId);
      }
      CURRENT_TASK_ID.set(taskId);
    } else {
      clearCurrentUnretainedContext(CURRENT_TASK_ID.get());
      CURRENT_TASK_ID.remove();
    }
  }

  public static String getCurrentTaskId() {
    return CURRENT_TASK_ID.get();
  }

  @SuppressWarnings("unchecked")
  public static <T> T getOrCreateCurrentContext(final Supplier<T> supplier) {
    final String currentTaskId = CURRENT_TASK_ID.get();
    if (currentTaskId == null) {
      return supplier.get();
    }

    final UnretainedContext currentUnretainedContext = CURRENT_UNRETAINED_CONTEXT.get();
    if (currentUnretainedContext != null) {
      if (currentTaskId.equals(currentUnretainedContext.taskId)) {
        return (T) currentUnretainedContext.context;
      }
      clearCurrentUnretainedContext(currentUnretainedContext.taskId);
    }

    Object evictedContext = null;
    final T context;
    boolean retained = false;
    synchronized (TASKS) {
      final Task task = TASKS.get(currentTaskId);
      if (task == null) {
        context = supplier.get();
      } else if (task.state == State.SUCCESS || task.state == State.FAILED) {
        // A parser callback that races with terminal completion may finish its current call, but
        // it must not recreate a checkpoint for a task that is already terminal.
        context = supplier.get();
      } else if (task.conversionContext != null) {
        context = (T) task.conversionContext;
        retained = true;
      } else {
        context = supplier.get();
        final ContextReservation reservation = reserveContextSlot(task);
        evictedContext = reservation.evictedContext;
        if (reservation.slotAvailable) {
          retainContext(task, context);
          retained = true;
        }
      }
    }
    closeContext(evictedContext);
    if (!retained) {
      CURRENT_UNRETAINED_CONTEXT.set(new UnretainedContext(currentTaskId, context));
    }
    return context;
  }

  public static void clearCurrentContext() {
    clearContext(CURRENT_TASK_ID.get());
  }

  public static void clearContext(final String taskId) {
    if (taskId == null || taskId.isEmpty()) {
      return;
    }
    Object context = null;
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task != null) {
        context = detachContext(task);
      }
    }
    closeContext(context);
    clearCurrentUnretainedContext(taskId);
  }

  private static ContextReservation reserveContextSlot(final Task currentTask) {
    if (RETAINED_CONTEXT_COUNT.get() < MAX_CONTEXTS) {
      return new ContextReservation(true, null);
    }
    for (final Task task : TASKS.values()) {
      if (task != currentTask && task.conversionContext != null && task.state != State.RUNNING) {
        return new ContextReservation(true, detachContext(task));
      }
    }
    return new ContextReservation(false, null);
  }

  private static final class ContextReservation {
    private final boolean slotAvailable;
    private final Object evictedContext;

    private ContextReservation(final boolean slotAvailable, final Object evictedContext) {
      this.slotAvailable = slotAvailable;
      this.evictedContext = evictedContext;
    }
  }

  private static void retainContext(final Task task, final Object context) {
    task.conversionContext = context;
    RETAINED_CONTEXT_COUNT.incrementAndGet();
  }

  private static Object detachContext(final Task task) {
    final Object context = task.conversionContext;
    if (context != null) {
      task.conversionContext = null;
      RETAINED_CONTEXT_COUNT.decrementAndGet();
    }
    return context;
  }

  private static void closeContext(final Object context) {
    if (!(context instanceof AutoCloseable)) {
      return;
    }
    try {
      ((AutoCloseable) context).close();
    } catch (final Exception e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_FAILED_TO_CLOSE_PIPE_TSFILE_CONVERSION_CONTEXT_8E4D886B, e);
    }
  }

  public static void leave() {
    clearCurrentUnretainedContext(CURRENT_TASK_ID.get());
    CURRENT_TASK_ID.remove();
    CURRENT_TYPE_MISMATCH.remove();
  }

  private static void clearCurrentUnretainedContext(final String taskId) {
    if (taskId == null) {
      return;
    }
    final UnretainedContext context = CURRENT_UNRETAINED_CONTEXT.get();
    if (context != null && taskId.equals(context.taskId)) {
      CURRENT_UNRETAINED_CONTEXT.remove();
      closeContext(context.context);
    }
  }

  public static void markTypeMismatchDetected() {
    CURRENT_TYPE_MISMATCH.set(true);
    final String taskId = CURRENT_TASK_ID.get();
    if (taskId == null) {
      return;
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task != null && task.state != State.SUCCESS && task.state != State.FAILED) {
        task.typeMismatchDetected = true;
        task.status = null;
        task.retrySealAllowed = false;
        task.state = State.RUNNING;
      }
    }
  }

  public static boolean isTypeMismatchDetected(final String taskId) {
    if (taskId == null || taskId.isEmpty()) {
      return Boolean.TRUE.equals(CURRENT_TYPE_MISMATCH.get());
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      return task != null && task.typeMismatchDetected;
    }
  }

  public static void markReceiverOwned(final String taskId) {
    if (taskId == null || taskId.isEmpty()) {
      return;
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task != null) {
        task.receiverOwned = true;
        task.retrySealAllowed = false;
      }
    }
  }

  public static void markPending(final String taskId) {
    update(taskId, State.PENDING, null);
  }

  /**
   * Moves a locally running task back to pending before its file becomes visible to active load.
   * The caller must invoke this before moving the file so an active-load worker cannot be running
   * the same task concurrently.
   */
  public static void prepareForActiveLoad(final String taskId) {
    if (taskId == null || taskId.isEmpty()) {
      return;
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task != null && task.state != State.SUCCESS && task.state != State.FAILED) {
        task.status = null;
        task.retrySealAllowed = false;
        task.state = State.PENDING;
      }
    }
  }

  public static void markRunning(final String taskId) {
    update(taskId, State.RUNNING, null);
  }

  public static void markPaused(final String taskId, final TSStatus status) {
    update(taskId, State.PAUSED, status);
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task != null && !task.receiverOwned && task.state == State.PAUSED) {
        // A retry claims this same task and reuses its retained parser checkpoint.
        task.retrySealAllowed = true;
      }
    }
  }

  public static void markRetryable(final String taskId, final TSStatus status) {
    if (taskId == null || taskId.isEmpty()) {
      return;
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task != null
          && !task.receiverOwned
          && task.state != State.SUCCESS
          && task.state != State.FAILED) {
        task.status = status;
        task.retrySealAllowed = true;
        task.state = State.PAUSED;
      }
    }
  }

  public static void markSuccess(final String taskId) {
    complete(taskId, State.SUCCESS, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public static void markFailed(final String taskId, final TSStatus status) {
    complete(taskId, State.FAILED, status);
  }

  private static void update(final String taskId, final State state, final TSStatus status) {
    if (taskId == null || taskId.isEmpty()) {
      return;
    }
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task == null) {
        return;
      }
      if (task.state == State.SUCCESS || task.state == State.FAILED) {
        return;
      }
      if (state == State.PENDING && task.state == State.RUNNING) {
        return;
      }
      task.status = status;
      if (state == State.PENDING || state == State.RUNNING) {
        task.retrySealAllowed = false;
      }
      task.state = state;
    }
  }

  private static void complete(final String taskId, final State state, final TSStatus status) {
    if (taskId == null || taskId.isEmpty()) {
      return;
    }
    Object context = null;
    synchronized (TASKS) {
      final Task task = TASKS.get(taskId);
      if (task != null && task.state != State.SUCCESS && task.state != State.FAILED) {
        task.status = status;
        task.retrySealAllowed = false;
        context = detachContext(task);
        task.state = state;
      }
    }
    closeContext(context);
    clearCurrentUnretainedContext(taskId);
  }

  @VisibleForTesting
  static int getMaxRetainedContextCount() {
    return MAX_CONTEXTS;
  }

  @VisibleForTesting
  static int getRetainedContextCount() {
    return RETAINED_CONTEXT_COUNT.get();
  }

  private static void evictCompletedTasksIfNecessary() {
    final Iterator<Map.Entry<String, Task>> iterator = TASKS.entrySet().iterator();
    while (iterator.hasNext()) {
      final Task task = iterator.next().getValue();
      if (task.state == State.SUCCESS || task.state == State.FAILED) {
        iterator.remove();
        if (TASKS.size() < MAX_TASKS) {
          return;
        }
      }
    }
  }

  private static boolean hasTaskCapacity() {
    if (TASKS.size() >= MAX_TASKS) {
      evictCompletedTasksIfNecessary();
    }
    return TASKS.size() < MAX_TASKS;
  }
}
