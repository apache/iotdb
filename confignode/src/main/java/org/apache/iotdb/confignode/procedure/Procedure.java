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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.exception.ProcedureAbortedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureTimeoutException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Abstract class of all procedures.
 *
 * @param <Env>
 */
public abstract class Procedure<Env> implements Comparable<Procedure<Env>> {
  private static final Logger LOG = LoggerFactory.getLogger(Procedure.class);
  public static final long NO_PROC_ID = -1;
  public static final long NO_TIMEOUT = -1;

  private long parentProcId = NO_PROC_ID;
  private long rootProcId = NO_PROC_ID;
  private long procId = NO_PROC_ID;
  private long submittedTime;

  private ProcedureState state = ProcedureState.INITIALIZING;
  private int childrenLatch = 0;
  private ProcedureException exception;

  private volatile long timeout = NO_TIMEOUT;
  private volatile long lastUpdate;

  private volatile byte[] result = null;
  private volatile boolean locked = false;
  private boolean lockedWhenLoading = false;

  private int[] stackIndexes = null;

  private boolean persist = true;

  public boolean needPersistance() {
    return this.persist;
  }

  public void resetPersistance() {
    this.persist = true;
  }

  public final void skipPersistance() {
    this.persist = false;
  }

  public final boolean hasLock() {
    return locked;
  }

  // User level code, override it if necessary

  /**
   * The main code of the procedure. It must be idempotent since execute() may be called multiple
   * times in case of machine failure in the middle of the execution.
   *
   * @param env the environment passed to the ProcedureExecutor
   * @return a set of sub-procedures to run or ourselves if there is more work to do or null if the
   *     procedure is done.
   * @throws ProcedureYieldException the procedure will be added back to the queue and retried
   *     later.
   * @throws InterruptedException the procedure will be added back to the queue and retried later.
   * @throws ProcedureSuspendedException Signal to the executor that Procedure has suspended itself
   *     and has set itself up waiting for an external event to wake it back up again.
   */
  protected abstract Procedure<Env>[] execute(Env env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException;

  /**
   * The code to undo what was done by the execute() code. It is called when the procedure or one of
   * the sub-procedures failed or an abort was requested. It should cleanup all the resources
   * created by the execute() call. The implementation must be idempotent since rollback() may be
   * called multiple time in case of machine failure in the middle of the execution.
   *
   * @param env the environment passed to the ProcedureExecutor
   * @throws IOException temporary failure, the rollback will retry later
   * @throws InterruptedException the procedure will be added back to the queue and retried later
   */
  protected abstract void rollback(Env env)
      throws IOException, InterruptedException, ProcedureException;

  /**
   * The abort() call is asynchronous and each procedure must decide how to deal with it, if they
   * want to be abortable. The simplest implementation is to have an AtomicBoolean set in the
   * abort() method and then the execute() will check if the abort flag is set or not. abort() may
   * be called multiple times from the client, so the implementation must be idempotent.
   *
   * <p>NOTE: abort() is not like Thread.interrupt(). It is just a notification that allows the
   * procedure implementor abort.
   */
  protected abstract boolean abort(Env env);

  public void serialize(DataOutputStream stream) throws IOException {
    // procid
    stream.writeLong(this.procId);
    // state
    stream.writeInt(this.state.ordinal());
    // submit time
    stream.writeLong(this.submittedTime);
    // last updated
    stream.writeLong(this.lastUpdate);
    // parent id
    stream.writeLong(this.parentProcId);
    // time out
    stream.writeLong(this.timeout);
    // stack indexes
    if (stackIndexes != null) {
      stream.writeInt(stackIndexes.length);
      for (int index : stackIndexes) {
        stream.writeInt(index);
      }
    } else {
      stream.writeInt(-1);
    }

    // exceptions
    if (hasException()) {
      stream.write((byte) 1);
      String exceptionClassName = exception.getClass().getName();
      byte[] exceptionClassNameBytes = exceptionClassName.getBytes(StandardCharsets.UTF_8);
      stream.writeInt(exceptionClassNameBytes.length);
      stream.write(exceptionClassNameBytes);
      String message = this.exception.getMessage();
      if (message != null) {
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        stream.writeInt(messageBytes.length);
        stream.write(messageBytes);
      } else {
        stream.writeInt(-1);
      }
    } else {
      stream.write((byte) 0);
    }

    // result
    if (result != null) {
      stream.writeInt(result.length);
      stream.write(result);
    } else {
      stream.writeInt(-1);
    }

    // has lock
    stream.write(this.hasLock() ? (byte) 1 : (byte) 0);
  }

  public void deserialize(ByteBuffer byteBuffer) {
    // procid
    this.setProcId(byteBuffer.getLong());
    // state
    this.setState(ProcedureState.values()[byteBuffer.getInt()]);
    //  submit time
    this.setSubmittedTime(byteBuffer.getLong());
    //  last updated
    this.setLastUpdate(byteBuffer.getLong());
    //  parent id
    this.setParentProcId(byteBuffer.getLong());
    //  time out
    this.setTimeout(byteBuffer.getLong());
    //  stack index
    int stackIndexesLen = byteBuffer.getInt();
    if (stackIndexesLen >= 0) {
      List<Integer> indexList = new ArrayList<>(stackIndexesLen);
      for (int i = 0; i < stackIndexesLen; i++) {
        indexList.add(byteBuffer.getInt());
      }
      this.setStackIndexes(indexList);
    }
    // exceptions
    if (byteBuffer.get() == 1) {
      Class<?> exceptionClass = deserializeTypeInfo(byteBuffer);
      int messageBytesLength = byteBuffer.getInt();
      String errMsg = null;
      if (messageBytesLength > 0) {
        byte[] messageBytes = new byte[messageBytesLength];
        byteBuffer.get(messageBytes);
        errMsg = new String(messageBytes, StandardCharsets.UTF_8);
      }
      ProcedureException exception;
      try {
        exception =
            (ProcedureException) exceptionClass.getConstructor(String.class).newInstance(errMsg);
      } catch (InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException e) {
        LOG.warn("Instantiation exception class failed", e);
        exception = new ProcedureException(errMsg);
      }

      setFailure(exception);
    }

    // result
    int resultLen = byteBuffer.getInt();
    if (resultLen > 0) {
      byte[] resultArr = new byte[resultLen];
      byteBuffer.get(resultArr);
    }
    //  has  lock
    if (byteBuffer.get() == 1) {
      this.lockedWhenLoading();
    }
  }

  /**
   * Deserialize class Name and load class
   *
   * @param byteBuffer bytebuffer
   * @return Procedure
   */
  public static Class<?> deserializeTypeInfo(ByteBuffer byteBuffer) {
    int classNameBytesLen = byteBuffer.getInt();
    byte[] classNameBytes = new byte[classNameBytesLen];
    byteBuffer.get(classNameBytes);
    String className = new String(classNameBytes, StandardCharsets.UTF_8);
    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Invalid procedure class", e);
    }
    return clazz;
  }

  public static Procedure<?> newInstance(ByteBuffer byteBuffer) {
    Class<?> procedureClass = deserializeTypeInfo(byteBuffer);
    Procedure<?> procedure;
    try {
      procedure = (Procedure<?>) procedureClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Instantiation failed", e);
    }
    return procedure;
  }

  /**
   * The {@link #doAcquireLock(Object, IProcedureStore)} will be split into two steps, first, it
   * will call us to determine whether we need to wait for initialization, second, it will call
   * {@link #acquireLock(Object)} to actually handle the lock for this procedure.
   *
   * @return true means we need to wait until the environment has been initialized, otherwise true.
   */
  protected boolean waitInitialized(Env env) {
    return false;
  }

  /**
   * Acquire a lock, user should override it if necessary.
   *
   * @param env environment
   * @return state of lock
   */
  protected ProcedureLockState acquireLock(Env env) {
    return ProcedureLockState.LOCK_ACQUIRED;
  }

  /**
   * Release a lock, user should override it if necessary.
   *
   * @param env env
   */
  protected void releaseLock(Env env) {
    // no op
  }

  /**
   * Used to keep procedure lock even when the procedure is yielded or suspended.
   *
   * @param env env
   * @return true if hold the lock
   */
  protected boolean holdLock(Env env) {
    return false;
  }

  /**
   * Called before the procedure is recovered and added into the queue.
   *
   * @param env environment
   */
  protected final void beforeRecover(Env env) {
    // no op
  }

  /**
   * Called when the procedure is recovered and added into the queue.
   *
   * @param env environment
   */
  protected final void afterRecover(Env env) {
    // no op
  }

  /**
   * Called when the procedure is completed (success or rollback). The procedure may use this method
   * to clean up in-memory states. This operation will not be retried on failure.
   *
   * @param env environment
   */
  protected void completionCleanup(Env env) {
    // no op
  }

  /**
   * To make executor yield between each execution step to give other procedures a chance to run.
   *
   * @param env environment
   * @return return true if yield is allowed.
   */
  protected boolean isYieldAfterExecution(Env env) {
    return false;
  }

  // -------------------------Internal methods - called by the procedureExecutor------------------
  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code execute().
   *
   * @param env execute environment
   * @return sub procedures
   */
  protected Procedure<Env>[] doExecute(Env env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    try {
      updateTimestamp();
      return execute(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code rollback().
   *
   * @param env execute environment
   * @throws IOException ioe
   * @throws InterruptedException interrupted exception
   */
  public void doRollback(Env env) throws IOException, InterruptedException, ProcedureException {
    try {
      updateTimestamp();
      rollback(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code acquireLock().
   *
   * @param env environment
   * @param store ProcedureStore
   * @return ProcedureLockState
   */
  public final ProcedureLockState doAcquireLock(Env env, IProcedureStore store) {
    if (waitInitialized(env)) {
      return ProcedureLockState.LOCK_EVENT_WAIT;
    }
    if (lockedWhenLoading) {
      lockedWhenLoading = false;
      locked = true;
      return ProcedureLockState.LOCK_ACQUIRED;
    }
    ProcedureLockState state = acquireLock(env);
    if (state == ProcedureLockState.LOCK_ACQUIRED) {
      locked = true;
      store.update(this);
    }
    return state;
  }

  /**
   * Presist lock state of the procedure
   *
   * @param env environment
   * @param store ProcedureStore
   */
  public final void doReleaseLock(Env env, IProcedureStore store) {
    locked = false;
    if (getState() != ProcedureState.ROLLEDBACK) {
      store.update(this);
    }
    releaseLock(env);
  }

  public final void restoreLock(Env env) {
    if (!lockedWhenLoading) {
      LOG.debug("{} didn't hold the lock before restarting, skip acquiring lock", this);
      return;
    }
    if (isFinished()) {
      LOG.debug("{} is already bypassed, skip acquiring lock.", this);
      return;
    }
    if (getState() == ProcedureState.WAITING && !holdLock(env)) {
      LOG.debug("{} is in WAITING STATE, and holdLock= false , skip acquiring lock.", this);
      return;
    }
    LOG.debug("{} held the lock before restarting, call acquireLock to restore it.", this);
    acquireLock(env);
  }

  @Override
  public String toString() {
    // Return the simple String presentation of the procedure.
    return toStringSimpleSB().toString();
  }

  /**
   * Build the StringBuilder for the simple form of procedure string.
   *
   * @return the StringBuilder
   */
  protected StringBuilder toStringSimpleSB() {
    final StringBuilder sb = new StringBuilder();

    sb.append("pid=");
    sb.append(getProcId());

    if (hasParent()) {
      sb.append(", ppid=");
      sb.append(getParentProcId());
    }

    /*
     * TODO
     * Enable later when this is being used.
     * Currently owner not used.
    if (hasOwner()) {
      sb.append(", owner=");
      sb.append(getOwner());
    }*/

    sb.append(", state="); // pState for Procedure State as opposed to any other kind.
    toStringState(sb);

    // Only print out locked if actually locked. Most of the time it is not.
    if (this.locked) {
      sb.append(", locked=").append(locked);
    }
    if (hasException()) {
      sb.append(", exception=" + getException());
    }

    sb.append("; ");
    toStringClassDetails(sb);

    return sb;
  }

  /** Extend the toString() information with more procedure details */
  public String toStringDetails() {
    final StringBuilder sb = toStringSimpleSB();

    sb.append(" submittedTime=");
    sb.append(getSubmittedTime());

    sb.append(", lastUpdate=");
    sb.append(getLastUpdate());

    final int[] stackIndices = getStackIndexes();
    if (stackIndices != null) {
      sb.append("\n");
      sb.append("stackIndexes=");
      sb.append(Arrays.toString(stackIndices));
    }

    return sb.toString();
  }

  protected String toStringClass() {
    StringBuilder sb = new StringBuilder();
    toStringClassDetails(sb);
    return sb.toString();
  }

  /**
   * Called from {@link #toString()} when interpolating {@link Procedure} State. Allows decorating
   * generic Procedure State with Procedure particulars.
   *
   * @param builder Append current {@link ProcedureState}
   */
  protected void toStringState(StringBuilder builder) {
    builder.append(getState());
  }

  /**
   * Extend the toString() information with the procedure details e.g. className and parameters
   *
   * @param builder the string builder to use to append the proc specific information
   */
  protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getName());
  }

  // ==========================================================================
  //  Those fields are unchanged after initialization.
  //
  //  Each procedure will get created from the user or during
  //  ProcedureExecutor.start() during the load() phase and then submitted
  //  to the executor. these fields will never be changed after initialization
  // ==========================================================================
  public long getProcId() {
    return procId;
  }

  public boolean hasParent() {
    return parentProcId != NO_PROC_ID;
  }

  public long getParentProcId() {
    return parentProcId;
  }

  public long getRootProcId() {
    return rootProcId;
  }

  public String getProcName() {
    return toStringClass();
  }

  public long getSubmittedTime() {
    return submittedTime;
  }

  /** Called by the ProcedureExecutor to assign the ID to the newly created procedure. */
  protected void setProcId(long procId) {
    this.procId = procId;
  }

  public void setProcRunnable() {
    this.submittedTime = System.currentTimeMillis();
    setState(ProcedureState.RUNNABLE);
  }

  /** Called by the ProcedureExecutor to assign the parent to the newly created procedure. */
  protected void setParentProcId(long parentProcId) {
    this.parentProcId = parentProcId;
  }

  protected void setRootProcId(long rootProcId) {
    this.rootProcId = rootProcId;
  }

  /**
   * Called on store load to initialize the Procedure internals after the creation/deserialization.
   */
  protected void setSubmittedTime(long submittedTime) {
    this.submittedTime = submittedTime;
  }

  // ==========================================================================
  //  runtime state - timeout related
  // ==========================================================================
  /** @param timeout timeout interval in msec */
  protected void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public boolean hasTimeout() {
    return timeout != NO_TIMEOUT;
  }

  /** @return the timeout in msec */
  public long getTimeout() {
    return timeout;
  }

  /**
   * Called on store load to initialize the Procedure internals after the creation/deserialization.
   */
  protected void setLastUpdate(long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  /** Called by ProcedureExecutor after each time a procedure step is executed. */
  protected void updateTimestamp() {
    this.lastUpdate = System.currentTimeMillis();
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  /**
   * Timeout of the next timeout. Called by the ProcedureExecutor if the procedure has timeout set
   * and the procedure is in the waiting queue.
   *
   * @return the timestamp of the next timeout.
   */
  protected long getTimeoutTimestamp() {
    return getLastUpdate() + getTimeout();
  }

  // ==========================================================================
  //  runtime state
  // ==========================================================================
  /** @return the time elapsed between the last update and the start time of the procedure. */
  public long elapsedTime() {
    return getLastUpdate() - getSubmittedTime();
  }

  /** @return the serialized result if any, otherwise null */
  public byte[] getResult() {
    return result;
  }

  /**
   * The procedure may leave a "result" on completion.
   *
   * @param result the serialized result that will be passed to the client
   */
  protected void setResult(byte[] result) {
    this.result = result;
  }

  /**
   * Will only be called when loading procedures from procedure store, where we need to record
   * whether the procedure has already held a lock. Later we will call {@link #restoreLock(Object)}
   * to actually acquire the lock.
   */
  final void lockedWhenLoading() {
    this.lockedWhenLoading = true;
  }

  /**
   * Can only be called when restarting, before the procedure actually being executed, as after we
   * actually call the {@link #doAcquireLock(Object, IProcedureStore)} method, we will reset {@link
   * #lockedWhenLoading} to false.
   *
   * <p>Now it is only used in the ProcedureScheduler to determine whether we should put a Procedure
   * in front of a queue.
   */
  public boolean isLockedWhenLoading() {
    return lockedWhenLoading;
  }

  // ==============================================================================================
  //  Runtime state, updated every operation by the ProcedureExecutor
  //
  //  There is always 1 thread at the time operating on the state of the procedure.
  //  The ProcedureExecutor may check and set states, or some Procecedure may
  //  update its own state. but no concurrent updates. we use synchronized here
  //  just because the procedure can get scheduled on different executor threads on each step.
  // ==============================================================================================

  /** @return true if the procedure is in a RUNNABLE state. */
  public synchronized boolean isRunnable() {
    return state == ProcedureState.RUNNABLE;
  }

  public synchronized boolean isInitializing() {
    return state == ProcedureState.INITIALIZING;
  }

  /** @return true if the procedure has failed. It may or may not have rolled back. */
  public synchronized boolean isFailed() {
    return state == ProcedureState.FAILED || state == ProcedureState.ROLLEDBACK;
  }

  /** @return true if the procedure is finished successfully. */
  public synchronized boolean isSuccess() {
    return state == ProcedureState.SUCCESS && !hasException();
  }

  /**
   * @return true if the procedure is finished. The Procedure may be completed successfully or
   *     rolledback.
   */
  public synchronized boolean isFinished() {
    return isSuccess() || state == ProcedureState.ROLLEDBACK;
  }

  /** @return true if the procedure is waiting for a child to finish or for an external event. */
  public synchronized boolean isWaiting() {
    switch (state) {
      case WAITING:
      case WAITING_TIMEOUT:
        return true;
      default:
        break;
    }
    return false;
  }

  protected synchronized void setState(final ProcedureState state) {
    this.state = state;
    updateTimestamp();
  }

  public synchronized ProcedureState getState() {
    return state;
  }

  protected synchronized void setFailure(final String source, final Throwable cause) {
    setFailure(new ProcedureException(source, cause));
  }

  protected synchronized void setFailure(final ProcedureException exception) {
    this.exception = exception;
    if (!isFinished()) {
      setState(ProcedureState.FAILED);
    }
  }

  protected void setAbortFailure(final String source, final String msg) {
    setFailure(source, new ProcedureAbortedException(msg));
  }

  /**
   * Called by the ProcedureExecutor when the timeout set by setTimeout() is expired.
   *
   * <p>Another usage for this method is to implement retrying. A procedure can set the state to
   * {@code WAITING_TIMEOUT} by calling {@code setState} method, and throw a {@link
   * ProcedureSuspendedException} to halt the execution of the procedure, and do not forget a call
   * {@link #setTimeout(long)} method to set the timeout. And you should also override this method
   * to wake up the procedure, and also return false to tell the ProcedureExecutor that the timeout
   * event has been handled.
   *
   * @return true to let the framework handle the timeout as abort, false in case the procedure
   *     handled the timeout itself.
   */
  protected synchronized boolean setTimeoutFailure(Env env) {
    if (state == ProcedureState.WAITING_TIMEOUT) {
      long timeDiff = System.currentTimeMillis() - lastUpdate;
      setFailure(
          "ProcedureExecutor",
          new ProcedureTimeoutException("Operation timed out after " + timeDiff + " ms."));
      return true;
    }
    return false;
  }

  public synchronized boolean hasException() {
    return exception != null;
  }

  public synchronized ProcedureException getException() {
    return exception;
  }

  /** Called by the ProcedureExecutor on procedure-load to restore the latch state */
  protected synchronized void setChildrenLatch(int numChildren) {
    this.childrenLatch = numChildren;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH INCREMENT SET " + this.childrenLatch, new Throwable(this.toString()));
    }
  }

  /** Called by the ProcedureExecutor on procedure-load to restore the latch state */
  protected synchronized void incChildrenLatch() {
    // TODO: can this be inferred from the stack? I think so...
    this.childrenLatch++;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH INCREMENT " + this.childrenLatch, new Throwable(this.toString()));
    }
  }

  /** Called by the ProcedureExecutor to notify that one of the sub-procedures has completed. */
  private synchronized boolean childrenCountDown() {
    assert childrenLatch > 0 : this;
    boolean b = --childrenLatch == 0;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH DECREMENT " + childrenLatch, new Throwable(this.toString()));
    }
    return b;
  }

  /**
   * Try to set this procedure into RUNNABLE state. Succeeds if all subprocedures/children are done.
   *
   * @return True if we were able to move procedure to RUNNABLE state.
   */
  synchronized boolean tryRunnable() {
    // Don't use isWaiting in the below; it returns true for WAITING and WAITING_TIMEOUT
    if (getState() == ProcedureState.WAITING && childrenCountDown()) {
      setState(ProcedureState.RUNNABLE);
      return true;
    } else {
      return false;
    }
  }

  protected synchronized boolean hasChildren() {
    return childrenLatch > 0;
  }

  protected synchronized int getChildrenLatch() {
    return childrenLatch;
  }

  /**
   * Called by the RootProcedureState on procedure execution. Each procedure store its stack-index
   * positions.
   */
  protected synchronized void addStackIndex(final int index) {
    if (stackIndexes == null) {
      stackIndexes = new int[] {index};
    } else {
      int count = stackIndexes.length;
      stackIndexes = Arrays.copyOf(stackIndexes, count + 1);
      stackIndexes[count] = index;
    }
  }

  protected synchronized boolean removeStackIndex() {
    if (stackIndexes != null && stackIndexes.length > 1) {
      stackIndexes = Arrays.copyOf(stackIndexes, stackIndexes.length - 1);
      return false;
    } else {
      stackIndexes = null;
      return true;
    }
  }

  /**
   * Called on store load to initialize the Procedure internals after the creation/deserialization.
   */
  protected synchronized void setStackIndexes(final List<Integer> stackIndexes) {
    this.stackIndexes = new int[stackIndexes.size()];
    for (int i = 0; i < this.stackIndexes.length; ++i) {
      this.stackIndexes[i] = stackIndexes.get(i);
    }
  }

  protected synchronized boolean wasExecuted() {
    return stackIndexes != null;
  }

  protected synchronized int[] getStackIndexes() {
    return stackIndexes;
  }

  /** Helper to lookup the root Procedure ID given a specified procedure. */
  protected static long getRootProcedureId(Map<Long, Procedure> procedures, Procedure proc) {
    while (proc.hasParent()) {
      proc = procedures.get(proc.getParentProcId());
      if (proc == null) {
        return NO_PROC_ID;
      }
    }
    return proc.getProcId();
  }

  public void setRootProcedureId(long rootProcedureId) {
    this.rootProcId = rootProcedureId;
  }

  /**
   * @param a the first procedure to be compared.
   * @param b the second procedure to be compared.
   * @return true if the two procedures have the same parent
   */
  public static boolean haveSameParent(Procedure<?> a, Procedure<?> b) {
    return a.hasParent() && b.hasParent() && (a.getParentProcId() == b.getParentProcId());
  }

  @Override
  public int compareTo(Procedure<Env> other) {
    return Long.compare(getProcId(), other.getProcId());
  }
}
