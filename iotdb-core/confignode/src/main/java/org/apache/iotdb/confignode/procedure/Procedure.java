1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
1import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
1import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
1import org.apache.iotdb.confignode.procedure.state.ProcedureState;
1import org.apache.iotdb.confignode.procedure.store.IProcedureStore;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.io.DataOutputStream;
1import java.io.IOException;
1import java.nio.ByteBuffer;
1import java.nio.charset.StandardCharsets;
1import java.util.ArrayList;
1import java.util.Arrays;
1import java.util.List;
1import java.util.concurrent.atomic.AtomicReference;
1
1/**
1 * Abstract class of all procedures.
1 *
1 * @param <Env>
1 */
1public abstract class Procedure<Env> implements Comparable<Procedure<Env>> {
1  private static final Logger LOG = LoggerFactory.getLogger(Procedure.class);
1  public static final long NO_PROC_ID = -1;
1  public static final long NO_TIMEOUT = -1;
1
1  private long parentProcId = NO_PROC_ID;
1  private long rootProcId = NO_PROC_ID;
1  private long procId = NO_PROC_ID;
1  private long submittedTime;
1
1  private ProcedureState state = ProcedureState.INITIALIZING;
1  private int childrenLatch = 0;
1  private ProcedureException exception;
1
1  private volatile long timeout = NO_TIMEOUT;
1  private volatile long lastUpdate;
1
1  private final AtomicReference<byte[]> result = new AtomicReference<>();
1  private volatile boolean locked = false;
1  private boolean lockedWhenLoading = false;
1
1  private int[] stackIndexes = null;
1
1  public final boolean hasLock() {
1    return locked;
1  }
1
1  // User level code, override it if necessary
1
1  /**
1   * The main code of the procedure. It must be idempotent since execute() may be called multiple
1   * times in case of machine failure in the middle of the execution.
1   *
1   * @param env the environment passed to the ProcedureExecutor
1   * @return a set of sub-procedures to run or ourselves if there is more work to do or null if the
1   *     procedure is done.
1   * @throws InterruptedException the procedure will be added back to the queue and retried later.
1   */
1  protected abstract Procedure<Env>[] execute(Env env) throws InterruptedException;
1
1  /**
1   * The code to undo what was done by the execute() code. It is called when the procedure or one of
1   * the sub-procedures failed or an abort was requested. It should cleanup all the resources
1   * created by the execute() call. The implementation must be idempotent since rollback() may be
1   * called multiple time in case of machine failure in the middle of the execution.
1   *
1   * @param env the environment passed to the ProcedureExecutor
1   * @throws IOException temporary failure, the rollback will retry later
1   * @throws InterruptedException the procedure will be added back to the queue and retried later
1   */
1  protected abstract void rollback(Env env)
1      throws IOException, InterruptedException, ProcedureException;
1
1  public void serialize(DataOutputStream stream) throws IOException {
1    // procid
1    stream.writeLong(this.procId);
1    // state
1    stream.writeInt(this.state.ordinal());
1    // submit time
1    stream.writeLong(this.submittedTime);
1    // last updated
1    stream.writeLong(this.lastUpdate);
1    // parent id
1    stream.writeLong(this.parentProcId);
1    // time out
1    stream.writeLong(this.timeout);
1    // stack indexes
1    if (stackIndexes != null) {
1      stream.writeInt(stackIndexes.length);
1      for (int index : stackIndexes) {
1        stream.writeInt(index);
1      }
1    } else {
1      stream.writeInt(-1);
1    }
1
1    // exceptions
1    if (hasException()) {
1      // symbol of exception
1      stream.write((byte) 1);
1      // exception's name
1      String exceptionClassName = exception.getClass().getName();
1      byte[] exceptionClassNameBytes = exceptionClassName.getBytes(StandardCharsets.UTF_8);
1      stream.writeInt(exceptionClassNameBytes.length);
1      stream.write(exceptionClassNameBytes);
1      // exception's message
1      String message = this.exception.getMessage();
1      if (message != null) {
1        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
1        stream.writeInt(messageBytes.length);
1        stream.write(messageBytes);
1      } else {
1        stream.writeInt(-1);
1      }
1    } else {
1      // symbol of no exception
1      stream.write((byte) 0);
1    }
1
1    // result
1    if (result.get() != null) {
1      stream.writeInt(result.get().length);
1      stream.write(result.get());
1    } else {
1      stream.writeInt(-1);
1    }
1
1    // Has lock
1    stream.write(this.hasLock() ? (byte) 1 : (byte) 0);
1  }
1
1  public void deserialize(ByteBuffer byteBuffer) {
1    // procid
1    this.setProcId(byteBuffer.getLong());
1    // state
1    this.setState(ProcedureState.values()[byteBuffer.getInt()]);
1    //  submit time
1    this.setSubmittedTime(byteBuffer.getLong());
1    //  last updated
1    this.setLastUpdate(byteBuffer.getLong());
1    //  parent id
1    this.setParentProcId(byteBuffer.getLong());
1    //  time out
1    this.setTimeout(byteBuffer.getLong());
1    //  stack index
1    int stackIndexesLen = byteBuffer.getInt();
1    if (stackIndexesLen >= 0) {
1      List<Integer> indexList = new ArrayList<>(stackIndexesLen);
1      for (int i = 0; i < stackIndexesLen; i++) {
1        indexList.add(byteBuffer.getInt());
1      }
1      this.setStackIndexes(indexList);
1    }
1
1    // exception
1    if (byteBuffer.get() == 1) {
1      deserializeTypeInfoForCompatibility(byteBuffer);
1      int messageBytesLength = byteBuffer.getInt();
1      String errMsg = null;
1      if (messageBytesLength > 0) {
1        byte[] messageBytes = new byte[messageBytesLength];
1        byteBuffer.get(messageBytes);
1        errMsg = new String(messageBytes, StandardCharsets.UTF_8);
1      }
1      setFailure(new ProcedureException(errMsg));
1    }
1
1    // result
1    int resultLen = byteBuffer.getInt();
1    if (resultLen > 0) {
1      byte[] resultArr = new byte[resultLen];
1      byteBuffer.get(resultArr);
1    }
1    //  has lock
1    if (byteBuffer.get() == 1) {
1      this.lockedWhenLoading();
1    }
1  }
1
1  /**
1   * Deserialize class Name and load class
1   *
1   * @param byteBuffer bytebuffer
1   * @return Procedure
1   */
1  @Deprecated
1  public static void deserializeTypeInfoForCompatibility(ByteBuffer byteBuffer) {
1    int classNameBytesLen = byteBuffer.getInt();
1    byte[] classNameBytes = new byte[classNameBytesLen];
1    byteBuffer.get(classNameBytes);
1  }
1
1  /**
1   * Acquire a lock, user should override it if necessary.
1   *
1   * @param env environment
1   * @return state of lock
1   */
1  protected ProcedureLockState acquireLock(Env env) {
1    return ProcedureLockState.LOCK_ACQUIRED;
1  }
1
1  /**
1   * Release a lock, user should override it if necessary.
1   *
1   * @param env env
1   */
1  protected void releaseLock(Env env) {
1    // no op
1  }
1
1  /**
1   * Used to keep procedure lock even when the procedure is yielded or suspended.
1   *
1   * @param env env
1   * @return true if hold the lock
1   */
1  protected boolean holdLock(Env env) {
1    return false;
1  }
1
1  /**
1   * To make executor yield between each execution step to give other procedures a chance to run.
1   *
1   * @param env environment
1   * @return return true if yield is allowed.
1   */
1  protected boolean isYieldAfterExecution(Env env) {
1    return false;
1  }
1
1  // -------------------------Internal methods - called by the procedureExecutor------------------
1  /**
1   * Internal method called by the ProcedureExecutor that starts the user-level code execute().
1   *
1   * @param env execute environment
1   * @return sub procedures
1   */
1  protected Procedure<Env>[] doExecute(Env env) throws InterruptedException {
1    try {
1      updateTimestamp();
1      return execute(env);
1    } finally {
1      updateTimestamp();
1    }
1  }
1
1  /**
1   * Internal method called by the ProcedureExecutor that starts the user-level code rollback().
1   *
1   * @param env execute environment
1   * @throws IOException ioe
1   * @throws InterruptedException interrupted exception
1   */
1  public void doRollback(Env env) throws IOException, InterruptedException, ProcedureException {
1    try {
1      updateTimestamp();
1      rollback(env);
1    } finally {
1      updateTimestamp();
1    }
1  }
1
1  /**
1   * Internal method called by the ProcedureExecutor that starts the user-level code acquireLock().
1   *
1   * @param env environment
1   * @param store ProcedureStore
1   * @return ProcedureLockState
1   */
1  public final ProcedureLockState doAcquireLock(Env env, IProcedureStore store) {
1    if (lockedWhenLoading) {
1      lockedWhenLoading = false;
1      locked = true;
1      return ProcedureLockState.LOCK_ACQUIRED;
1    }
1    ProcedureLockState state = acquireLock(env);
1    if (state == ProcedureLockState.LOCK_ACQUIRED) {
1      locked = true;
1      store.update(this);
1    }
1    return state;
1  }
1
1  /**
1   * Presist lock state of the procedure
1   *
1   * @param env environment
1   * @param store ProcedureStore
1   */
1  public final void doReleaseLock(Env env, IProcedureStore store) {
1    locked = false;
1    if (getState() != ProcedureState.ROLLEDBACK) {
1      store.update(this);
1    }
1    releaseLock(env);
1  }
1
1  public final void restoreLock(Env env) {
1    if (!lockedWhenLoading) {
1      LOG.debug("{} didn't hold the lock before restarting, skip acquiring lock", this);
1      return;
1    }
1    if (isFinished()) {
1      LOG.debug("{} is already bypassed, skip acquiring lock.", this);
1      return;
1    }
1    if (getState() == ProcedureState.WAITING && !holdLock(env)) {
1      LOG.debug("{} is in WAITING STATE, and holdLock= false , skip acquiring lock.", this);
1      return;
1    }
1    LOG.debug("{} held the lock before restarting, call acquireLock to restore it.", this);
1    acquireLock(env);
1  }
1
1  @Override
1  public String toString() {
1    // Return the simple String presentation of the procedure.
1    return toStringSimpleSB().toString();
1  }
1
1  /**
1   * Build the StringBuilder for the simple form of procedure string.
1   *
1   * @return the StringBuilder
1   */
1  protected StringBuilder toStringSimpleSB() {
1    final StringBuilder sb = new StringBuilder();
1
1    sb.append("pid=");
1    sb.append(getProcId());
1
1    if (hasParent()) {
1      sb.append(", ppid=");
1      sb.append(getParentProcId());
1    }
1
1    /*
1     * TODO
1     * Enable later when this is being used.
1     * Currently owner not used.
1    if (hasOwner()) {
1      sb.append(", owner=");
1      sb.append(getOwner());
1    }*/
1
1    sb.append(", state="); // pState for Procedure State as opposed to any other kind.
1    toStringState(sb);
1
1    // Only print out locked if actually locked. Most of the time it is not.
1    if (this.locked) {
1      sb.append(", locked=").append(locked);
1    }
1    if (hasException()) {
1      sb.append(", exception=" + getException());
1    }
1
1    sb.append("; ");
1    toStringClassDetails(sb);
1
1    return sb;
1  }
1
1  /** Extend the toString() information with more procedure details */
1  public String toStringDetails() {
1    final StringBuilder sb = toStringSimpleSB();
1
1    sb.append(" submittedTime=");
1    sb.append(getSubmittedTime());
1
1    sb.append(", lastUpdate=");
1    sb.append(getLastUpdate());
1
1    final int[] stackIndices = getStackIndexes();
1    if (stackIndices != null) {
1      sb.append("\n");
1      sb.append("stackIndexes=");
1      sb.append(Arrays.toString(stackIndices));
1    }
1
1    return sb.toString();
1  }
1
1  /**
1   * Called from {@link #toString()} when interpolating {@link Procedure} State. Allows decorating
1   * generic Procedure State with Procedure particulars.
1   *
1   * @param builder Append current {@link ProcedureState}
1   */
1  protected void toStringState(StringBuilder builder) {
1    builder.append(getState());
1  }
1
1  /**
1   * Extend the toString() information with the procedure details e.g. className and parameters
1   *
1   * @param builder the string builder to use to append the proc specific information
1   */
1  protected void toStringClassDetails(StringBuilder builder) {
1    builder.append(getClass().getName());
1  }
1
1  // ==========================================================================
1  //  Those fields are unchanged after initialization.
1  //
1  //  Each procedure will get created from the user or during
1  //  ProcedureExecutor.start() during the load() phase and then submitted
1  //  to the executor. these fields will never be changed after initialization
1  // ==========================================================================
1  public long getProcId() {
1    return procId;
1  }
1
1  public boolean hasParent() {
1    return parentProcId != NO_PROC_ID;
1  }
1
1  public long getParentProcId() {
1    return parentProcId;
1  }
1
1  public long getRootProcId() {
1    return rootProcId;
1  }
1
1  public String getProcType() {
1    return getClass().getSimpleName();
1  }
1
1  public long getSubmittedTime() {
1    return submittedTime;
1  }
1
1  /** Called by the ProcedureExecutor to assign the ID to the newly created procedure. */
1  public void setProcId(long procId) {
1    this.procId = procId;
1  }
1
1  public void setProcRunnable() {
1    this.submittedTime = System.currentTimeMillis();
1    setState(ProcedureState.RUNNABLE);
1  }
1
1  /** Called by the ProcedureExecutor to assign the parent to the newly created procedure. */
1  protected void setParentProcId(long parentProcId) {
1    this.parentProcId = parentProcId;
1  }
1
1  protected void setRootProcId(long rootProcId) {
1    this.rootProcId = rootProcId;
1  }
1
1  /**
1   * Called on store load to initialize the Procedure internals after the creation/deserialization.
1   */
1  protected void setSubmittedTime(long submittedTime) {
1    this.submittedTime = submittedTime;
1  }
1
1  // ==========================================================================
1  //  runtime state - timeout related
1  // ==========================================================================
1  /**
1   * @param timeout timeout interval in msec
1   */
1  protected void setTimeout(long timeout) {
1    this.timeout = timeout;
1  }
1
1  public boolean hasTimeout() {
1    return timeout != NO_TIMEOUT;
1  }
1
1  /**
1   * @return the timeout in msec
1   */
1  public long getTimeout() {
1    return timeout;
1  }
1
1  /**
1   * Called on store load to initialize the Procedure internals after the creation/deserialization.
1   */
1  protected void setLastUpdate(long lastUpdate) {
1    this.lastUpdate = lastUpdate;
1  }
1
1  /** Called by ProcedureExecutor after each time a procedure step is executed. */
1  protected void updateTimestamp() {
1    this.lastUpdate = System.currentTimeMillis();
1  }
1
1  public long getLastUpdate() {
1    return lastUpdate;
1  }
1
1  /**
1   * Timeout of the next timeout. Called by the ProcedureExecutor if the procedure has timeout set
1   * and the procedure is in the waiting queue.
1   *
1   * @return the timestamp of the next timeout.
1   */
1  protected long getTimeoutTimestamp() {
1    return getLastUpdate() + getTimeout();
1  }
1
1  // ==========================================================================
1  //  runtime state
1  // ==========================================================================
1  /**
1   * @return the time elapsed between the last update and the start time of the procedure.
1   */
1  public long elapsedTime() {
1    return getLastUpdate() - getSubmittedTime();
1  }
1
1  /**
1   * @return the serialized result if any, otherwise null
1   */
1  public byte[] getResult() {
1    return result.get();
1  }
1
1  /**
1   * The procedure may leave a "result" on completion.
1   *
1   * @param result the serialized result that will be passed to the client
1   */
1  protected void setResult(byte[] result) {
1    this.result.set(result);
1  }
1
1  /**
1   * Will only be called when loading procedures from procedure store, where we need to record
1   * whether the procedure has already held a lock. Later we will call {@link #restoreLock(Object)}
1   * to actually acquire the lock.
1   */
1  final void lockedWhenLoading() {
1    this.lockedWhenLoading = true;
1  }
1
1  /**
1   * Can only be called when restarting, before the procedure actually being executed, as after we
1   * actually call the {@link #doAcquireLock(Object, IProcedureStore)} method, we will reset {@link
1   * #lockedWhenLoading} to false.
1   *
1   * <p>Now it is only used in the ProcedureScheduler to determine whether we should put a Procedure
1   * in front of a queue.
1   */
1  public boolean isLockedWhenLoading() {
1    return lockedWhenLoading;
1  }
1
1  // ==============================================================================================
1  //  Runtime state, updated every operation by the ProcedureExecutor
1  //
1  //  There is always 1 thread at the time operating on the state of the procedure.
1  //  The ProcedureExecutor may check and set states, or some Procecedure may
1  //  update its own state. but no concurrent updates. we use synchronized here
1  //  just because the procedure can get scheduled on different executor threads on each step.
1  // ==============================================================================================
1
1  /**
1   * @return true if the procedure is in a RUNNABLE state.
1   */
1  public synchronized boolean isRunnable() {
1    return state == ProcedureState.RUNNABLE;
1  }
1
1  public synchronized boolean isInitializing() {
1    return state == ProcedureState.INITIALIZING;
1  }
1
1  /**
1   * @return true if the procedure has failed. It may or may not have rolled back.
1   */
1  public synchronized boolean isFailed() {
1    return state == ProcedureState.FAILED || state == ProcedureState.ROLLEDBACK;
1  }
1
1  /**
1   * @return true if the procedure is finished successfully.
1   */
1  public synchronized boolean isSuccess() {
1    return state == ProcedureState.SUCCESS && !hasException();
1  }
1
1  /**
1   * @return true if the procedure is finished. The Procedure may be completed successfully or
1   *     rolledback.
1   */
1  public synchronized boolean isFinished() {
1    return isSuccess() || state == ProcedureState.ROLLEDBACK;
1  }
1
1  /**
1   * @return true if the procedure is waiting for a child to finish or for an external event.
1   */
1  public synchronized boolean isWaiting() {
1    switch (state) {
1      case WAITING:
1      case WAITING_TIMEOUT:
1        return true;
1      default:
1        break;
1    }
1    return false;
1  }
1
1  protected synchronized void setState(final ProcedureState state) {
1    this.state = state;
1    updateTimestamp();
1  }
1
1  public synchronized ProcedureState getState() {
1    return state;
1  }
1
1  protected synchronized void setFailure(final String source, final Throwable cause) {
1    setFailure(new ProcedureException(source, cause));
1  }
1
1  protected synchronized void setFailure(final ProcedureException exception) {
1    this.exception = exception;
1    if (!isFinished()) {
1      setState(ProcedureState.FAILED);
1    }
1  }
1
1  /**
1   * Called by the ProcedureExecutor when the timeout set by setTimeout() is expired.
1   *
1   * @return true to let the framework handle the timeout as abort, false in case the procedure
1   *     handled the timeout itself.
1   */
1  protected synchronized boolean setTimeoutFailure(Env env) {
1    if (state == ProcedureState.WAITING_TIMEOUT) {
1      long timeDiff = System.currentTimeMillis() - lastUpdate;
1      setFailure(
1          "ProcedureExecutor",
1          new ProcedureException("Operation timed out after " + timeDiff + " ms."));
1      return true;
1    }
1    return false;
1  }
1
1  public synchronized boolean hasException() {
1    return exception != null;
1  }
1
1  public synchronized ProcedureException getException() {
1    return exception;
1  }
1
1  /** Called by the ProcedureExecutor on procedure-load to restore the latch state */
1  protected synchronized void setChildrenLatch(int numChildren) {
1    this.childrenLatch = numChildren;
1    if (LOG.isTraceEnabled()) {
1      LOG.trace("CHILD LATCH INCREMENT SET " + this.childrenLatch, new Throwable(this.toString()));
1    }
1  }
1
1  /** Called by the ProcedureExecutor on procedure-load to restore the latch state */
1  protected synchronized void incChildrenLatch() {
1    // TODO: can this be inferred from the stack? I think so...
1    this.childrenLatch++;
1    if (LOG.isTraceEnabled()) {
1      LOG.trace("CHILD LATCH INCREMENT " + this.childrenLatch, new Throwable(this.toString()));
1    }
1  }
1
1  /** Called by the ProcedureExecutor to notify that one of the sub-procedures has completed. */
1  private synchronized boolean childrenCountDown() {
1    assert childrenLatch > 0 : this;
1    boolean b = --childrenLatch == 0;
1    if (LOG.isTraceEnabled()) {
1      LOG.trace("CHILD LATCH DECREMENT " + childrenLatch, new Throwable(this.toString()));
1    }
1    return b;
1  }
1
1  /**
1   * Try to set this procedure into RUNNABLE state. Succeeds if all subprocedures/children are done.
1   *
1   * @return True if we were able to move procedure to RUNNABLE state.
1   */
1  synchronized boolean tryRunnable() {
1    // Don't use isWaiting in the below; it returns true for WAITING and WAITING_TIMEOUT
1    if (getState() == ProcedureState.WAITING && childrenCountDown()) {
1      setState(ProcedureState.RUNNABLE);
1      return true;
1    } else {
1      return false;
1    }
1  }
1
1  protected synchronized boolean hasChildren() {
1    return childrenLatch > 0;
1  }
1
1  protected synchronized int getChildrenLatch() {
1    return childrenLatch;
1  }
1
1  /**
1   * Called by the RootProcedureState on procedure execution. Each procedure store its stack-index
1   * positions.
1   */
1  protected synchronized void addStackIndex(final int index) {
1    if (stackIndexes == null) {
1      stackIndexes = new int[] {index};
1    } else {
1      int count = stackIndexes.length;
1      stackIndexes = Arrays.copyOf(stackIndexes, count + 1);
1      stackIndexes[count] = index;
1    }
1  }
1
1  protected synchronized boolean removeStackIndex() {
1    if (stackIndexes != null && stackIndexes.length > 1) {
1      stackIndexes = Arrays.copyOf(stackIndexes, stackIndexes.length - 1);
1      return false;
1    } else {
1      stackIndexes = null;
1      return true;
1    }
1  }
1
1  /**
1   * Called on store load to initialize the Procedure internals after the creation/deserialization.
1   */
1  protected synchronized void setStackIndexes(final List<Integer> stackIndexes) {
1    this.stackIndexes = new int[stackIndexes.size()];
1    for (int i = 0; i < this.stackIndexes.length; ++i) {
1      this.stackIndexes[i] = stackIndexes.get(i);
1    }
1  }
1
1  protected synchronized boolean wasExecuted() {
1    return stackIndexes != null;
1  }
1
1  protected synchronized int[] getStackIndexes() {
1    return stackIndexes;
1  }
1
1  public void setRootProcedureId(long rootProcedureId) {
1    this.rootProcId = rootProcedureId;
1  }
1
1  /**
1   * @param a the first procedure to be compared.
1   * @param b the second procedure to be compared.
1   * @return true if the two procedures have the same parent
1   */
1  public static boolean haveSameParent(Procedure<?> a, Procedure<?> b) {
1    return a.hasParent() && b.hasParent() && (a.getParentProcId() == b.getParentProcId());
1  }
1
1  @Override
1  public int compareTo(Procedure<Env> other) {
1    return Long.compare(getProcId(), other.getProcId());
1  }
1
1  /**
1   * This function will be called just when procedure is submitted for execution.
1   *
1   * @param env The environment passed to the procedure executor
1   */
1  protected void updateMetricsOnSubmit(Env env) {
1    if (env instanceof ConfigNodeProcedureEnv) {
1      ((ConfigNodeProcedureEnv) env)
1          .getConfigManager()
1          .getProcedureManager()
1          .getProcedureMetrics()
1          .updateMetricsOnSubmit(getProcType());
1    }
1  }
1
1  /**
1   * This function will be called just after procedure execution is finished. Override this method
1   * to update metrics at the end of the procedure. The default implementation adds runtime of a
1   * procedure to a time histogram for successfully completed procedures. Increments failed counter
1   * for failed procedures.
1   *
1   * @param env The environment passed to the procedure executor
1   * @param runtime Runtime of the procedure in milliseconds
1   * @param success true if procedure is completed successfully
1   */
1  protected void updateMetricsOnFinish(Env env, long runtime, boolean success) {
1    if (env instanceof ConfigNodeProcedureEnv) {
1      ((ConfigNodeProcedureEnv) env)
1          .getConfigManager()
1          .getProcedureManager()
1          .getProcedureMetrics()
1          .updateMetricsOnFinish(getProcType(), runtime, success);
1    }
1  }
1}
1