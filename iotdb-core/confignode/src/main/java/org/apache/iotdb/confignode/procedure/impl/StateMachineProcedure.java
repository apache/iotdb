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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;

import org.apache.thrift.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Procedure described by a series of steps.
 *
 * <p>The procedure implementor must have an enum of 'states', describing the various step of the
 * procedure. Once the procedure is running, the procedure-framework will call executeFromState()
 * using the 'state' provided by the user. The first call to executeFromState() will be performed
 * with 'state = null'. The implementor can jump between states using
 * setNextState(MyStateEnum.ordinal()). The rollback will call rollbackState() for each state that
 * was executed, in reverse order.
 */
public abstract class StateMachineProcedure<Env, TState> extends Procedure<Env> {
  private static final Logger LOG = LoggerFactory.getLogger(StateMachineProcedure.class);

  private static final int EOF_STATE = Integer.MIN_VALUE;

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  private Flow stateFlow = Flow.HAS_MORE_STATE;
  protected int stateCount = 0;
  private int[] states = null;

  private List<Procedure<Env>> subProcList = null;

  /** Cycles on same state. Good for figuring if we are stuck. */
  private int cycles = 0;

  /** Ordinal of the previous state. So we can tell if we are progressing or not. */
  private int previousState;

  /** Mark whether this procedure is called by a pipe forwarded request. */
  protected boolean isGeneratedByPipe;

  private boolean stateDeserialized = false;

  protected StateMachineProcedure() {
    this(false);
  }

  protected StateMachineProcedure(final boolean isGeneratedByPipe) {
    this.isGeneratedByPipe = isGeneratedByPipe;
  }

  public enum Flow {
    HAS_MORE_STATE,
    NO_MORE_STATE,
  }

  protected final int getCycles() {
    return cycles;
  }

  /**
   * Called to perform a single step of the specified 'state' of the procedure.
   *
   * @param state state to execute
   * @return Flow.NO_MORE_STATE if the procedure is completed, Flow.HAS_MORE_STATE if there is
   *     another step.
   */
  protected abstract Flow executeFromState(Env env, TState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException;

  /**
   * Called to perform the rollback of the specified state.
   *
   * @param state state to rollback
   * @throws IOException temporary failure, the rollback will retry later
   */
  protected abstract void rollbackState(Env env, TState state)
      throws IOException, InterruptedException, ProcedureException;

  /**
   * Convert an ordinal (or state id) to an Enum (or more descriptive) state object.
   *
   * @param stateId the ordinal() of the state enum (or state id)
   * @return the state enum object
   */
  protected abstract TState getState(int stateId);

  /**
   * Convert the Enum (or more descriptive) state object to an ordinal (or state id).
   *
   * @param state the state enum object
   * @return stateId the ordinal() of the state enum (or state id)
   */
  protected abstract int getStateId(TState state);

  /**
   * Return the initial state object that will be used for the first call to executeFromState().
   *
   * @return the initial state enum object
   */
  protected abstract TState getInitialState();

  /**
   * Set the next state for the procedure.
   *
   * @param state the state enum object
   */
  protected void setNextState(final TState state) {
    setNextState(getStateId(state));
    failIfAborted();
  }

  /**
   * By default, the executor will try ro run all the steps of the procedure start to finish. Return
   * true to make the executor yield between execution steps to give other procedures time to run
   * their steps.
   *
   * @param state the state we are going to execute next.
   * @return Return true if the executor should yield before the execution of the specified step.
   *     Defaults to return false.
   */
  protected boolean isYieldBeforeExecuteFromState(Env env, TState state) {
    return false;
  }

  /**
   * Add a child procedure to execute.
   *
   * @param childProcedure the child procedure
   */
  protected void addChildProcedure(Procedure<Env> childProcedure) {
    if (childProcedure == null) {
      return;
    }
    if (subProcList == null) {
      subProcList = new ArrayList<>();
    }
    subProcList.add(childProcedure);
  }

  @Override
  protected Procedure[] execute(final Env env)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    updateTimestamp();
    try {
      failIfAborted();

      if (!hasMoreState() || isFailed()) {
        return null;
      }

      TState state = getCurrentState();
      if (stateCount == 0) {
        setNextState(getStateId(state));
      }

      LOG.debug("{} {}; cycles={}", state, this, cycles);
      // Keep running count of cycles
      if (getStateId(state) != this.previousState) {
        this.previousState = getStateId(state);
        this.cycles = 0;
      } else {
        this.cycles++;
      }

      LOG.trace("{}", this);
      stateFlow = executeFromState(env, state);
      setStateDeserialized(false);
      if (!hasMoreState()) {
        setNextState(EOF_STATE);
      }

      if (subProcList != null && !subProcList.isEmpty()) {
        Procedure[] subProcedures = subProcList.toArray(new Procedure[subProcList.size()]);
        subProcList = null;
        return subProcedures;
      }
      return (isWaiting() || isFailed() || !hasMoreState()) ? null : new Procedure[] {this};
    } finally {
      updateTimestamp();
    }
  }

  @Override
  protected void rollback(final Env env)
      throws IOException, InterruptedException, ProcedureException {
    if (isEofState()) {
      stateCount--;
    }

    try {
      updateTimestamp();
      rollbackState(env, getCurrentState());
    } finally {
      stateCount--;
      updateTimestamp();
    }
  }

  protected boolean isEofState() {
    return stateCount > 0 && states[stateCount - 1] == EOF_STATE;
  }

  @Override
  protected boolean abort(final Env env) {
    LOG.debug("Abort requested for {}", this);
    if (!hasMoreState()) {
      LOG.warn("Ignore abort request on {} because it has already been finished", this);
      return false;
    }
    if (!isRollbackSupported(getCurrentState())) {
      LOG.warn("Ignore abort request on {} because it does not support rollback", this);
      return false;
    }
    aborted.set(true);
    return true;
  }

  /**
   * If procedure has more states then abort it otherwise procedure is finished and abort can be
   * ignored.
   */
  protected final void failIfAborted() {
    if (aborted.get()) {
      if (hasMoreState()) {
        setAbortFailure(getClass().getSimpleName(), "abort requested");
      } else {
        LOG.warn("Ignoring abort request on state='{}' for {}", getCurrentState(), this);
      }
    }
  }

  /**
   * Used by the default implementation of abort() to know if the current state can be aborted and
   * rollback can be triggered.
   */
  protected boolean isRollbackSupported(final TState state) {
    return false;
  }

  @Override
  protected boolean isYieldAfterExecution(final Env env) {
    return isYieldBeforeExecuteFromState(env, getCurrentState());
  }

  private boolean hasMoreState() {
    return stateFlow != Flow.NO_MORE_STATE;
  }

  @Nullable
  protected TState getCurrentState() {
    if (stateCount > 0) {
      if (states[stateCount - 1] == EOF_STATE) {
        return null;
      }
      return getState(states[stateCount - 1]);
    }
    return getInitialState();
  }

  /**
   * This method is used from test code as it cannot be assumed that state transition will happen
   * sequentially. Some procedures may skip steps/ states, some may add intermediate steps in
   * future.
   */
  public int getCurrentStateId() {
    return getStateId(getCurrentState());
  }

  /**
   * Set the next state for the procedure.
   *
   * @param stateId the ordinal() of the state enum (or state id)
   */
  private void setNextState(final int stateId) {
    if (states == null || states.length == stateCount) {
      int newCapacity = stateCount + 8;
      if (states != null) {
        states = Arrays.copyOf(states, newCapacity);
      } else {
        states = new int[newCapacity];
      }
    }
    states[stateCount++] = stateId;
  }

  @Override
  protected void toStringState(StringBuilder builder) {
    super.toStringState(builder);
    if (!isFinished() && !isEofState() && getCurrentState() != null) {
      builder.append(":").append(getCurrentState());
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    stream.writeInt(stateCount);
    for (int i = 0; i < stateCount; ++i) {
      stream.writeInt(states[i]);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    stateCount = byteBuffer.getInt();
    if (stateCount > 0) {
      states = new int[stateCount];
      for (int i = 0; i < stateCount; ++i) {
        states[i] = byteBuffer.getInt();
      }
      if (isEofState()) {
        stateFlow = Flow.NO_MORE_STATE;
      }
    } else {
      states = null;
    }
    this.setStateDeserialized(true);
  }

  /**
   * The isStateDeserialized indicates whether the current stage of this procedure was generated by
   * deserialization. If true, this means the procedure has undergone a leader switch or a restart
   * recovery at this stage. After the procedure is recovered, you may not want to re-execute all
   * the code in this stage, which is the purpose of this variable.
   */
  public boolean isStateDeserialized() {
    return stateDeserialized;
  }

  private void setStateDeserialized(boolean isDeserialized) {
    this.stateDeserialized = isDeserialized;
  }
}
