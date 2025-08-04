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

import org.apache.thrift.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

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

  private Flow stateFlow = Flow.HAS_MORE_STATE;
  private final ConcurrentLinkedDeque<Integer> states = new ConcurrentLinkedDeque<>();

  private final List<Procedure<?>> subProcList = new ArrayList<>();

  /** Cycles on the same state. Good for figuring if we are stuck. */
  private int cycles = 0;

  private static final int NO_NEXT_STATE = -1;
  private int nextState = NO_NEXT_STATE;

  /** Mark whether this procedure is called by a pipe forwarded request. */
  protected boolean isGeneratedByPipe;

  private boolean isStateDeserialized = false;

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
  protected abstract Flow executeFromState(Env env, TState state) throws InterruptedException;

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
  }

  /**
   * Add a child procedure to execute.
   *
   * @param childProcedure the child procedure
   */
  protected void addChildProcedure(Procedure<Env> childProcedure) {
    subProcList.add(childProcedure);
  }

  @Override
  protected Procedure<Env>[] execute(final Env env) throws InterruptedException {
    updateTimestamp();
    try {
      if (noMoreState() || isFailed()) {
        return null;
      }

      TState state = getCurrentState();

      // init for the first execution
      if (states.isEmpty()) {
        setNextState(getStateId(state));
        addNextStateAndCalculateCycles();
      }

      LOG.trace("{}", this);
      stateFlow = executeFromState(env, state);
      if (!isFailed()) {
        addNextStateAndCalculateCycles();
      }
      setStateDeserialized(false);

      if (!subProcList.isEmpty()) {
        Procedure<Env>[] subProcedures = subProcList.toArray(new Procedure[0]);
        subProcList.clear();
        return subProcedures;
      }
      return (isWaiting() || isFailed() || noMoreState()) ? null : new Procedure[] {this};
    } finally {
      updateTimestamp();
    }
  }

  private void addNextStateAndCalculateCycles() {
    int stateToBeAdded = EOF_STATE;
    if (Flow.HAS_MORE_STATE == stateFlow) {
      if (nextState == NO_NEXT_STATE) {
        LOG.error(
            "StateMachineProcedure pid={} not set next state, but return HAS_MORE_STATE. It is likely that there is some problem with the code. Please check the code. This procedure is about to be terminated: {}",
            getProcId(),
            this);
        stateFlow = Flow.NO_MORE_STATE;
      } else {
        stateToBeAdded = nextState;
      }
    }
    if (Flow.NO_MORE_STATE == stateFlow) {
      if (nextState != NO_NEXT_STATE) {
        LOG.warn(
            "StateMachineProcedure pid={} set next state to {}, but return NO_MORE_STATE",
            getProcId(),
            nextState);
      }
    }
    if (getStateId(getCurrentState()) == stateToBeAdded) {
      cycles++;
    } else {
      cycles = 0;
    }
    states.add(stateToBeAdded);
    nextState = NO_NEXT_STATE;
  }

  @Override
  protected void rollback(final Env env)
      throws IOException, InterruptedException, ProcedureException {
    if (isEofState()) {
      states.removeLast();
    }

    try {
      updateTimestamp();
      rollbackState(env, getCurrentState());
    } finally {
      states.removeLast();
      updateTimestamp();
    }
  }

  protected boolean isEofState() {
    return !states.isEmpty() && states.getLast() == EOF_STATE;
  }

  /**
   * Used by the default implementation of abort() to know if the current state can be aborted and
   * rollback can be triggered.
   */
  protected boolean isRollbackSupported(final TState state) {
    return false;
  }

  private boolean noMoreState() {
    return stateFlow == Flow.NO_MORE_STATE;
  }

  @Nullable
  protected TState getCurrentState() {
    if (!states.isEmpty()) {
      if (states.getLast() == EOF_STATE) {
        return null;
      }
      return getState(states.getLast());
    }
    return getInitialState();
  }

  /**
   * Set the next state for the procedure.
   *
   * @param stateId the ordinal() of the state enum (or state id)
   */
  private void setNextState(final int stateId) {
    nextState = stateId;
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

    // Ensure that the Size does not differ from the actual length during the reading process
    final ArrayList<Integer> copyStates = new ArrayList<>(states);
    stream.writeInt(copyStates.size());
    for (int state : copyStates) {
      stream.writeInt(state);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    int stateCount = byteBuffer.getInt();
    states.clear();
    if (stateCount > 0) {
      for (int i = 0; i < stateCount; ++i) {
        states.add(byteBuffer.getInt());
      }
      if (isEofState()) {
        stateFlow = Flow.NO_MORE_STATE;
      }
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
    return isStateDeserialized;
  }

  private void setStateDeserialized(boolean isDeserialized) {
    this.isStateDeserialized = isDeserialized;
  }
}
