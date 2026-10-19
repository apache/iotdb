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

import org.apache.iotdb.confignode.procedure.entity.SimpleSTMProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;

import org.junit.Assert;
import org.junit.Test;

public class StateMachineProcedureTest {

  private enum State {
    PREPARE,
    APPLY,
    RELEASE
  }

  /**
   * A procedure which must not be rolled back once {@code APPLY} has completed, the way the table
   * procedures treat the state which applies their change on the ConfigNode.
   */
  private static class CommitPointProcedure extends StateMachineProcedure<TestProcEnv, State> {

    private boolean failAtApply;

    /** How many times APPLY schedules itself before it moves on. */
    private int applyRepeats;

    @Override
    protected Flow executeFromState(final TestProcEnv env, final State state) {
      switch (state) {
        case PREPARE:
          setNextState(State.APPLY);
          break;
        case APPLY:
          if (failAtApply) {
            setFailure(new ProcedureException("apply failed"));
          } else if (applyRepeats-- > 0) {
            // A state which keeps scheduling itself, the way one waiting for a condition does.
            setNextState(State.APPLY);
          } else {
            setNextState(State.RELEASE);
          }
          break;
        case RELEASE:
          return Flow.NO_MORE_STATE;
        default:
          throw new IllegalStateException();
      }
      return Flow.HAS_MORE_STATE;
    }

    @Override
    protected void rollbackState(final TestProcEnv env, final State state) {
      // The point of the test is that this is never reached after the commit state.
    }

    @Override
    public boolean isRollbackAllowed() {
      return !hasCompletedState(State.APPLY);
    }

    @Override
    protected State getState(final int stateId) {
      return State.values()[stateId];
    }

    @Override
    protected int getStateId(final State state) {
      return state.ordinal();
    }

    @Override
    protected State getInitialState() {
      return State.PREPARE;
    }

    /** Runs one state, the way the executor does. */
    private void runStep() throws InterruptedException {
      execute(new TestProcEnv());
    }

    private boolean completed(final State state) {
      return hasCompletedState(state);
    }

    private State currentState() {
      return getCurrentState();
    }

    private void addSlot(final int index) {
      addStackIndex(index);
    }

    private int[] stackIndexes() {
      return getStackIndexes();
    }

    private void abandon() {
      abandonRollback();
    }
  }

  @Test
  public void rollbackIsAllowedUntilTheCommitStateCompletes() throws InterruptedException {
    final CommitPointProcedure procedure = new CommitPointProcedure();

    // PREPARE has completed and APPLY is scheduled, but APPLY has not run yet.
    procedure.runStep();
    Assert.assertTrue(procedure.completed(State.PREPARE));
    Assert.assertFalse(procedure.completed(State.APPLY));
    Assert.assertTrue(procedure.isRollbackAllowed());

    // APPLY has completed: nothing may be undone from now on.
    procedure.runStep();
    Assert.assertTrue(procedure.completed(State.APPLY));
    Assert.assertFalse(procedure.isRollbackAllowed());

    // A later state does not change the answer.
    procedure.runStep();
    Assert.assertFalse(procedure.isRollbackAllowed());
  }

  @Test
  public void aCommitStateWhichRunsAgainStillCountsAsCompleted() throws InterruptedException {
    final CommitPointProcedure procedure = new CommitPointProcedure();
    procedure.applyRepeats = 1;
    procedure.runStep(); // PREPARE
    procedure.runStep(); // APPLY, which schedules APPLY again

    // The last element is the APPLY which has not run yet, but the one before it has completed.
    Assert.assertEquals(State.APPLY, procedure.currentState());
    Assert.assertTrue(procedure.completed(State.APPLY));
    Assert.assertFalse(procedure.isRollbackAllowed());
  }

  @Test
  public void aCommitStateWhichFailsDoesNotBlockTheRollback() throws InterruptedException {
    final CommitPointProcedure procedure = new CommitPointProcedure();
    procedure.runStep();

    procedure.failAtApply = true;
    procedure.runStep();

    Assert.assertFalse(procedure.completed(State.APPLY));
    Assert.assertTrue(procedure.isRollbackAllowed());
  }

  @Test
  public void abandoningTheRollbackDropsTheStackSlotsAndTheStateHistory()
      throws InterruptedException {
    final CommitPointProcedure procedure = new CommitPointProcedure();
    procedure.runStep();
    procedure.runStep();
    procedure.addSlot(0);
    procedure.addSlot(1);
    procedure.addSlot(2);
    Assert.assertEquals(3, procedure.stackIndexes().length);
    Assert.assertFalse(procedure.isRollbackAllowed());

    procedure.abandon();

    // A recovered rollback rebuilds its stack from the persisted indexes, so they must be gone.
    Assert.assertNull(procedure.stackIndexes());
    // An empty history reports the initial state, which is how a drained history shows up.
    Assert.assertEquals(State.PREPARE, procedure.currentState());
  }

  @Test
  public void aProcedureWithoutACommitStateIsAlwaysRollbackable() {
    Assert.assertTrue(new SimpleSTMProcedure().isRollbackAllowed());
  }
}
