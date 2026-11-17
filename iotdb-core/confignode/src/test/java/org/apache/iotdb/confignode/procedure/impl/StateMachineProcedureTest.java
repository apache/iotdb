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
   * A procedure whose commit point is the completion of {@code APPLY}: it stays rollbackable while
   * it is at {@code PREPARE} or {@code APPLY}, and not once it has moved on to {@code RELEASE}.
   */
  private static class CommitPointProcedure extends StateMachineProcedure<TestProcEnv, State> {

    private boolean failAtApply;
    private boolean failAtRelease;

    @Override
    protected Flow executeFromState(final TestProcEnv env, final State state) {
      switch (state) {
        case PREPARE:
          setNextState(State.APPLY);
          break;
        case APPLY:
          if (failAtApply) {
            setFailure(new ProcedureException("apply failed"));
          } else {
            setNextState(State.RELEASE);
          }
          break;
        case RELEASE:
          if (failAtRelease) {
            setFailure(new ProcedureException("release failed"));
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new IllegalStateException();
      }
      return Flow.HAS_MORE_STATE;
    }

    @Override
    protected void rollbackState(final TestProcEnv env, final State state) {
      // The point of the test is that this is never called once the commit point has been passed.
    }

    @Override
    protected boolean isRollbackSupported(final State state) {
      return state != State.RELEASE;
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
  public void rollbackIsAllowedUntilTheStateAfterTheCommitPointIsReached()
      throws InterruptedException {
    final CommitPointProcedure procedure = new CommitPointProcedure();

    // PREPARE has completed and APPLY is scheduled.
    procedure.runStep();
    Assert.assertEquals(State.APPLY, procedure.currentState());
    Assert.assertTrue(procedure.isRollbackSupported());

    // APPLY has completed: the procedure is at RELEASE and must not be rolled back any more.
    procedure.runStep();
    Assert.assertEquals(State.RELEASE, procedure.currentState());
    Assert.assertFalse(procedure.isRollbackSupported());

    // Every state has run; the decision falls on the last state which ran, not on the end marker.
    procedure.runStep();
    Assert.assertFalse(procedure.isRollbackSupported());
  }

  @Test
  public void aFailureBeforeTheCommitPointStillAllowsTheRollback() throws InterruptedException {
    final CommitPointProcedure procedure = new CommitPointProcedure();
    procedure.runStep();

    procedure.failAtApply = true;
    procedure.runStep();

    Assert.assertEquals(State.APPLY, procedure.currentState());
    Assert.assertTrue(procedure.isRollbackSupported());
  }

  @Test
  public void aFailureAfterTheCommitPointDoesNotAllowTheRollback() throws InterruptedException {
    final CommitPointProcedure procedure = new CommitPointProcedure();
    procedure.runStep();
    procedure.runStep();

    procedure.failAtRelease = true;
    procedure.runStep();

    Assert.assertFalse(procedure.isRollbackSupported());
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
    Assert.assertFalse(procedure.isRollbackSupported());

    procedure.abandon();

    // A recovered rollback rebuilds its stack from the persisted indexes, so they must be gone.
    Assert.assertNull(procedure.stackIndexes());
    // An empty history reports the initial state, which is how a drained history shows up.
    Assert.assertEquals(State.PREPARE, procedure.currentState());
  }

  @Test
  public void aProcedureWhichOverridesNothingIsRollbackableEverywhere() {
    Assert.assertTrue(new SimpleSTMProcedure().isRollbackSupported());
  }
}
