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

import org.apache.iotdb.confignode.procedure.entity.SimpleSTMProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class STMProcedureTest extends TestProcedureBase {

  @Test
  public void testSubmitProcedure() {
    SimpleSTMProcedure stmProcedure = new SimpleSTMProcedure();
    long procId = this.procExecutor.submitProcedure(stmProcedure);
    ProcedureTestUtil.waitForProcedure(this.procExecutor, procId);
    TestProcEnv env = this.getEnv();
    AtomicInteger acc = env.getAcc();
    Assert.assertEquals(acc.get(), 10);
  }

  @Test
  public void testRolledBackProcedure() {
    SimpleSTMProcedure stmProcedure = new SimpleSTMProcedure();
    stmProcedure.throwAtIndex = 4;
    long procId = this.procExecutor.submitProcedure(stmProcedure);
    ProcedureTestUtil.waitForProcedure(this.procExecutor, procId);
    TestProcEnv env = this.getEnv();
    AtomicInteger acc = env.getAcc();
    int success = env.successCount.get();
    int rolledback = env.rolledBackCount.get();
    System.out.println(acc.get());
    System.out.println(success);
    System.out.println(rolledback);
    Assert.assertEquals(1 + success - rolledback, acc.get());
  }

  @Test
  public void testFailedBeforeExecutionCanRollbackWithoutState() throws Exception {
    final RetryingRollbackProcedure procedure = new RetryingRollbackProcedure();
    procedure.setFailure(procedure.originalFailure);
    procedure.doRollback(env);
    Assert.assertEquals(Arrays.asList(0), procedure.attemptedStates);
    Assert.assertSame(procedure.originalFailure, procedure.getException());
  }

  @Test
  public void testFailedRollbackRetainsStateAfterSerialization() throws Exception {
    final RetryingRollbackProcedure procedure = new RetryingRollbackProcedure();
    procedure.setState(ProcedureState.RUNNABLE);
    procedure.doExecute(env);
    procedure.doExecute(env);
    try {
      procedure.doRollback(env);
      Assert.fail("Compensation should fail while the Region is unavailable");
    } catch (IOException expected) {
      Assert.assertEquals(Arrays.asList(1), procedure.attemptedStates);
    }
    procedure.setTimeout(1000);
    procedure.setState(ProcedureState.WAITING_TIMEOUT);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    procedure.serialize(new DataOutputStream(bytes));
    final RetryingRollbackProcedure restored = new RetryingRollbackProcedure();
    restored.deserialize(ByteBuffer.wrap(bytes.toByteArray()));
    Assert.assertTrue(restored.isFailed());
    Assert.assertEquals(
        procedure.originalFailure.getMessage(), restored.getException().getMessage());
    restored.attemptedStates.addAll(Arrays.asList(1, 1));
    restored.doRollback(env);
    restored.doRollback(env);
    Assert.assertEquals(Arrays.asList(1, 1, 1, 0), restored.attemptedStates);
  }

  @Test
  public void testFailedRollbackRetriesSameStateWithoutOverwritingFailure() throws Exception {
    final RetryingRollbackProcedure procedure = new RetryingRollbackProcedure();
    final long procId = procExecutor.submitProcedure(procedure);
    Assert.assertTrue(procedure.firstFailure.await(5, TimeUnit.SECONDS));
    Assert.assertTrue(procedure.completed.await(5, TimeUnit.SECONDS));
    ProcedureTestUtil.waitForProcedure(procExecutor, procId);
    Assert.assertTrue(procedure.isFinished());
    Assert.assertEquals(Arrays.asList(1, 1, 1, 0), procedure.attemptedStates);
    Assert.assertSame(procedure.originalFailure, procedure.getException());
  }

  private static class RetryingRollbackProcedure
      extends StateMachineProcedure<TestProcEnv, Integer> {
    private final CountDownLatch firstFailure = new CountDownLatch(1);
    private final CountDownLatch completed = new CountDownLatch(1);
    private final List<Integer> attemptedStates = new ArrayList<>();
    private final ProcedureException originalFailure = new ProcedureException("Execution failed");

    @Override
    protected Flow executeFromState(TestProcEnv env, Integer state) {
      if (state == 0) {
        setNextState(1);
        return Flow.HAS_MORE_STATE;
      }
      setFailure(originalFailure);
      return Flow.NO_MORE_STATE;
    }

    @Override
    protected void rollbackState(TestProcEnv env, Integer state) throws IOException {
      attemptedStates.add(state);
      if (state == 1 && attemptedStates.size() < 3) {
        firstFailure.countDown();
        throw new IOException("Region temporarily unavailable");
      }
      if (state == 0) {
        completed.countDown();
      }
    }

    @Override
    protected long getRollbackRetryTimeout() {
      return 50;
    }

    @Override
    protected Integer getState(int stateId) {
      return stateId;
    }

    @Override
    protected int getStateId(Integer state) {
      return state;
    }

    @Override
    protected Integer getInitialState() {
      return 0;
    }
  }

  @Test
  public void testEofStateReexecutionDoesNotCallExecuteFromState() throws Exception {
    EofReexecutionProcedure procedure = new EofReexecutionProcedure();
    procedure.setProcId(1);
    procedure.setState(ProcedureState.RUNNABLE);

    forceEofStateWithHasMoreFlow(procedure);

    Assert.assertEquals(0, procedure.doExecute(env).length);
    Assert.assertEquals(0, procedure.executeCount);
  }

  private static void forceEofStateWithHasMoreFlow(StateMachineProcedure<?, ?> procedure)
      throws Exception {
    Field eofStateField = StateMachineProcedure.class.getDeclaredField("EOF_STATE");
    eofStateField.setAccessible(true);

    Field statesField = StateMachineProcedure.class.getDeclaredField("states");
    statesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentLinkedDeque<Integer> states =
        (ConcurrentLinkedDeque<Integer>) statesField.get(procedure);
    states.clear();
    states.add(eofStateField.getInt(null));

    Field stateFlowField = StateMachineProcedure.class.getDeclaredField("stateFlow");
    stateFlowField.setAccessible(true);
    stateFlowField.set(procedure, StateMachineProcedure.Flow.HAS_MORE_STATE);
  }

  private static class EofReexecutionProcedure
      extends StateMachineProcedure<TestProcEnv, EofReexecutionProcedure.TestState> {

    private int executeCount = 0;

    private enum TestState {
      STEP
    }

    @Override
    protected Flow executeFromState(TestProcEnv testProcEnv, TestState testState) {
      executeCount++;
      return Flow.NO_MORE_STATE;
    }

    @Override
    protected void rollbackState(TestProcEnv testProcEnv, TestState testState) {
      // No rollback work is required for this regression test.
    }

    @Override
    protected TestState getState(int stateId) {
      return TestState.values()[stateId];
    }

    @Override
    protected int getStateId(TestState testState) {
      return testState.ordinal();
    }

    @Override
    protected TestState getInitialState() {
      return TestState.STEP;
    }
  }
}
