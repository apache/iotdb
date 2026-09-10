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
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedDeque;
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
