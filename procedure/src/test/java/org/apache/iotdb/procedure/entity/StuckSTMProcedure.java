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

package org.apache.iotdb.procedure.entity;

import org.apache.iotdb.procedure.StateMachineProcedure;
import org.apache.iotdb.procedure.TestProcEnv;
import org.apache.iotdb.procedure.exception.ProcedureException;
import org.apache.iotdb.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.procedure.exception.ProcedureYieldException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class StuckSTMProcedure
    extends StateMachineProcedure<TestProcEnv, StuckSTMProcedure.TestState> {
  private int childCount = 0;

  public StuckSTMProcedure() {}

  public StuckSTMProcedure(int childCount) {
    this.childCount = childCount;
  }

  public enum TestState {
    STEP_1,
    STEP_2,
    STEP_3
  }

  @Override
  protected Flow executeFromState(TestProcEnv testProcEnv, TestState testState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    AtomicInteger acc = testProcEnv.getAcc();
    try {
      switch (testState) {
        case STEP_1:
          acc.getAndAdd(1);
          setNextState(TestState.STEP_2);
          break;
        case STEP_2:
          for (int i = 0; i < childCount; i++) {
            SleepProcedure child = new SleepProcedure();
            addChildProcedure(child);
          }
          setNextState(TestState.STEP_3);
          break;
        case STEP_3:
          acc.getAndAdd(-1);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(testState)) {
        setFailure("proc failed", new ProcedureException(e));
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean isRollbackSupported(TestState testState) {
    return true;
  }

  @Override
  protected void rollbackState(TestProcEnv testProcEnv, TestState testState)
      throws IOException, InterruptedException {}

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
    return TestState.STEP_1;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) throws IOException {
    super.serialize(byteBuffer);
    byteBuffer.putInt(childCount);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) throws IOException {
    super.deserialize(byteBuffer);
    this.childCount = byteBuffer.getInt();
  }
}
