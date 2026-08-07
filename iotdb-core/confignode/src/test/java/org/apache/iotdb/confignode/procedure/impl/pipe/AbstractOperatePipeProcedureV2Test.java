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

package org.apache.iotdb.confignode.procedure.impl.pipe;

import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.task.OperatePipeTaskState;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class AbstractOperatePipeProcedureV2Test {

  @Test
  public void testSuccessfulStateDoesNotYield() throws Exception {
    final TestOperatePipeProcedure procedure = new TestOperatePipeProcedure();

    Assert.assertEquals(
        StateMachineProcedure.Flow.HAS_MORE_STATE,
        procedure.executeFromState(null, OperatePipeTaskState.VALIDATE_TASK));

    Assert.assertFalse(procedure.isYieldAfterExecution(null));
    Assert.assertEquals(1, procedure.validateExecutionCount);
  }

  @Test
  public void testRetryStateYieldsAndResetsAfterNextExecution() throws Exception {
    final TestOperatePipeProcedure procedure = new TestOperatePipeProcedure();
    procedure.failValidation = true;

    Assert.assertEquals(
        StateMachineProcedure.Flow.HAS_MORE_STATE,
        procedure.executeFromState(null, OperatePipeTaskState.VALIDATE_TASK));

    Assert.assertTrue(procedure.isYieldAfterExecution(null));
    Assert.assertEquals(1, procedure.validateExecutionCount);

    procedure.failValidation = false;
    Assert.assertEquals(
        StateMachineProcedure.Flow.HAS_MORE_STATE,
        procedure.executeFromState(null, OperatePipeTaskState.VALIDATE_TASK));

    Assert.assertFalse(procedure.isYieldAfterExecution(null));
    Assert.assertEquals(2, procedure.validateExecutionCount);
  }

  @Test
  public void testRetryStateYieldsOnlyBeforeRetryThreshold() throws Exception {
    final TestOperatePipeProcedure procedure = new TestOperatePipeProcedure();

    final Procedure<?>[] validateSubProcedures = procedure.runOnce();
    Assert.assertEquals(1, validateSubProcedures.length);
    Assert.assertSame(procedure, validateSubProcedures[0]);
    Assert.assertFalse(procedure.isYieldAfterExecution(null));

    procedure.failCalculation = true;
    final Procedure<?>[] calculateSubProcedures = procedure.runOnce();
    Assert.assertEquals(1, calculateSubProcedures.length);
    Assert.assertSame(procedure, calculateSubProcedures[0]);
    Assert.assertTrue(procedure.isYieldAfterExecution(null));
    Assert.assertEquals(1, procedure.calculateExecutionCount);

    final Procedure<?>[] failedSubProcedures = procedure.runOnce();
    Assert.assertEquals(0, failedSubProcedures.length);
    Assert.assertTrue(procedure.hasException());
    Assert.assertFalse(procedure.isYieldAfterExecution(null));
    Assert.assertEquals(2, procedure.calculateExecutionCount);
  }

  @Test
  public void testTimeoutDiagnosticReportsCurrentStateAndRetryReason() throws Exception {
    final TestOperatePipeProcedure procedure = new TestOperatePipeProcedure();
    procedure.failValidation = true;

    procedure.executeFromState(null, OperatePipeTaskState.VALIDATE_TASK);

    final String diagnosticMessage = procedure.getTimeoutDiagnosticMessage();
    Assert.assertTrue(diagnosticMessage.contains("START_PIPE"));
    Assert.assertTrue(diagnosticMessage.contains("VALIDATE_TASK"));
    Assert.assertTrue(diagnosticMessage.contains("retry"));
  }

  @Test
  public void testTimeoutDiagnosticReportsDataNodeOperation() throws Exception {
    final TestOperatePipeProcedure procedure = new TestOperatePipeProcedure();
    procedure.setPendingDataNodeIdsForTest(new HashSet<>(Arrays.asList(3, 1)));

    procedure.executeFromState(null, OperatePipeTaskState.OPERATE_ON_DATA_NODES);

    final String diagnosticMessage = procedure.getTimeoutDiagnosticMessage();
    Assert.assertTrue(diagnosticMessage.contains("OPERATE_ON_DATA_NODES"));
    Assert.assertTrue(diagnosticMessage.contains("DataNodes [1, 3]"));
    Assert.assertTrue(diagnosticMessage.contains("SHOW CLUSTER"));
  }

  private static class TestOperatePipeProcedure extends AbstractOperatePipeProcedureV2 {

    private int validateExecutionCount;
    private int calculateExecutionCount;
    private boolean failValidation;
    private boolean failCalculation;

    private TestOperatePipeProcedure() {
      pipeTaskInfo = new AtomicReference<>(new PipeTaskInfo());
    }

    private Procedure<?>[] runOnce() throws InterruptedException {
      return execute(null);
    }

    private void setPendingDataNodeIdsForTest(final Set<Integer> pendingDataNodeIds) {
      setPendingDataNodeIds(pendingDataNodeIds);
    }

    @Override
    protected PipeTaskOperation getOperation() {
      return PipeTaskOperation.START_PIPE;
    }

    @Override
    public boolean executeFromValidateTask(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env)
        throws PipeException {
      validateExecutionCount++;
      if (failValidation) {
        throw new PipeException("retry");
      }
      return true;
    }

    @Override
    public void executeFromCalculateInfoForTask(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env) {
      calculateExecutionCount++;
      if (failCalculation) {
        throw new RuntimeException("retry");
      }
    }

    @Override
    public void executeFromWriteConfigNodeConsensus(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env) {
      // Do nothing
    }

    @Override
    public void executeFromOperateOnDataNodes(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env) {
      // Do nothing
    }

    @Override
    public void rollbackFromValidateTask(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env) {
      // Do nothing
    }

    @Override
    public void rollbackFromCalculateInfoForTask(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env) {
      // Do nothing
    }

    @Override
    public void rollbackFromWriteConfigNodeConsensus(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env) {
      // Do nothing
    }

    @Override
    public void rollbackFromOperateOnDataNodes(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env)
        throws IOException {
      // Do nothing
    }
  }
}
