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

package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StartPipeProcedureV2Test {
  @Test
  public void testDataNodeConstructionFailureIsRecorded() throws Exception {
    final String pipeName = "startFailurePipe";
    final PipeTaskInfo taskInfo = new PipeTaskInfo();
    final PipeStaticMeta staticMeta =
        new PipeStaticMeta(
            pipeName,
            System.currentTimeMillis(),
            Collections.singletonMap("extractor", "iotdb-source"),
            Collections.singletonMap("processor", "do-nothing-processor"),
            Collections.singletonMap("connector", "write-back-sink"));
    final PipeRuntimeMeta runtimeMeta = new PipeRuntimeMeta(new ConcurrentHashMap<>());
    runtimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    taskInfo.createPipe(new CreatePipePlanV2(staticMeta, runtimeMeta));
    taskInfo.setPipeStatus(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));

    final StartPipeProcedureV2 procedure = new TestableStartPipeProcedureV2(pipeName, taskInfo);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConsensusManager consensusManager = mockSuccessfulConsensusWrite(env);
    final String error = "Failed to construct PipeConnector";
    final TPushPipeMetaRespExceptionMessage exceptionMessage =
        new TPushPipeMetaRespExceptionMessage(pipeName, error, System.currentTimeMillis())
            .setCreationTime(staticMeta.getCreationTime());
    Mockito.when(env.pushSinglePipeMetaToDataNodes(Mockito.any(ByteBuffer.class), Mockito.any()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                        new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
                    .setExceptionMessages(Collections.singletonList(exceptionMessage))));

    procedure.executeFromOperateOnDataNodes(env);

    final PipeRuntimeMeta recordedRuntimeMeta =
        taskInfo.getPipeMetaByPipeName(pipeName).getRuntimeMeta();
    assertEquals(PipeStatus.STOPPED, recordedRuntimeMeta.getStatus().get());
    assertEquals(
        error, recordedRuntimeMeta.getNodeId2PipeRuntimeExceptionMap().get(1).getMessage());
    assertPipeMetaChangeWasWritten(consensusManager, pipeName, error);
  }

  @Test
  public void testFailureWithoutExceptionDetailsIsNotTreatedAsSuccessfulStart() throws Exception {
    final String pipeName = "startFailureWithoutDetailsPipe";
    final PipeTaskInfo taskInfo = new PipeTaskInfo();
    final PipeStaticMeta staticMeta =
        new PipeStaticMeta(
            pipeName,
            System.currentTimeMillis(),
            Collections.singletonMap("extractor", "iotdb-source"),
            Collections.singletonMap("processor", "do-nothing-processor"),
            Collections.singletonMap("connector", "write-back-sink"));
    final PipeRuntimeMeta runtimeMeta = new PipeRuntimeMeta(new ConcurrentHashMap<>());
    runtimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    taskInfo.createPipe(new CreatePipePlanV2(staticMeta, runtimeMeta));
    taskInfo.setPipeStatus(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));

    final StartPipeProcedureV2 procedure = new TestableStartPipeProcedureV2(pipeName, taskInfo);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConsensusManager consensusManager = mockSuccessfulConsensusWrite(env);
    final String error = "DataNode failed before returning pipe-specific details";
    Mockito.when(env.pushSinglePipeMetaToDataNodes(Mockito.any(ByteBuffer.class), Mockito.any()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                        new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode())
                            .setMessage(error))
                    .setExceptionMessages(Collections.emptyList())));

    procedure.executeFromOperateOnDataNodes(env);

    final PipeRuntimeMeta recordedRuntimeMeta =
        taskInfo.getPipeMetaByPipeName(pipeName).getRuntimeMeta();
    assertEquals(PipeStatus.STOPPED, recordedRuntimeMeta.getStatus().get());
    assertTrue(recordedRuntimeMeta.getIsStoppedByRuntimeException());
    assertTrue(
        recordedRuntimeMeta
            .getNodeId2PipeRuntimeExceptionMap()
            .get(1)
            .getMessage()
            .contains(error));
    assertPipeMetaChangeWasWritten(consensusManager, pipeName, error);
  }

  @Test
  public void testRetryAfterFailureToPersistRuntimeStopDoesNotClearFailure() throws Exception {
    final String pipeName = "startFailurePersistRetryPipe";
    final PipeTaskInfo taskInfo = new PipeTaskInfo();
    final PipeStaticMeta staticMeta =
        new PipeStaticMeta(
            pipeName,
            System.currentTimeMillis(),
            Collections.singletonMap("extractor", "iotdb-source"),
            Collections.singletonMap("processor", "do-nothing-processor"),
            Collections.singletonMap("connector", "write-back-sink"));
    final PipeRuntimeMeta runtimeMeta = new PipeRuntimeMeta(new ConcurrentHashMap<>());
    runtimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    taskInfo.createPipe(new CreatePipePlanV2(staticMeta, runtimeMeta));
    taskInfo.setPipeStatus(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));

    final StartPipeProcedureV2 procedure = new TestableStartPipeProcedureV2(pipeName, taskInfo);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    final String error = "Failed to construct PipeConnector";
    final TPushPipeMetaRespExceptionMessage exceptionMessage =
        new TPushPipeMetaRespExceptionMessage(pipeName, error, System.currentTimeMillis())
            .setCreationTime(staticMeta.getCreationTime());
    Mockito.when(env.pushSinglePipeMetaToDataNodes(Mockito.any(ByteBuffer.class), Mockito.any()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                        new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
                    .setExceptionMessages(Collections.singletonList(exceptionMessage))));

    try {
      procedure.executeFromOperateOnDataNodes(env);
      fail("Expected the first attempt to fail its ConfigNode consensus write");
    } catch (final org.apache.iotdb.pipe.api.exception.PipeException expected) {
      // The procedure retries this state after the persistence failure.
    }

    final PipeRuntimeMeta runtimeMetaAfterFirstAttempt =
        taskInfo.getPipeMetaByPipeName(pipeName).getRuntimeMeta();
    assertEquals(PipeStatus.STOPPED, runtimeMetaAfterFirstAttempt.getStatus().get());
    assertTrue(runtimeMetaAfterFirstAttempt.getIsStoppedByRuntimeException());

    procedure.executeFromOperateOnDataNodes(env);

    final PipeRuntimeMeta runtimeMetaAfterRetry =
        taskInfo.getPipeMetaByPipeName(pipeName).getRuntimeMeta();
    assertEquals(PipeStatus.STOPPED, runtimeMetaAfterRetry.getStatus().get());
    assertTrue(runtimeMetaAfterRetry.getIsStoppedByRuntimeException());
    assertEquals(
        error, runtimeMetaAfterRetry.getNodeId2PipeRuntimeExceptionMap().get(1).getMessage());
    Mockito.verify(env, Mockito.times(1))
        .pushSinglePipeMetaToDataNodes(Mockito.any(ByteBuffer.class), Mockito.any());
    Mockito.verify(consensusManager, Mockito.times(2)).write(Mockito.any(ConfigPhysicalPlan.class));
  }

  private static ConsensusManager mockSuccessfulConsensusWrite(final ConfigNodeProcedureEnv env)
      throws Exception {
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return consensusManager;
  }

  private static void assertPipeMetaChangeWasWritten(
      final ConsensusManager consensusManager, final String pipeName, final String error)
      throws Exception {
    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager).write(planCaptor.capture());
    assertTrue(planCaptor.getValue() instanceof PipeHandleMetaChangePlan);

    final PipeRuntimeMeta persistedRuntimeMeta =
        ((PipeHandleMetaChangePlan) planCaptor.getValue())
            .getPipeMetaList().stream()
                .filter(meta -> pipeName.equals(meta.getStaticMeta().getPipeName()))
                .findFirst()
                .orElseThrow(AssertionError::new)
                .getRuntimeMeta();
    assertEquals(PipeStatus.STOPPED, persistedRuntimeMeta.getStatus().get());
    assertTrue(persistedRuntimeMeta.getIsStoppedByRuntimeException());
    assertEquals(
        error, persistedRuntimeMeta.getNodeId2PipeRuntimeExceptionMap().get(1).getMessage());
  }

  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    StartPipeProcedureV2 proc = new StartPipeProcedureV2("testPipe", true);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      StartPipeProcedureV2 proc2 =
          (StartPipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void serializeDeserializeLegacyFormatTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    StartPipeProcedureV2 proc = new StartPipeProcedureV2("testPipe");

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      StartPipeProcedureV2 proc2 =
          (StartPipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  private static class TestableStartPipeProcedureV2 extends StartPipeProcedureV2 {
    private TestableStartPipeProcedureV2(final String pipeName, final PipeTaskInfo taskInfo)
        throws Exception {
      super(pipeName);
      pipeTaskInfo = new AtomicReference<>(taskInfo);
    }
  }
}
