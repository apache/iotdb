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

package org.apache.iotdb.confignode.procedure.impl.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PipeMetaSyncProcedureTest {

  @Test
  public void serializeDeserializeTest() {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    final PipeMetaSyncProcedure proc1 = new PipeMetaSyncProcedure();

    try {
      proc1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      final PipeMetaSyncProcedure proc2 =
          (PipeMetaSyncProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc1, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testDataNodePushFailureIsPersistedToConsensus() throws Exception {
    final String pipeName = "metaSyncFailurePipe";
    final long creationTime = System.currentTimeMillis();
    final PipeStaticMeta staticMeta =
        new PipeStaticMeta(
            pipeName,
            creationTime,
            Collections.singletonMap("extractor", "iotdb-source"),
            Collections.singletonMap("processor", "do-nothing-processor"),
            Collections.singletonMap("connector", "write-back-sink"));
    final PipeRuntimeMeta runtimeMeta = new PipeRuntimeMeta(new ConcurrentHashMap<>());
    runtimeMeta.getStatus().set(PipeStatus.RUNNING);
    runtimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    final PipeTaskInfo taskInfo = new PipeTaskInfo();
    taskInfo.createPipe(new CreatePipePlanV2(staticMeta, runtimeMeta));

    final String error = "Failed to construct PipeConnector";
    final TPushPipeMetaRespExceptionMessage exceptionMessage =
        new TPushPipeMetaRespExceptionMessage(pipeName, error, creationTime)
            .setCreationTime(creationTime);
    final Map<Integer, TPushPipeMetaResp> responses =
        Collections.singletonMap(
            1,
            new TPushPipeMetaResp(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
                .setExceptionMessages(Collections.singletonList(exceptionMessage)));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    final PipeMetaSyncProcedure procedure = new TestablePipeMetaSyncProcedure(taskInfo, responses);
    try {
      procedure.executeFromOperateOnDataNodes(env);
      fail("Expected the sync procedure to report the DataNode push failure");
    } catch (final PipeException expected) {
      assertTrue(expected.getMessage().contains(error));
    }

    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager).write(planCaptor.capture());
    assertTrue(planCaptor.getValue() instanceof PipeHandleMetaChangePlan);
    final PipeMeta persistedPipeMeta =
        ((PipeHandleMetaChangePlan) planCaptor.getValue())
            .getPipeMetaList().stream()
                .filter(meta -> pipeName.equals(meta.getStaticMeta().getPipeName()))
                .findFirst()
                .orElseThrow(AssertionError::new);
    assertEquals(PipeStatus.STOPPED, persistedPipeMeta.getRuntimeMeta().getStatus().get());
    assertTrue(persistedPipeMeta.getRuntimeMeta().getIsStoppedByRuntimeException());
    assertEquals(
        error,
        persistedPipeMeta.getRuntimeMeta().getNodeId2PipeRuntimeExceptionMap().get(1).getMessage());
  }

  @Test
  public void testUnscopedPushFailureStopsAndPersistsAllPipes() throws Exception {
    final PipeTaskInfo taskInfo = new PipeTaskInfo();
    final PipeStaticMeta firstPipe = createRunningPipe(taskInfo, "firstUnscopedPipe");
    final PipeStaticMeta secondPipe = createRunningPipe(taskInfo, "secondUnscopedPipe");
    final String statusMessage = "DataNode failed before returning pipe-specific details";
    final Map<Integer, TPushPipeMetaResp> responses =
        Collections.singletonMap(
            1,
            new TPushPipeMetaResp(
                new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode())
                    .setMessage(statusMessage)));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    final PipeMetaSyncProcedure procedure = new TestablePipeMetaSyncProcedure(taskInfo, responses);
    try {
      procedure.executeFromOperateOnDataNodes(env);
      fail("Expected the sync procedure to report the DataNode push failure");
    } catch (final PipeException expected) {
      assertTrue(expected.getMessage().contains(statusMessage));
    }

    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager).write(planCaptor.capture());
    assertTrue(planCaptor.getValue() instanceof PipeHandleMetaChangePlan);
    final PipeHandleMetaChangePlan persistedPlan =
        (PipeHandleMetaChangePlan)
            ConfigPhysicalPlan.Factory.create(planCaptor.getValue().serializeToByteBuffer());
    final Map<String, PipeMeta> persistedPipes =
        persistedPlan.getPipeMetaList().stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    meta -> meta.getStaticMeta().getPipeName(), meta -> meta));
    assertEquals(2, persistedPipes.size());
    for (final PipeStaticMeta pipeStaticMeta : new PipeStaticMeta[] {firstPipe, secondPipe}) {
      final PipeRuntimeMeta persistedRuntimeMeta =
          persistedPipes.get(pipeStaticMeta.getPipeName()).getRuntimeMeta();
      assertEquals(PipeStatus.STOPPED, persistedRuntimeMeta.getStatus().get());
      assertTrue(persistedRuntimeMeta.getIsStoppedByRuntimeException());
      assertTrue(
          persistedRuntimeMeta
              .getNodeId2PipeRuntimeExceptionMap()
              .get(1)
              .getMessage()
              .contains(statusMessage));
    }
    assertTrue(
        taskInfo
            .getPipeMetaByPipeName(firstPipe.getPipeName())
            .getRuntimeMeta()
            .getNodeId2PipeRuntimeExceptionMap()
            .containsKey(1));
  }

  @Test
  public void testRetriesPersistenceWhenPushSucceedsAfterConsensusFailure() throws Exception {
    final PipeTaskInfo taskInfo = new PipeTaskInfo();
    final PipeStaticMeta pipeStaticMeta = createRunningPipe(taskInfo, "metaSyncRetryPipe");
    final String error = "DataNode failed before returning pipe-specific details";
    final Map<Integer, TPushPipeMetaResp> responses = new HashMap<>();
    responses.put(
        1,
        new TPushPipeMetaResp(
            new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()).setMessage(error)));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    final PipeMetaSyncProcedure procedure = new TestablePipeMetaSyncProcedure(taskInfo, responses);
    try {
      procedure.executeFromOperateOnDataNodes(env);
      fail("Expected the first attempt to fail its ConfigNode consensus write");
    } catch (final PipeException expected) {
      // The procedure retries this state after the persistence failure.
    }

    responses.put(
        1, new TPushPipeMetaResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    procedure.executeFromOperateOnDataNodes(env);

    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager, Mockito.times(2)).write(planCaptor.capture());
    assertTrue(planCaptor.getAllValues().get(1) instanceof PipeHandleMetaChangePlan);
    final PipeMeta persistedPipeMeta =
        ((PipeHandleMetaChangePlan) planCaptor.getAllValues().get(1))
            .getPipeMetaList().stream()
                .filter(
                    meta -> pipeStaticMeta.getPipeName().equals(meta.getStaticMeta().getPipeName()))
                .findFirst()
                .orElseThrow(AssertionError::new);
    assertEquals(PipeStatus.STOPPED, persistedPipeMeta.getRuntimeMeta().getStatus().get());
    assertTrue(persistedPipeMeta.getRuntimeMeta().getIsStoppedByRuntimeException());
    assertTrue(
        persistedPipeMeta
            .getRuntimeMeta()
            .getNodeId2PipeRuntimeExceptionMap()
            .get(1)
            .getMessage()
            .contains(error));
  }

  private static PipeStaticMeta createRunningPipe(
      final PipeTaskInfo taskInfo, final String pipeName) {
    final long creationTime = System.nanoTime();
    final PipeStaticMeta staticMeta =
        new PipeStaticMeta(
            pipeName,
            creationTime,
            Collections.singletonMap("extractor", "iotdb-source"),
            Collections.singletonMap("processor", "do-nothing-processor"),
            Collections.singletonMap("connector", "write-back-sink"));
    final PipeRuntimeMeta runtimeMeta = new PipeRuntimeMeta(new ConcurrentHashMap<>());
    runtimeMeta.getStatus().set(PipeStatus.RUNNING);
    runtimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    taskInfo.createPipe(new CreatePipePlanV2(staticMeta, runtimeMeta));
    return staticMeta;
  }

  private static class TestablePipeMetaSyncProcedure extends PipeMetaSyncProcedure {
    private final Map<Integer, TPushPipeMetaResp> responses;

    private TestablePipeMetaSyncProcedure(
        final PipeTaskInfo taskInfo, final Map<Integer, TPushPipeMetaResp> responses) {
      this.pipeTaskInfo = new AtomicReference<>(taskInfo);
      this.responses = responses;
    }

    @Override
    protected Map<Integer, TPushPipeMetaResp> pushPipeMetaToDataNodesBestEffortAndGetResponse(
        final ConfigNodeProcedureEnv env) throws IOException {
      return responses;
    }
  }
}
