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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TPipeCompletedDataRegion;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.PipeRuntimeCoordinator;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinator;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeHeartbeatParserTest {

  private static final int DATA_NODE_ID = 1;

  private boolean originalSeparatedPipeHeartbeatEnabled;

  @Before
  public void setUp() {
    originalSeparatedPipeHeartbeatEnabled =
        CommonDescriptor.getInstance().getConfig().isSeperatedPipeHeartbeatEnabled();
  }

  @After
  public void tearDown() {
    CommonDescriptor.getInstance()
        .getConfig()
        .setSeperatedPipeHeartbeatEnabled(originalSeparatedPipeHeartbeatEnabled);
  }

  @Test
  public void testParseHeartbeatCountsOnlyDataNodesWhenSeparatedHeartbeatDisabled()
      throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final ParserTestContext context = createParserTestContext(2);
    setMetaChangeFlags(context.parser, true, false);

    context.parser.parseHeartbeat(1, emptyHeartbeat());
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());

    context.parser.parseHeartbeat(2, emptyHeartbeat());
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(true, false);
  }

  @Test
  public void testParseHeartbeatCountsLocalConfigNodeWhenSeparatedHeartbeatEnabled()
      throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(true);

    final ParserTestContext context = createParserTestContext(2);
    setMetaChangeFlags(context.parser, true, false);

    context.parser.parseHeartbeat(1, emptyHeartbeat());
    context.parser.parseHeartbeat(2, emptyHeartbeat());
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());

    context.parser.parseHeartbeat(3, emptyHeartbeat());
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(true, false);
  }

  @Test
  public void testParseHeartbeatKeepsPendingFlagsWhenProcedureSubmissionFails() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final ParserTestContext context = createParserTestContext(2);
    when(context.procedureManager.pipeHandleMetaChange(anyBoolean(), anyBoolean()))
        .thenReturn(false, true);
    setMetaChangeFlags(context.parser, true, false);

    context.parser.parseHeartbeat(1, emptyHeartbeat());
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());

    context.parser.parseHeartbeat(2, emptyHeartbeat());
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(true, false);

    context.parser.parseHeartbeat(3, emptyHeartbeat());
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(true, false);

    context.parser.parseHeartbeat(4, emptyHeartbeat());
    verify(context.procedureManager, times(2)).pipeHandleMetaChange(true, false);
  }

  @Test
  public void testParseHeartbeatIgnoresExceptionsBeforeClearTime() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final String pipeName = "staleExceptionPipe";
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    createPipe(pipeTaskInfo, pipeName, PipeStatus.RUNNING);

    final PipeMeta pipeMeta = pipeTaskInfo.getPipeMetaByPipeName(pipeName);
    final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
    final PipeTaskMeta coordinatorTaskMeta =
        runtimeMeta.getConsensusGroupId2TaskMetaMap().get(DATA_NODE_ID);
    coordinatorTaskMeta.trackExceptionMessage(
        new PipeRuntimeCriticalException("stale failure", 100L));

    pipeTaskInfo.clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(pipeName, 200L);

    final PipeTaskMeta agentTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, DATA_NODE_ID);
    agentTaskMeta.trackExceptionMessage(new PipeRuntimeCriticalException("stale failure", 100L));
    final ConcurrentMap<Integer, PipeTaskMeta> agentPipeTasks = new ConcurrentHashMap<>();
    agentPipeTasks.put(DATA_NODE_ID, agentTaskMeta);
    final PipeHeartbeat heartbeat =
        new PipeHeartbeat(
            Collections.singletonList(
                new PipeMeta(pipeMeta.getStaticMeta(), new PipeRuntimeMeta(agentPipeTasks))
                    .serialize()),
            Collections.singletonList(false),
            Collections.singletonList(0L),
            Collections.singletonList(0D),
            null);

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(DATA_NODE_ID, heartbeat);

    Assert.assertFalse(coordinatorTaskMeta.hasExceptionMessages());
    Assert.assertEquals(PipeStatus.RUNNING, runtimeMeta.getStatus().get());
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(false, true);
  }

  @Test
  public void testParseHeartbeatTracksExceptionsAfterClearTime() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final String pipeName = "freshExceptionPipe";
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    createPipe(pipeTaskInfo, pipeName, PipeStatus.RUNNING);

    final PipeMeta pipeMeta = pipeTaskInfo.getPipeMetaByPipeName(pipeName);
    final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
    final PipeTaskMeta coordinatorTaskMeta =
        runtimeMeta.getConsensusGroupId2TaskMetaMap().get(DATA_NODE_ID);
    pipeTaskInfo.clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(pipeName, 200L);

    final PipeTaskMeta agentTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, DATA_NODE_ID);
    agentTaskMeta.trackExceptionMessage(new PipeRuntimeCriticalException("fresh failure", 300L));
    final ConcurrentMap<Integer, PipeTaskMeta> agentPipeTasks = new ConcurrentHashMap<>();
    agentPipeTasks.put(DATA_NODE_ID, agentTaskMeta);
    final PipeHeartbeat heartbeat =
        new PipeHeartbeat(
            Collections.singletonList(
                new PipeMeta(pipeMeta.getStaticMeta(), new PipeRuntimeMeta(agentPipeTasks))
                    .serialize()),
            Collections.singletonList(false),
            Collections.singletonList(0L),
            Collections.singletonList(0D),
            null);

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(DATA_NODE_ID, heartbeat);

    Assert.assertTrue(coordinatorTaskMeta.hasExceptionMessages());
    Assert.assertEquals(PipeStatus.STOPPED, runtimeMeta.getStatus().get());
    Assert.assertTrue(runtimeMeta.getIsStoppedByRuntimeException());
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(true, false);
  }

  @Test
  public void testParseHeartbeatDoesNotOverwritePreDeleteStatus() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final String pipeName = "preDeletePipe";
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    createPipe(pipeTaskInfo, pipeName, PipeStatus.RUNNING);

    final PipeMeta pipeMeta = pipeTaskInfo.getPipeMetaByPipeName(pipeName);
    final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
    runtimeMeta.getStatus().set(PipeStatus.PRE_DELETE);

    final PipeTaskMeta agentTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, DATA_NODE_ID);
    agentTaskMeta.trackExceptionMessage(new PipeRuntimeCriticalException("fresh failure", 300L));
    final ConcurrentMap<Integer, PipeTaskMeta> agentPipeTasks = new ConcurrentHashMap<>();
    agentPipeTasks.put(DATA_NODE_ID, agentTaskMeta);
    final PipeHeartbeat heartbeat =
        new PipeHeartbeat(
            Collections.singletonList(
                new PipeMeta(pipeMeta.getStaticMeta(), new PipeRuntimeMeta(agentPipeTasks))
                    .serialize()),
            Collections.singletonList(false),
            Collections.singletonList(0L),
            Collections.singletonList(0D),
            null);

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(DATA_NODE_ID, heartbeat);

    Assert.assertEquals(PipeStatus.PRE_DELETE, runtimeMeta.getStatus().get());
    Assert.assertFalse(
        runtimeMeta.getConsensusGroupId2TaskMetaMap().get(DATA_NODE_ID).hasExceptionMessages());
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testParseHeartbeatRecordsPipeDegradedStatus() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createPipeMeta();
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(
        1,
        new PipeHeartbeat(
            Collections.singletonList(pipeMeta.serialize()),
            Collections.singletonList(false),
            Collections.singletonList(0L),
            Collections.singletonList(0d),
            Collections.singletonList(PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_TRUE)));

    assertEquals(Boolean.TRUE, getTemporaryMeta(pipeTaskInfo).getGlobalDegraded());
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testParseHeartbeatAggregatesPipeDegradedStatusFromAllDataNodes() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createPipeMeta();
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(2, pipeTaskInfo);
    context.parser.parseHeartbeat(1, createPipeHeartbeat(pipeMeta, true));
    assertEquals(Boolean.TRUE, getTemporaryMeta(pipeTaskInfo).getGlobalDegraded());

    context.parser.parseHeartbeat(2, createPipeHeartbeat(pipeMeta, false));
    assertEquals(Boolean.TRUE, getTemporaryMeta(pipeTaskInfo).getGlobalDegraded());

    context.parser.parseHeartbeat(1, createPipeHeartbeat(pipeMeta, false));
    assertEquals(Boolean.FALSE, getTemporaryMeta(pipeTaskInfo).getGlobalDegraded());
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testParseHeartbeatTreatsMissingPipeDegradedStatusAsUnknown() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createPipeMeta();
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(
        1,
        new PipeHeartbeat(
            Collections.singletonList(pipeMeta.serialize()),
            Collections.singletonList(false),
            Collections.singletonList(0L),
            Collections.singletonList(0d),
            null));

    assertNull(getTemporaryMeta(pipeTaskInfo).getGlobalDegraded());
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testParseHeartbeatAggregatesRecentFailuresFromAllDataNodes() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createPipeMeta();
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(2, pipeTaskInfo);
    final Map<String, Long> firstNodeFailures = new HashMap<>();
    firstNodeFailures.put("network_timeout", 10L);
    final Map<String, Long> secondNodeFailures = new HashMap<>();
    secondNodeFailures.put("network_timeout", 2L);
    secondNodeFailures.put("memory_timeout", 15L);

    context.parser.parseHeartbeat(1, createPipeHeartbeat(pipeMeta, false, firstNodeFailures));
    context.parser.parseHeartbeat(2, createPipeHeartbeat(pipeMeta, false, secondNodeFailures));

    Assert.assertEquals(
        Long.valueOf(12),
        getTemporaryMeta(pipeTaskInfo).getGlobalRecentFailures().get("network_timeout"));
    Assert.assertEquals(
        Long.valueOf(15),
        getTemporaryMeta(pipeTaskInfo).getGlobalRecentFailures().get("memory_timeout"));
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testPipeHeartbeatTreatsNullRecentFailureMapAsEmpty() throws Exception {
    final PipeMeta pipeMeta = createPipeMeta();
    final PipeHeartbeat heartbeat =
        new PipeHeartbeat(
            Collections.singletonList(pipeMeta.serialize()),
            Collections.singletonList(false),
            Collections.singletonList(0L),
            Collections.singletonList(0d),
            Collections.singletonList(PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_UNKNOWN),
            Collections.singletonList(null));

    Assert.assertTrue(heartbeat.getRecentFailures(pipeMeta.getStaticMeta()).isEmpty());
  }

  @Test
  public void testParseHeartbeatDoesNotCompleteWhenRequiredDataRegionMissing() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createHistoryOnlyPipeMeta(1);
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(
        1,
        new PipeHeartbeat(
            Collections.singletonList(pipeMeta.serialize()),
            Collections.singletonList(true),
            Collections.singletonList(0L),
            Collections.singletonList(0d),
            null,
            null,
            Collections.singletonList(
                new TPipeCompletedDataRegion(
                    pipeMeta.getStaticMeta().getPipeName(),
                    pipeMeta.getStaticMeta().getCreationTime(),
                    Collections.emptyList()))));

    Assert.assertTrue(getTemporaryMeta(pipeTaskInfo).getCompletedDataRegionIds().isEmpty());
    Assert.assertNotNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testParseHeartbeatCompletesHistoryOnlyPipeWithoutRequiredDataRegion()
      throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.realtime.enable", Boolean.FALSE.toString());
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createPipeMeta(sourceAttributes);
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(
        1, createPipeHeartbeatWithCompletedRegions(pipeMeta, false, Collections.emptyList()));

    Assert.assertNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(true, true);
  }

  @Test
  public void testParseHeartbeatKeepsHistoryOnlyPipeWithoutDataRegionReport() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.realtime.enable", Boolean.FALSE.toString());
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createPipeMeta(sourceAttributes);
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(1, createPipeHeartbeat(pipeMeta, false));

    Assert.assertNotNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testParseHeartbeatKeepsRealtimePipeWithoutRequiredDataRegion() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createPipeMeta(Collections.emptyMap());
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    context.parser.parseHeartbeat(
        1, createPipeHeartbeatWithCompletedRegions(pipeMeta, false, Collections.emptyList()));

    Assert.assertNotNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));
    verify(context.procedureManager, never()).pipeHandleMetaChange(anyBoolean(), anyBoolean());
  }

  @Test
  public void testParseHeartbeatDoesNotTrustDataNodeBooleanForCompletion() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createHistoryOnlyPipeMeta(1);
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(1, pipeTaskInfo);
    // The DataNode's boolean is false, but the required DataRegion is reported complete. The
    // coordinator should still complete the pipe because it no longer trusts the boolean.
    context.parser.parseHeartbeat(
        1, createPipeHeartbeatWithCompletedRegions(pipeMeta, false, Collections.singletonList(1)));

    Assert.assertNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));
  }

  @Test
  public void testParseHeartbeatCompletesOnlyAfterAllRequiredDataRegionsReported()
      throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createHistoryOnlyPipeMeta(1, 2);
    pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().get(2).setLeaderNodeId(2);
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(2, pipeTaskInfo);

    context.parser.parseHeartbeat(
        1, createPipeHeartbeatWithCompletedRegions(pipeMeta, true, Collections.singletonList(1)));
    Assert.assertNotNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));

    context.parser.parseHeartbeat(
        2, createPipeHeartbeatWithCompletedRegions(pipeMeta, true, Collections.singletonList(2)));
    Assert.assertNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));
    // After CN decides the pipe is complete, the next heartbeat round pushes the updated meta so
    // DataNodes will drop their local pipe tasks.
    verify(context.procedureManager, times(1)).pipeHandleMetaChange(true, true);
  }

  @Test
  public void testParseHeartbeatKeepsCompletedDataRegionAfterLeaderChange() throws Exception {
    CommonDescriptor.getInstance().getConfig().setSeperatedPipeHeartbeatEnabled(false);

    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    final PipeMeta pipeMeta = createHistoryOnlyPipeMeta(1, 2);
    pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().get(2).setLeaderNodeId(2);
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));

    final ParserTestContext context = createParserTestContext(2, pipeTaskInfo);

    // The old leader of region 1 reports it complete before the leader changes.
    context.parser.parseHeartbeat(
        1, createPipeHeartbeatWithCompletedRegions(pipeMeta, true, Collections.singletonList(1)));
    Assert.assertNotNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));

    // Region 1's leader moves to node 2, which only reports region 2. Region 1's completion is
    // still valid because its historical data was already transferred by the old leader.
    pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().get(1).setLeaderNodeId(2);
    context.parser.parseHeartbeat(
        2, createPipeHeartbeatWithCompletedRegions(pipeMeta, true, Collections.singletonList(2)));

    Assert.assertNull(pipeTaskInfo.getPipeMetaByPipeName("test_pipe"));
  }

  private ParserTestContext createParserTestContext(final int registeredDataNodeCount) {
    return createParserTestContext(registeredDataNodeCount, new PipeTaskInfo());
  }

  private ParserTestContext createParserTestContext(
      final int registeredDataNodeCount, final PipeTaskInfo pipeTaskInfo) {
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final NodeManager nodeManager = Mockito.mock(NodeManager.class);
    final ProcedureManager procedureManager = Mockito.mock(ProcedureManager.class);
    final PipeManager pipeManager = Mockito.mock(PipeManager.class);
    final PipeRuntimeCoordinator pipeRuntimeCoordinator =
        Mockito.mock(PipeRuntimeCoordinator.class);
    final PipeTaskCoordinator pipeTaskCoordinator = Mockito.mock(PipeTaskCoordinator.class);
    final PartitionManager partitionManager = Mockito.mock(PartitionManager.class);
    final ExecutorService procedureSubmitter = Mockito.mock(ExecutorService.class);

    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    when(configManager.getPipeManager()).thenReturn(pipeManager);
    when(nodeManager.getRegisteredDataNodeCount()).thenReturn(registeredDataNodeCount);
    when(pipeManager.getPipeRuntimeCoordinator()).thenReturn(pipeRuntimeCoordinator);
    when(pipeManager.getPipeTaskCoordinator()).thenReturn(pipeTaskCoordinator);
    when(pipeRuntimeCoordinator.getProcedureSubmitter()).thenReturn(procedureSubmitter);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(partitionManager.isRegionGroupExists(any(TConsensusGroupId.class))).thenReturn(true);
    when(pipeTaskCoordinator.tryLock()).thenReturn(new AtomicReference<>(pipeTaskInfo));
    when(procedureManager.pipeHandleMetaChange(anyBoolean(), anyBoolean())).thenReturn(true);
    Mockito.doAnswer(
            invocation -> {
              ((Runnable) invocation.getArgument(0)).run();
              return CompletableFuture.completedFuture(null);
            })
        .when(procedureSubmitter)
        .submit(any(Runnable.class));

    return new ParserTestContext(new PipeHeartbeatParser(configManager), procedureManager);
  }

  private void createPipe(
      final PipeTaskInfo pipeTaskInfo, final String pipeName, final PipeStatus initialStatus) {
    final Map<String, String> extractorAttributes = new HashMap<>();
    extractorAttributes.put("extractor", "iotdb-source");
    final Map<String, String> processorAttributes = new HashMap<>();
    processorAttributes.put("processor", "do-nothing-processor");
    final Map<String, String> connectorAttributes = new HashMap<>();
    connectorAttributes.put("connector", "iotdb-thrift-sink");

    final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, DATA_NODE_ID);
    final ConcurrentMap<Integer, PipeTaskMeta> pipeTasks = new ConcurrentHashMap<>();
    pipeTasks.put(DATA_NODE_ID, pipeTaskMeta);
    final PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            pipeName,
            System.currentTimeMillis(),
            extractorAttributes,
            processorAttributes,
            connectorAttributes);
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    pipeTaskInfo.createPipe(new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta));

    if (PipeStatus.RUNNING.equals(initialStatus)) {
      pipeTaskInfo
          .getPipeMetaByPipeName(pipeName)
          .getRuntimeMeta()
          .getStatus()
          .set(PipeStatus.RUNNING);
    }
  }

  private PipeHeartbeat createPipeHeartbeat(final PipeMeta pipeMeta, final boolean isDegraded)
      throws Exception {
    return createPipeHeartbeat(pipeMeta, isDegraded, Collections.emptyMap());
  }

  private PipeHeartbeat createPipeHeartbeat(
      final PipeMeta pipeMeta, final boolean isDegraded, final Map<String, Long> recentFailures)
      throws Exception {
    return new PipeHeartbeat(
        Collections.singletonList(pipeMeta.serialize()),
        Collections.singletonList(false),
        Collections.singletonList(0L),
        Collections.singletonList(0d),
        Collections.singletonList(PipeTemporaryMeta.encodeTsFileEpochDegradedStatus(isDegraded)),
        Collections.singletonList(recentFailures));
  }

  private PipeTemporaryMetaInCoordinator getTemporaryMeta(final PipeTaskInfo pipeTaskInfo) {
    return (PipeTemporaryMetaInCoordinator)
        pipeTaskInfo.getPipeMetaByPipeName("test_pipe").getTemporaryMeta();
  }

  private void setMetaChangeFlags(
      final PipeHeartbeatParser parser,
      final boolean needWriteConsensusOnConfigNodes,
      final boolean needPushPipeMetaToDataNodes)
      throws Exception {
    setAtomicBooleanField(
        parser, "needWriteConsensusOnConfigNodes", needWriteConsensusOnConfigNodes);
    setAtomicBooleanField(parser, "needPushPipeMetaToDataNodes", needPushPipeMetaToDataNodes);
  }

  private void setAtomicBooleanField(
      final PipeHeartbeatParser parser, final String fieldName, final boolean value)
      throws Exception {
    final Field field = PipeHeartbeatParser.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    ((AtomicBoolean) field.get(parser)).set(value);
  }

  private PipeMeta createPipeMeta() {
    return createPipeMeta(1);
  }

  private PipeMeta createPipeMeta(final int... regionIds) {
    return createPipeMeta(Collections.emptyMap(), regionIds);
  }

  private PipeMeta createHistoryOnlyPipeMeta(final int... regionIds) {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.realtime.enable", Boolean.FALSE.toString());
    return createPipeMeta(sourceAttributes, regionIds);
  }

  private PipeMeta createPipeMeta(
      final Map<String, String> sourceAttributes, final int... regionIds) {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();
    for (final int regionId : regionIds) {
      pipeRuntimeMeta
          .getConsensusGroupId2TaskMetaMap()
          .put(regionId, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    }
    return new PipeMeta(
        new PipeStaticMeta("test_pipe", 1L, sourceAttributes, new HashMap<>(), new HashMap<>()),
        pipeRuntimeMeta);
  }

  private PipeHeartbeat createPipeHeartbeatWithCompletedRegions(
      final PipeMeta pipeMeta, final boolean isCompleted, final List<Integer> completedRegionIds)
      throws Exception {
    return new PipeHeartbeat(
        Collections.singletonList(pipeMeta.serialize()),
        Collections.singletonList(isCompleted),
        Collections.singletonList(0L),
        Collections.singletonList(0d),
        null,
        null,
        Collections.singletonList(
            new TPipeCompletedDataRegion(
                pipeMeta.getStaticMeta().getPipeName(),
                pipeMeta.getStaticMeta().getCreationTime(),
                completedRegionIds)));
  }

  private PipeHeartbeat emptyHeartbeat() {
    return new PipeHeartbeat(Collections.emptyList(), null, null, null, null);
  }

  private static class ParserTestContext {
    private final PipeHeartbeatParser parser;
    private final ProcedureManager procedureManager;

    private ParserTestContext(
        final PipeHeartbeatParser parser, final ProcedureManager procedureManager) {
      this.parser = parser;
      this.procedureManager = procedureManager;
    }
  }
}
