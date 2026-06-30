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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.PipeRuntimeCoordinator;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinator;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
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
    final ExecutorService procedureSubmitter = Mockito.mock(ExecutorService.class);

    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    when(configManager.getPipeManager()).thenReturn(pipeManager);
    when(nodeManager.getRegisteredDataNodeCount()).thenReturn(registeredDataNodeCount);
    when(pipeManager.getPipeRuntimeCoordinator()).thenReturn(pipeRuntimeCoordinator);
    when(pipeManager.getPipeTaskCoordinator()).thenReturn(pipeTaskCoordinator);
    when(pipeRuntimeCoordinator.getProcedureSubmitter()).thenReturn(procedureSubmitter);
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

  private PipeHeartbeat createPipeHeartbeat(final PipeMeta pipeMeta, final boolean isDegraded)
      throws Exception {
    return new PipeHeartbeat(
        Collections.singletonList(pipeMeta.serialize()),
        Collections.singletonList(false),
        Collections.singletonList(0L),
        Collections.singletonList(0d),
        Collections.singletonList(PipeTemporaryMeta.encodeTsFileEpochDegradedStatus(isDegraded)));
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
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();
    pipeRuntimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    return new PipeMeta(
        new PipeStaticMeta("test_pipe", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
        pipeRuntimeMeta);
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
