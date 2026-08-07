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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class DropPipeProcedureV2Test {

  private static class TestDropPipeProcedureV2 extends DropPipeProcedureV2 {

    private TestDropPipeProcedureV2() {
      super();
    }

    private TestDropPipeProcedureV2(final String pipeName) throws PipeException {
      super(pipeName);
    }

    private void setPipeTaskInfo(final PipeTaskInfo pipeTaskInfo) {
      this.pipeTaskInfo = new AtomicReference<>(pipeTaskInfo);
    }
  }

  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    DropPipeProcedureV2 proc = new DropPipeProcedureV2("testPipe");

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      DropPipeProcedureV2 proc2 =
          (DropPipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);

      final ByteBuffer legacyBuffer =
          ByteBuffer.wrap(
              byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size() - Integer.BYTES - 1);
      final DropPipeProcedureV2 legacyProc =
          (DropPipeProcedureV2) ProcedureFactory.getInstance().create(legacyBuffer);
      assertEquals(proc, legacyProc);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testWriteConsensusMarksPreDelete() throws Exception {
    final String pipeName = "testPipe";
    final PipeTaskInfo pipeTaskInfo = createPipeTaskInfo(pipeName);
    final TestDropPipeProcedureV2 procedure = new TestDropPipeProcedureV2(pipeName);
    procedure.setPipeTaskInfo(pipeTaskInfo);
    procedure.executeFromCalculateInfoForTask(Mockito.mock(ConfigNodeProcedureEnv.class));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    procedure.executeFromWriteConfigNodeConsensus(env);

    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager).write(planCaptor.capture());
    assertEquals(new SetPipeStatusPlanV2(pipeName, PipeStatus.PRE_DELETE), planCaptor.getValue());
  }

  @Test
  public void testDataNodeStagePushesDroppedMetaBeforeFinalDrop() throws Exception {
    final String pipeName = "testPipe";
    final PipeTaskInfo pipeTaskInfo = createPipeTaskInfo(pipeName);
    final TestDropPipeProcedureV2 procedure = new TestDropPipeProcedureV2(pipeName);
    procedure.setPipeTaskInfo(pipeTaskInfo);
    procedure.executeFromCalculateInfoForTask(Mockito.mock(ConfigNodeProcedureEnv.class));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(env.pushSinglePipeMetaToDataNodes(Mockito.any(ByteBuffer.class), Mockito.any()))
        .thenReturn(Collections.emptyMap());
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    procedure.executeFromOperateOnDataNodes(env);

    final ArgumentCaptor<ByteBuffer> metaCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    Mockito.verify(env).pushSinglePipeMetaToDataNodes(metaCaptor.capture(), Mockito.any());
    final PipeMeta droppedPipeMeta = PipeMeta.deserialize4TaskAgent(metaCaptor.getValue());
    assertEquals(PipeStatus.DROPPED, droppedPipeMeta.getRuntimeMeta().getStatus().get());

    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager).write(planCaptor.capture());
    assertEquals(new DropPipePlanV2(pipeName), planCaptor.getValue());
  }

  @Test
  public void testPipeMetaToDropSurvivesSerialization() throws Exception {
    final String pipeName = "testPipe";
    final TestDropPipeProcedureV2 procedure = new TestDropPipeProcedureV2(pipeName);
    procedure.setPipeTaskInfo(createPipeTaskInfo(pipeName));
    procedure.executeFromCalculateInfoForTask(Mockito.mock(ConfigNodeProcedureEnv.class));

    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    procedure.serialize(new DataOutputStream(byteArrayOutputStream));
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    final DropPipeProcedureV2 recoveredProcedure =
        (DropPipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

    assertNotNull(recoveredProcedure.getPipeMetaToDrop());
    assertEquals(pipeName, recoveredProcedure.getPipeMetaToDrop().getStaticMeta().getPipeName());
  }

  private PipeTaskInfo createPipeTaskInfo(final String pipeName) {
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(
            new PipeStaticMeta(
                pipeName,
                System.currentTimeMillis(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()),
            new PipeRuntimeMeta()));
    return pipeTaskInfo;
  }
}
