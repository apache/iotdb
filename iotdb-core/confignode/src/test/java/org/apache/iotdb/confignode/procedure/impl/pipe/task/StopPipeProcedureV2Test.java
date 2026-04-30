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
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusWithStoppedByRuntimeExceptionPlanV2;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StopPipeProcedureV2Test {

  private static class TestStopPipeProcedureV2 extends StopPipeProcedureV2 {

    private TestStopPipeProcedureV2(final String pipeName) throws PipeException {
      super(pipeName);
    }

    private void setPipeTaskInfo(final PipeTaskInfo pipeTaskInfo) {
      this.pipeTaskInfo = new AtomicReference<>(pipeTaskInfo);
    }
  }

  private static PipeTaskInfo createExceptionStoppedPipeTaskInfo(final String pipeName) {
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();

    final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    final ConcurrentMap<Integer, PipeTaskMeta> pipeTasks = new ConcurrentHashMap<>();
    pipeTasks.put(1, pipeTaskMeta);

    final PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            pipeName,
            System.currentTimeMillis(),
            Collections.singletonMap("extractor", "iotdb-source"),
            Collections.singletonMap("processor", "do-nothing-processor"),
            Collections.singletonMap("connector", "iotdb-thrift-connector"));
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    pipeRuntimeMeta.getStatus().set(PipeStatus.STOPPED);
    pipeRuntimeMeta.setIsStoppedByRuntimeException(true);

    pipeTaskInfo.createPipe(new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta));
    return pipeTaskInfo;
  }

  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    StopPipeProcedureV2 proc = new StopPipeProcedureV2("testPipe");

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      StopPipeProcedureV2 proc2 =
          (StopPipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testStopPipeWritesStatusAndRuntimeExceptionFlagAtomically() throws Exception {
    final String pipeName = "testPipe";
    final TestStopPipeProcedureV2 proc = new TestStopPipeProcedureV2(pipeName);
    proc.setPipeTaskInfo(createExceptionStoppedPipeTaskInfo(pipeName));
    proc.executeFromCalculateInfoForTask(Mockito.mock(ConfigNodeProcedureEnv.class));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    proc.executeFromWriteConfigNodeConsensus(env);
    proc.rollbackFromWriteConfigNodeConsensus(env);

    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager, Mockito.times(2)).write(planCaptor.capture());

    assertEquals(
        new SetPipeStatusWithStoppedByRuntimeExceptionPlanV2(
            pipeName, PipeStatus.STOPPED, false),
        planCaptor.getAllValues().get(0));
    assertEquals(
        new SetPipeStatusWithStoppedByRuntimeExceptionPlanV2(pipeName, PipeStatus.RUNNING, true),
        planCaptor.getAllValues().get(1));
  }
}
