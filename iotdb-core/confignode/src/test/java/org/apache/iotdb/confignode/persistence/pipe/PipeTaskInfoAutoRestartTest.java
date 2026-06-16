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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeTaskInfoAutoRestartTest {

  private static final int DATA_NODE_ID = 1;

  private PipeTaskInfo pipeTaskInfo;
  private long creationTime;

  private static boolean trySetPushPipeMetaRespExceptionMessageCreationTime(
      final TPushPipeMetaRespExceptionMessage exceptionMessage, final long creationTime) {
    try {
      exceptionMessage
          .getClass()
          .getMethod("setCreationTime", long.class)
          .invoke(exceptionMessage, creationTime);
      return true;
    } catch (final Exception ignored) {
      return false;
    }
  }

  private static OptionalLong tryGetPushPipeMetaRespExceptionMessageCreationTime(
      final TPushPipeMetaRespExceptionMessage exceptionMessage) {
    try {
      if (!Boolean.TRUE.equals(
          exceptionMessage.getClass().getMethod("isSetCreationTime").invoke(exceptionMessage))) {
        return OptionalLong.empty();
      }
      return OptionalLong.of(
          (long) exceptionMessage.getClass().getMethod("getCreationTime").invoke(exceptionMessage));
    } catch (final Exception ignored) {
      return OptionalLong.empty();
    }
  }

  @Before
  public void setUp() {
    pipeTaskInfo = new PipeTaskInfo();
    creationTime = System.currentTimeMillis();
  }

  @Test
  public void testRecordDataNodePushPipeMetaExceptionsMarksRunningPipeForAutoRestart() {
    final String pipeName = "runningPipe";
    createPipe(pipeName, PipeStatus.RUNNING);

    Assert.assertTrue(
        pipeTaskInfo.recordDataNodePushPipeMetaExceptions(createErrorRespMap(pipeName)));

    final PipeRuntimeMeta runtimeMeta =
        pipeTaskInfo.getPipeMetaByPipeName(pipeName).getRuntimeMeta();
    Assert.assertEquals(PipeStatus.STOPPED, runtimeMeta.getStatus().get());
    Assert.assertTrue(runtimeMeta.getIsStoppedByRuntimeException());

    Assert.assertTrue(pipeTaskInfo.autoRestart());
    Assert.assertEquals(PipeStatus.RUNNING, runtimeMeta.getStatus().get());
  }

  @Test
  public void testRecordDataNodePushPipeMetaExceptionsKeepsUserStoppedPipeOutOfAutoRestart() {
    final String pipeName = "stoppedPipe";
    createPipe(pipeName, PipeStatus.STOPPED);

    Assert.assertTrue(pipeTaskInfo.isPipeStoppedByUser(pipeName));
    Assert.assertTrue(
        pipeTaskInfo.recordDataNodePushPipeMetaExceptions(createErrorRespMap(pipeName)));

    final PipeRuntimeMeta runtimeMeta =
        pipeTaskInfo.getPipeMetaByPipeName(pipeName).getRuntimeMeta();
    Assert.assertEquals(PipeStatus.STOPPED, runtimeMeta.getStatus().get());
    Assert.assertFalse(runtimeMeta.getIsStoppedByRuntimeException());
    Assert.assertTrue(pipeTaskInfo.isPipeStoppedByUser(pipeName));

    Assert.assertFalse(pipeTaskInfo.autoRestart());
    Assert.assertEquals(PipeStatus.STOPPED, runtimeMeta.getStatus().get());
  }

  @Test
  public void testRecordDataNodePushPipeMetaExceptionsTargetsSameNamePipeByCreationTime() {
    final String pipeName = "sameNamePipe";
    final PipeStaticMeta treePipeStaticMeta = createPipe(pipeName, PipeStatus.RUNNING, false);
    final PipeStaticMeta tablePipeStaticMeta = createPipe(pipeName, PipeStatus.RUNNING, true);

    final TPushPipeMetaRespExceptionMessage probe =
        new TPushPipeMetaRespExceptionMessage(pipeName, "probe", System.currentTimeMillis());
    trySetPushPipeMetaRespExceptionMessageCreationTime(
        probe, tablePipeStaticMeta.getCreationTime());
    org.junit.Assume.assumeTrue(
        tryGetPushPipeMetaRespExceptionMessageCreationTime(probe).isPresent());

    Assert.assertTrue(
        pipeTaskInfo.recordDataNodePushPipeMetaExceptions(
            createErrorRespMap(pipeName, tablePipeStaticMeta.getCreationTime())));

    final PipeRuntimeMeta treeRuntimeMeta =
        pipeTaskInfo.getPipeMetaByPipeName(pipeName, false).getRuntimeMeta();
    final PipeRuntimeMeta tableRuntimeMeta =
        pipeTaskInfo.getPipeMetaByPipeName(pipeName, true).getRuntimeMeta();

    Assert.assertEquals(PipeStatus.RUNNING, treeRuntimeMeta.getStatus().get());
    Assert.assertFalse(treeRuntimeMeta.getIsStoppedByRuntimeException());
    Assert.assertEquals(PipeStatus.STOPPED, tableRuntimeMeta.getStatus().get());
    Assert.assertTrue(tableRuntimeMeta.getIsStoppedByRuntimeException());
    Assert.assertTrue(
        tableRuntimeMeta.getNodeId2PipeRuntimeExceptionMap().containsKey(DATA_NODE_ID));

    Assert.assertNotEquals(
        treePipeStaticMeta.getCreationTime(), tablePipeStaticMeta.getCreationTime());
  }

  private Map<Integer, TPushPipeMetaResp> createErrorRespMap(final String pipeName) {
    return createErrorRespMap(pipeName, null);
  }

  private Map<Integer, TPushPipeMetaResp> createErrorRespMap(
      final String pipeName, final Long creationTime) {
    final TPushPipeMetaRespExceptionMessage exceptionMessage =
        new TPushPipeMetaRespExceptionMessage(
            pipeName, "failed to push pipe meta", System.currentTimeMillis());
    if (creationTime != null) {
      trySetPushPipeMetaRespExceptionMessageCreationTime(exceptionMessage, creationTime);
    }
    final TPushPipeMetaResp resp =
        new TPushPipeMetaResp()
            .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
            .setExceptionMessages(Collections.singletonList(exceptionMessage));
    return Collections.singletonMap(DATA_NODE_ID, resp);
  }

  private PipeStaticMeta createPipe(final String pipeName, final PipeStatus initialStatus) {
    return createPipe(pipeName, initialStatus, false);
  }

  private PipeStaticMeta createPipe(
      final String pipeName, final PipeStatus initialStatus, final boolean isTableModel) {
    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();
    extractorAttributes.put("extractor", "iotdb-source");
    if (isTableModel) {
      extractorAttributes.put(
          SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    }
    processorAttributes.put("processor", "do-nothing-processor");
    connectorAttributes.put("connector", "iotdb-thrift-sink");

    final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, DATA_NODE_ID);
    final ConcurrentMap<Integer, PipeTaskMeta> pipeTasks = new ConcurrentHashMap<>();
    pipeTasks.put(DATA_NODE_ID, pipeTaskMeta);

    final PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            pipeName,
            ++creationTime,
            extractorAttributes,
            processorAttributes,
            connectorAttributes);
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    pipeTaskInfo.createPipe(new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta));

    if (PipeStatus.RUNNING.equals(initialStatus)) {
      pipeTaskInfo.setPipeStatus(
          new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING, isTableModel));
    }
    return pipeStaticMeta;
  }
}
