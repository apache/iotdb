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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeTaskInfoAutoRestartTest {

  private static final int DATA_NODE_ID = 1;

  private PipeTaskInfo pipeTaskInfo;
  private long creationTime;

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

  @Test
  public void testEnrichOldUserPipeWithRootUserForCompatibility() {
    final String rootUserName = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
    final String rootPassword = "root-current-password";
    pipeTaskInfo = new PipeTaskInfo(username -> rootPassword);

    createPipe("oldPipe", PipeStatus.STOPPED);

    final Map<String, String> sourceAttributes =
        pipeTaskInfo
            .getPipeMetaByPipeName("oldPipe")
            .getStaticMeta()
            .getSourceParameters()
            .getAttribute();
    Assert.assertEquals(
        String.valueOf(IoTDBConstant.SUPER_USER_ID),
        sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_USER_ID));
    Assert.assertEquals(
        rootUserName, sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY));
    Assert.assertEquals(
        rootPassword, sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY));
    Assert.assertFalse(
        pipeTaskInfo
            .getPipeMetaByPipeName("oldPipe")
            .getStaticMeta()
            .getSinkParameters()
            .getAttribute()
            .containsKey(PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY));
  }

  @Test
  public void testDoNotOverwritePipeWithUserForCompatibility() {
    pipeTaskInfo = new PipeTaskInfo(username -> "root-current-password");

    createPipeWithSourceAttributes(
        "newPipe",
        new HashMap<String, String>() {
          {
            put("extractor", "iotdb-source");
            put(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY, "user");
            put(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY, "user-password");
          }
        });

    final Map<String, String> sourceAttributes =
        pipeTaskInfo
            .getPipeMetaByPipeName("newPipe")
            .getStaticMeta()
            .getSourceParameters()
            .getAttribute();
    Assert.assertEquals("user", sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY));
    Assert.assertEquals(
        "user-password", sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY));
  }

  @Test
  public void testDoNotEnrichSystemPipeForCompatibility() {
    pipeTaskInfo = new PipeTaskInfo(username -> "root-current-password");

    createPipeWithSourceAttributes(
        PipeStaticMeta.generateSubscriptionPipeName("topic", "group"),
        new HashMap<String, String>() {
          {
            put("extractor", "iotdb-source");
          }
        });

    final Map<String, String> sourceAttributes =
        pipeTaskInfo
            .getPipeMetaByPipeName(PipeStaticMeta.generateSubscriptionPipeName("topic", "group"))
            .getStaticMeta()
            .getSourceParameters()
            .getAttribute();
    Assert.assertFalse(sourceAttributes.containsKey(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY));
    Assert.assertFalse(sourceAttributes.containsKey(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY));
  }

  @Test
  public void testEnrichOldWriteBackSinkWithRootUserForCompatibility() {
    final String rootUserName = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
    final String rootPassword = "root-current-password";
    pipeTaskInfo = new PipeTaskInfo(username -> rootPassword);

    createPipeWithAttributes(
        "oldWriteBackPipe",
        new HashMap<String, String>() {
          {
            put("extractor", "iotdb-source");
            put(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY, "source-user");
            put(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY, "source-password");
          }
        },
        new HashMap<String, String>() {
          {
            put("connector", "write-back-sink");
          }
        });

    final Map<String, String> sinkAttributes =
        pipeTaskInfo
            .getPipeMetaByPipeName("oldWriteBackPipe")
            .getStaticMeta()
            .getSinkParameters()
            .getAttribute();
    Assert.assertEquals(
        String.valueOf(IoTDBConstant.SUPER_USER_ID),
        sinkAttributes.get(PipeSinkConstant.SINK_IOTDB_USER_ID));
    Assert.assertEquals(rootUserName, sinkAttributes.get(PipeSinkConstant.SINK_IOTDB_USERNAME_KEY));
    Assert.assertEquals(rootPassword, sinkAttributes.get(PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY));
  }

  @Test
  public void testEnrichLoadedPipeMetasWithRootUserForCompatibility() {
    final String rootPassword = "root-current-password";
    pipeTaskInfo = new PipeTaskInfo(username -> rootPassword);

    createPipeWithSourceAttributes(
        "loadedPipe",
        new HashMap<String, String>() {
          {
            put("extractor", "iotdb-source");
          }
        });
    final Map<String, String> sourceAttributes =
        pipeTaskInfo
            .getPipeMetaByPipeName("loadedPipe")
            .getStaticMeta()
            .getSourceParameters()
            .getAttribute();
    sourceAttributes.remove(PipeSourceConstant.SOURCE_IOTDB_USER_ID);
    sourceAttributes.remove(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY);
    sourceAttributes.remove(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY);

    pipeTaskInfo.enrichPipeMetasWithRootUserForCompatibility();

    Assert.assertEquals(
        rootPassword, sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY));
  }

  @Test
  public void testHandleSuccessfulRestartClearsRuntimeExceptionMessages() {
    final String pipeName = "restartPipe";
    createPipe(pipeName, PipeStatus.RUNNING);

    Assert.assertTrue(
        pipeTaskInfo.recordDataNodePushPipeMetaExceptions(createErrorRespMap(pipeName)));

    final PipeRuntimeMeta runtimeMeta =
        pipeTaskInfo.getPipeMetaByPipeName(pipeName).getRuntimeMeta();
    Assert.assertEquals(PipeStatus.STOPPED, runtimeMeta.getStatus().get());
    Assert.assertTrue(runtimeMeta.getIsStoppedByRuntimeException());
    Assert.assertFalse(runtimeMeta.getNodeId2PipeRuntimeExceptionMap().isEmpty());

    Assert.assertTrue(pipeTaskInfo.autoRestart());
    final long exceptionsClearTime = runtimeMeta.getExceptionsClearTime();
    Assert.assertTrue(
        runtimeMeta.getNodeId2PipeRuntimeExceptionMap().values().stream()
            .allMatch(exception -> exception.getTimeStamp() <= exceptionsClearTime));

    pipeTaskInfo.handleSuccessfulRestart();

    Assert.assertEquals(PipeStatus.RUNNING, runtimeMeta.getStatus().get());
    Assert.assertFalse(runtimeMeta.getIsStoppedByRuntimeException());
    Assert.assertTrue(runtimeMeta.getNodeId2PipeRuntimeExceptionMap().isEmpty());
    Assert.assertEquals(exceptionsClearTime, runtimeMeta.getExceptionsClearTime());
  }

  @Test
  public void testLegacyStatusAndDropPlansTargetTableOnlyPipeByName() {
    final String pipeName = "legacyTablePipe";
    createPipe(pipeName, PipeStatus.STOPPED, true);

    pipeTaskInfo.setPipeStatus(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));
    Assert.assertEquals(
        PipeStatus.RUNNING,
        pipeTaskInfo.getPipeMetaByPipeName(pipeName, true).getRuntimeMeta().getStatus().get());

    pipeTaskInfo.dropPipe(new DropPipePlanV2(pipeName));
    Assert.assertFalse(pipeTaskInfo.isPipeExisted(pipeName, true));
  }

  @Test
  public void testAlterLegacyDoubleLivingPipePreservesBothVisibility() throws Exception {
    final String pipeName = "oldDoubleLivingPipe";
    createPipeWithSourceAttributes(
        pipeName,
        new HashMap<String, String>() {
          {
            put("extractor", "iotdb-source");
            put(PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY, "true");
          }
        });

    final TAlterPipeReq alterPipeRequest = createAlterPipeRequest(pipeName, true);
    pipeTaskInfo.checkAndUpdateRequestBeforeAlterPipe(alterPipeRequest);

    final Map<String, String> extractorAttributes = alterPipeRequest.getExtractorAttributes();
    Assert.assertEquals(
        "true", extractorAttributes.get(PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY));
    Assert.assertFalse(extractorAttributes.containsKey(SystemConstant.SQL_DIALECT_KEY));
    Assert.assertFalse(extractorAttributes.containsKey(SystemConstant.PIPE_VISIBILITY_KEY));
  }

  @Test
  public void testAlterStrictPipeKeepsStrictVisibilityAndTargetDialect() throws Exception {
    final String pipeName = "strictTablePipe";
    createPipeWithSourceAttributes(
        pipeName,
        new HashMap<String, String>() {
          {
            put("extractor", "iotdb-source");
            put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
            put(SystemConstant.PIPE_VISIBILITY_KEY, SystemConstant.PIPE_VISIBILITY_STRICT_VALUE);
            put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "true");
          }
        });

    final TAlterPipeReq alterPipeRequest = createAlterPipeRequest(pipeName, true);
    pipeTaskInfo.checkAndUpdateRequestBeforeAlterPipe(alterPipeRequest);

    final Map<String, String> extractorAttributes = alterPipeRequest.getExtractorAttributes();
    Assert.assertEquals(
        SystemConstant.SQL_DIALECT_TABLE_VALUE,
        extractorAttributes.get(SystemConstant.SQL_DIALECT_KEY));
    Assert.assertEquals(
        SystemConstant.PIPE_VISIBILITY_STRICT_VALUE,
        extractorAttributes.get(SystemConstant.PIPE_VISIBILITY_KEY));
  }

  private TAlterPipeReq createAlterPipeRequest(final String pipeName, final boolean isTableModel) {
    final TAlterPipeReq alterPipeRequest =
        new TAlterPipeReq(pipeName, new HashMap<>(), new HashMap<>(), false, false);
    alterPipeRequest.setExtractorAttributes(new HashMap<>());
    alterPipeRequest.setIsReplaceAllExtractorAttributes(false);
    alterPipeRequest.setIsTableModel(isTableModel);
    return alterPipeRequest;
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
      exceptionMessage.setCreationTime(creationTime);
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
    extractorAttributes.put("extractor", "iotdb-source");
    if (isTableModel) {
      extractorAttributes.put(
          SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    }
    final PipeStaticMeta pipeStaticMeta =
        createPipeWithSourceAttributes(pipeName, extractorAttributes);

    if (PipeStatus.RUNNING.equals(initialStatus)) {
      pipeTaskInfo.setPipeStatus(
          new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING, isTableModel));
    }
    return pipeStaticMeta;
  }

  private PipeStaticMeta createPipeWithSourceAttributes(
      final String pipeName, final Map<String, String> extractorAttributes) {
    final Map<String, String> connectorAttributes = new HashMap<>();
    connectorAttributes.put("connector", "iotdb-thrift-sink");
    return createPipeWithAttributes(pipeName, extractorAttributes, connectorAttributes);
  }

  private PipeStaticMeta createPipeWithAttributes(
      final String pipeName,
      final Map<String, String> extractorAttributes,
      final Map<String, String> connectorAttributes) {
    final Map<String, String> processorAttributes = new HashMap<>();
    processorAttributes.put("processor", "do-nothing-processor");

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
    return pipeStaticMeta;
  }
}
