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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.schema.RenameTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.mpp.rpc.thrift.TCreateAliasSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropAliasSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TRenameTimeSeriesResp;
import org.apache.iotdb.mpp.rpc.thrift.TTimeSeriesInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RenameTimeSeriesProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IOException, IllegalPathException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    PartialPath oldPath = new PartialPath("root.sg1.d1.s1");
    PartialPath newPath = new PartialPath("root.sg1.d1.s1_alias");
    RenameTimeSeriesProcedure proc =
        new RenameTimeSeriesProcedure("testQueryId", oldPath, newPath, false);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      RenameTimeSeriesProcedure proc2 =
          (RenameTimeSeriesProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(proc.getQueryId(), proc2.getQueryId());
      assertEquals(proc.getOldPath(), proc2.getOldPath());
      assertEquals(proc.getNewPath(), proc2.getNewPath());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void serializeDeserializeShouldRetainRollbackContext() throws Exception {
    final PartialPath oldPath = new PartialPath("root.sg.d1.alias_s1");
    final PartialPath newPath = new PartialPath("root.sg.d2.alias_s1");
    final PartialPath physicalPath = new PartialPath("root.sg.device.s1");
    final RenameTimeSeriesProcedure proc =
        new RenameTimeSeriesProcedure("testQueryId", oldPath, newPath, false);
    final TTimeSeriesInfo timeSeriesInfo = createTTimeSeriesInfo(physicalPath, "alias_s1");

    proc.setAnalysisContext(createAnalysisContext(true, physicalPath, timeSeriesInfo));
    proc.registerRollbackAction(RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_CREATE_ALIAS);
    proc.registerRollbackAction(
        RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_DROP_ALIAS_SERIES);

    final RenameTimeSeriesProcedure proc2 = serializeAndDeserialize(proc);

    assertNotNull(proc2.getAnalysisContext());
    assertTrue(proc2.getAnalysisContext().isIsRenamed());
    assertEquals(
        physicalPath,
        (PartialPath)
            PathDeserializeUtil.deserialize(
                ByteBuffer.wrap(proc2.getAnalysisContext().getPhysicalPath())));
    assertEquals("alias_s1", proc2.getAnalysisContext().getTimeSeriesInfo().getMeasurementAlias());
    assertEquals(
        Arrays.asList(
            RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_DROP_ALIAS_SERIES,
            RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_CREATE_ALIAS),
        proc2.getRollbackActionsSnapshot());
  }

  @Test
  public void rollbackStateShouldExecuteRollbackActionsWhenFailed() throws Exception {
    final PartialPath oldPath = new PartialPath("root.sg.device.s1");
    final PartialPath newPath = new PartialPath("root.sg.device.alias_s1");
    final CapturingRenameTimeSeriesProcedure proc =
        new CapturingRenameTimeSeriesProcedure("testQueryId", oldPath, newPath);

    proc.setAnalysisContext(
        createAnalysisContext(false, null, createTTimeSeriesInfo(oldPath, "alias_s1")));
    proc.registerRollbackAction(RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_CREATE_ALIAS);
    proc.bindRegion(oldPath, 1);
    proc.bindRegion(newPath, 2);
    proc.markFailedForTest();

    proc.rollbackTransformMetadata();

    assertTrue(proc.unlocked);
    assertEquals(1, proc.capturedTasks.size());
    final CapturedTask capturedTask = proc.capturedTasks.get(0);
    assertEquals(CnToDnAsyncRequestType.CREATE_ALIAS_SERIES, capturedTask.requestType);
    assertEquals(2, capturedTask.regionIds.get(0).getId());

    final TCreateAliasSeriesReq req = (TCreateAliasSeriesReq) capturedTask.request;
    assertTrue(req.isIsRollback());
    assertEquals(
        oldPath, (PartialPath) PathDeserializeUtil.deserialize(ByteBuffer.wrap(req.getOldPath())));
    assertEquals(
        newPath, (PartialPath) PathDeserializeUtil.deserialize(ByteBuffer.wrap(req.getNewPath())));
  }

  @Test(timeout = 2000)
  public void rollbackStateShouldDropFailingRollbackAction() throws Exception {
    final PartialPath oldPath = new PartialPath("root.sg.device.s1");
    final PartialPath newPath = new PartialPath("root.sg.device.alias_s1");
    final CapturingRenameTimeSeriesProcedure proc =
        new CapturingRenameTimeSeriesProcedure("testQueryId", oldPath, newPath);

    proc.setAnalysisContext(
        createAnalysisContext(false, null, createTTimeSeriesInfo(oldPath, "alias_s1")));
    proc.registerRollbackAction(RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_CREATE_ALIAS);
    proc.bindRegion(newPath, 2);
    proc.failRegionTaskExecutionForTest();
    proc.markFailedForTest();

    proc.rollbackTransformMetadata();

    assertTrue(proc.unlocked);
    assertEquals(1, proc.executeAttempts);
    assertTrue(proc.getRollbackActionsSnapshot().isEmpty());
  }

  @Test
  public void rollbackStateShouldUseRollbackDropAliasRequestForAliasToPhysical() throws Exception {
    final PartialPath oldPath = new PartialPath("root.sg.device.alias_s1");
    final PartialPath newPath = new PartialPath("root.sg.device.s1");
    final CapturingRenameTimeSeriesProcedure proc =
        new CapturingRenameTimeSeriesProcedure("testQueryId", oldPath, newPath);

    proc.setAnalysisContext(
        createAnalysisContext(true, newPath, createTTimeSeriesInfo(newPath, "alias_s1")));
    proc.registerRollbackAction(
        RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_DROP_ALIAS_SERIES);
    proc.bindRegion(oldPath, 11);
    proc.bindRegion(newPath, 12);
    proc.markFailedForTest();

    proc.rollbackTransformMetadata();

    assertEquals(1, proc.capturedTasks.size());
    final CapturedTask capturedTask = proc.capturedTasks.get(0);
    assertEquals(CnToDnAsyncRequestType.DROP_ALIAS_SERIES, capturedTask.requestType);
    assertEquals(11, capturedTask.regionIds.get(0).getId());

    final TDropAliasSeriesReq req = (TDropAliasSeriesReq) capturedTask.request;
    assertTrue(req.isIsRollback());
    assertEquals(
        oldPath,
        (PartialPath) PathDeserializeUtil.deserialize(ByteBuffer.wrap(req.getAliasPath())));
    assertEquals(
        newPath,
        (PartialPath) PathDeserializeUtil.deserialize(ByteBuffer.wrap(req.getPhysicalPath())));
  }

  @Test
  public void rollbackStateShouldUseCorrectRegionsForAliasToAlias() throws Exception {
    final PartialPath oldPath = new PartialPath("root.sg.old.alias_s1");
    final PartialPath newPath = new PartialPath("root.sg.new.alias_s1");
    final PartialPath physicalPath = new PartialPath("root.sg.device.s1");
    final CapturingRenameTimeSeriesProcedure proc =
        new CapturingRenameTimeSeriesProcedure("testQueryId", oldPath, newPath);

    proc.setAnalysisContext(
        createAnalysisContext(true, physicalPath, createTTimeSeriesInfo(physicalPath, "alias_s1")));
    proc.registerRollbackAction(RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_CREATE_ALIAS);
    proc.registerRollbackAction(
        RenameTimeSeriesProcedure.RollbackAction.ROLLBACK_DROP_ALIAS_SERIES);
    proc.bindRegion(oldPath, 21);
    proc.bindRegion(newPath, 22);
    proc.bindRegion(physicalPath, 23);
    proc.markFailedForTest();

    proc.rollbackTransformMetadata();

    assertEquals(2, proc.capturedTasks.size());

    final CapturedTask dropTask = proc.capturedTasks.get(0);
    assertEquals(CnToDnAsyncRequestType.DROP_ALIAS_SERIES, dropTask.requestType);
    assertEquals(21, dropTask.regionIds.get(0).getId());
    final TDropAliasSeriesReq dropReq = (TDropAliasSeriesReq) dropTask.request;
    assertTrue(dropReq.isIsRollback());

    final CapturedTask createTask = proc.capturedTasks.get(1);
    assertEquals(CnToDnAsyncRequestType.CREATE_ALIAS_SERIES, createTask.requestType);
    assertEquals(22, createTask.regionIds.get(0).getId());
    final TCreateAliasSeriesReq createReq = (TCreateAliasSeriesReq) createTask.request;
    assertTrue(createReq.isIsRollback());
    assertEquals(
        physicalPath,
        (PartialPath) PathDeserializeUtil.deserialize(ByteBuffer.wrap(createReq.getOldPath())));
    assertEquals(
        newPath,
        (PartialPath) PathDeserializeUtil.deserialize(ByteBuffer.wrap(createReq.getNewPath())));
  }

  private RenameTimeSeriesProcedure serializeAndDeserialize(final RenameTimeSeriesProcedure proc)
      throws IOException {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    proc.serialize(outputStream);
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    return (RenameTimeSeriesProcedure) ProcedureFactory.getInstance().create(buffer);
  }

  private TRenameTimeSeriesResp createAnalysisContext(
      final boolean isRenamed, final PartialPath physicalPath, final TTimeSeriesInfo timeSeriesInfo)
      throws Exception {
    final TRenameTimeSeriesResp resp = new TRenameTimeSeriesResp();
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    resp.setIsRenamed(isRenamed);
    resp.setTimeSeriesInfo(timeSeriesInfo);
    if (physicalPath != null) {
      resp.setPhysicalPath(physicalPath.serialize());
    }
    return resp;
  }

  private TTimeSeriesInfo createTTimeSeriesInfo(final PartialPath path, final String alias)
      throws Exception {
    final TTimeSeriesInfo timeSeriesInfo = new TTimeSeriesInfo();
    final MeasurementPath measurementPath =
        new MeasurementPath(path.getFullPath(), TSDataType.INT32);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    measurementPath.serialize(dataOutputStream);
    timeSeriesInfo.setPath(byteArrayOutputStream.toByteArray());
    timeSeriesInfo.setDataType(TSDataType.INT32.ordinal());
    timeSeriesInfo.setEncoding(TSEncoding.PLAIN.ordinal());
    timeSeriesInfo.setCompressor(CompressionType.GZIP.ordinal());
    timeSeriesInfo.setMeasurementAlias(alias);
    timeSeriesInfo.setProps(Collections.emptyMap());
    final Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    timeSeriesInfo.setTags(tags);
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("attr1", "value1");
    timeSeriesInfo.setAttributes(attributes);
    return timeSeriesInfo;
  }

  private static class CapturingRenameTimeSeriesProcedure extends RenameTimeSeriesProcedure {

    private final Map<String, Map<TConsensusGroupId, TRegionReplicaSet>> boundRegions =
        new HashMap<>();
    private final List<CapturedTask> capturedTasks = new ArrayList<>();
    private boolean unlocked = false;
    private boolean failRegionTaskExecution = false;
    private int executeAttempts = 0;

    private CapturingRenameTimeSeriesProcedure(
        final String queryId, final PartialPath oldPath, final PartialPath newPath) {
      super(queryId, oldPath, newPath, false);
    }

    private void bindRegion(final PartialPath path, final int regionId) {
      final TConsensusGroupId groupId =
          new TConsensusGroupId(TConsensusGroupType.SchemaRegion, regionId);
      boundRegions.put(
          path.getFullPath(),
          Collections.singletonMap(
              groupId, new TRegionReplicaSet(groupId, Collections.emptyList())));
    }

    private void markFailedForTest() {
      setFailure(new ProcedureException("Injected failure for rollback test"));
    }

    private void failRegionTaskExecutionForTest() {
      failRegionTaskExecution = true;
    }

    private void rollbackTransformMetadata()
        throws IOException, InterruptedException, ProcedureException {
      rollbackState(null, RenameTimeSeriesState.TRANSFORM_METADATA);
    }

    @Override
    protected Map<TConsensusGroupId, TRegionReplicaSet> getRelatedSchemaRegionGroup(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env,
        final PartialPath path) {
      return boundRegions.getOrDefault(path.getFullPath(), Collections.emptyMap());
    }

    @Override
    protected Map<TConsensusGroupId, TRegionReplicaSet> getOrCreateRelatedSchemaRegionGroup(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env,
        final PartialPath path) {
      return getRelatedSchemaRegionGroup(env, path);
    }

    @Override
    protected <Q> void executeRegionTask(
        final String taskName,
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> regionGroup,
        final CnToDnAsyncRequestType type,
        final java.util.function.BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q>
            requestGen) {
      executeAttempts++;
      if (failRegionTaskExecution) {
        throw new RuntimeException("Injected rollback task failure");
      }
      final List<TConsensusGroupId> regionIds = new ArrayList<>(regionGroup.keySet());
      final Q request = requestGen.apply(new TDataNodeLocation(), regionIds);
      capturedTasks.add(new CapturedTask(taskName, type, regionIds, request));
    }

    @Override
    protected void unlock(
        final org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv env) {
      unlocked = true;
    }
  }

  private static class CapturedTask {

    private final String taskName;
    private final CnToDnAsyncRequestType requestType;
    private final List<TConsensusGroupId> regionIds;
    private final Object request;

    private CapturedTask(
        final String taskName,
        final CnToDnAsyncRequestType requestType,
        final List<TConsensusGroupId> regionIds,
        final Object request) {
      this.taskName = taskName;
      this.requestType = requestType;
      this.regionIds = regionIds;
      this.request = request;
    }
  }
}
