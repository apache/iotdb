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

package org.apache.iotdb.db.queryengine.plan.planner.node.metadata.write;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAliasSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DropAliasSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.LockAliasNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MarkSeriesEnabledNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MarkSeriesInvalidNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.PreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.RollbackPreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.RollbackSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.UnlockForAliasNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.UpdatePhysicalAliasRefNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.ConstructLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.DeleteLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.RollbackLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.mpp.rpc.thrift.TTimeSeriesInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataWriteNodeSerDeTest {
  @Test
  public void testActivateTemplateNode() throws IllegalPathException {
    PlanNodeId planNodeId = new PlanNodeId("ActivateTemplateNode");
    ActivateTemplateNode activateTemplateNode =
        new ActivateTemplateNode(planNodeId, new PartialPath("root.sg.d1.s1"), 2, 1);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    activateTemplateNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(activateTemplateNode, deserializedNode);
  }

  @Test
  public void testAlterTimeSeriesNode() throws IllegalPathException {
    PlanNodeId planNodeId = new PlanNodeId("AlterTimeSeriesNode");
    Map<String, String> map = new HashMap<>();
    map.put("a", "b");
    AlterTimeSeriesNode alterTimeSeriesNode =
        new AlterTimeSeriesNode(
            planNodeId,
            new MeasurementPath("root.sg.d1.s1"),
            AlterTimeSeriesStatement.AlterType.RENAME,
            map,
            "alias",
            map,
            map,
            false);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    alterTimeSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(alterTimeSeriesNode, deserializedNode);
  }

  @Test
  public void testBatchActivateTemplateNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("batchActivateTemplateNode");
    Map<PartialPath, Pair<Integer, Integer>> map = new HashMap<>();
    map.put(new PartialPath("root.db.d1.s1"), new Pair<>(1, 2));
    BatchActivateTemplateNode batchActivateTemplateNode =
        new BatchActivateTemplateNode(planNodeId, map);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    batchActivateTemplateNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(batchActivateTemplateNode, deserializedNode);
  }

  @Test
  public void testConstructSchemaBlackListNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("ConstructSchemaBlackListNode");
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.d1.s1"));
    patternTree.appendPathPattern(new PartialPath("root.sg.d2.*"));
    patternTree.constructTree();
    ConstructSchemaBlackListNode constructSchemaBlackListNode =
        new ConstructSchemaBlackListNode(planNodeId, patternTree);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    constructSchemaBlackListNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(constructSchemaBlackListNode, deserializedNode);
  }

  @Test
  public void testCreateAlignedTimeSeriesNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("CreateAlignedTimeSeriesNode");
    CreateAlignedTimeSeriesNode createAlignedTimeSeriesNode =
        new CreateAlignedTimeSeriesNode(
            planNodeId,
            new PartialPath("root.db.d1"),
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.INT64),
            Arrays.asList(TSEncoding.PLAIN, TSEncoding.RLE),
            Arrays.asList(CompressionType.GZIP, CompressionType.ZSTD),
            Arrays.asList("a1", "a2"),
            Arrays.asList(null, null),
            Arrays.asList(null, null));
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    createAlignedTimeSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(createAlignedTimeSeriesNode, deserializedNode);
  }

  @Test
  public void testCreateMultiTimeSeriesNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("CreateMultiTimeSeriesNode");
    MeasurementGroup measurementGroup = new MeasurementGroup();
    measurementGroup.addMeasurement("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.GZIP);
    Map<PartialPath, MeasurementGroup> deviceMap = new HashMap<>();
    deviceMap.put(new PartialPath("root.db"), measurementGroup);
    CreateMultiTimeSeriesNode createAlignedTimeSeriesNode =
        new CreateMultiTimeSeriesNode(planNodeId, deviceMap);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    createAlignedTimeSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(createAlignedTimeSeriesNode, deserializedNode);
  }

  @Test
  public void testCreateTimeSeriesNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("CreateTimeSeriesNode");
    CreateTimeSeriesNode createTimeSeriesNode =
        new CreateTimeSeriesNode(
            planNodeId,
            new MeasurementPath("root.db.d1.s1"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            "alias");
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    createTimeSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(createTimeSeriesNode, deserializedNode);
  }

  @Test
  public void testDeactivateTemplateNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("DeactivateTemplateNode");
    Map<PartialPath, List<Integer>> map = new HashMap<>();
    map.put(new PartialPath("root.db.d1"), Arrays.asList(1, 2));
    DeactivateTemplateNode deactivateTemplateNode = new DeactivateTemplateNode(planNodeId, map);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    deactivateTemplateNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(deactivateTemplateNode, deserializedNode);
  }

  @Test
  public void testDeleteTimeSeriesNode() throws IllegalPathException {
    PlanNodeId planNodeId = new PlanNodeId("DeleteTimeSeriesNode");
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.d1.s1"));
    patternTree.appendPathPattern(new PartialPath("root.sg.d2.*"));
    patternTree.constructTree();
    DeleteTimeSeriesNode deleteTimeSeriesNode = new DeleteTimeSeriesNode(planNodeId, patternTree);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    deleteTimeSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();

    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(deleteTimeSeriesNode, deserializedNode);
  }

  @Test
  public void testInternalBatchActivateTemplateNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("InternalBatchActivateTemplateNode");
    Map<PartialPath, Pair<Integer, Integer>> map = new HashMap<>();
    map.put(new PartialPath("root.db.d1.s1"), new Pair<>(1, 2));
    InternalBatchActivateTemplateNode internalBatchActivateTemplateNode =
        new InternalBatchActivateTemplateNode(planNodeId, map);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    internalBatchActivateTemplateNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(internalBatchActivateTemplateNode, deserializedNode);
  }

  @Test
  public void testInternalCreateMultiTimeSeriesNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("InternalCreateMultiTimeSeriesNode");
    MeasurementGroup measurementGroup = new MeasurementGroup();
    measurementGroup.addMeasurement("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.GZIP);
    Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap = new HashMap<>();
    deviceMap.put(new PartialPath("root.db"), new Pair<>(false, measurementGroup));
    InternalCreateMultiTimeSeriesNode internalCreateMultiTimeSeriesNode =
        new InternalCreateMultiTimeSeriesNode(planNodeId, deviceMap);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    internalCreateMultiTimeSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(internalCreateMultiTimeSeriesNode, deserializedNode);
  }

  @Test
  public void testInternalCreateTimeSeriesNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("InternalCreateTimeSeriesNode");
    MeasurementGroup measurementGroup = new MeasurementGroup();
    measurementGroup.addMeasurement("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.GZIP);
    InternalCreateTimeSeriesNode internalCreateTimeSeriesNode =
        new InternalCreateTimeSeriesNode(
            planNodeId, new PartialPath("root.db.d1"), measurementGroup, false);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    internalCreateTimeSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(internalCreateTimeSeriesNode, deserializedNode);
  }

  @Test
  public void testPreDeactivateTemplateNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("PreDeactivateTemplateNode");
    Map<PartialPath, List<Integer>> map = new HashMap<>();
    map.put(new PartialPath("root.db.d1"), Arrays.asList(1, 2));
    PreDeactivateTemplateNode preDeactivateTemplateNode =
        new PreDeactivateTemplateNode(planNodeId, map);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    preDeactivateTemplateNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(preDeactivateTemplateNode, deserializedNode);
  }

  @Test
  public void testRollbackPreDeactivateTemplateNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("RollbackPreDeactivateTemplateNode");
    Map<PartialPath, List<Integer>> map = new HashMap<>();
    map.put(new PartialPath("root.db.d1"), Arrays.asList(1, 2));
    RollbackPreDeactivateTemplateNode rollbackPreDeactivateTemplateNode =
        new RollbackPreDeactivateTemplateNode(planNodeId, map);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    rollbackPreDeactivateTemplateNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(rollbackPreDeactivateTemplateNode, deserializedNode);
  }

  @Test
  public void testRollbackSchemaBlackListNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("RollbackSchemaBlackListNode");
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.d1.s1"));
    patternTree.appendPathPattern(new PartialPath("root.sg.d2.*"));
    patternTree.constructTree();
    RollbackSchemaBlackListNode rollbackSchemaBlackListNode =
        new RollbackSchemaBlackListNode(planNodeId, patternTree);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    rollbackSchemaBlackListNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(rollbackSchemaBlackListNode, deserializedNode);
  }

  @Test
  public void testAlterLogicalViewNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("AlterLogicalViewNode");
    Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    viewPathToSourceMap.put(
        new PartialPath("root.sg1.d1"), new TimeSeriesViewOperand("root.sg1.d1"));
    AlterLogicalViewNode alterLogicalViewNode =
        new AlterLogicalViewNode(planNodeId, viewPathToSourceMap);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    alterLogicalViewNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(alterLogicalViewNode, deserializedNode);
  }

  @Test
  public void testConstructLogicalViewBlackListNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("ConstructLogicalViewBlackListNode");
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.d1.s1"));
    patternTree.appendPathPattern(new PartialPath("root.sg.d2.*"));
    patternTree.constructTree();
    ConstructLogicalViewBlackListNode constructLogicalViewBlackListNode =
        new ConstructLogicalViewBlackListNode(planNodeId, patternTree);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    constructLogicalViewBlackListNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(constructLogicalViewBlackListNode, deserializedNode);
  }

  @Test
  public void testCreateLogicalViewNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("CreateLogicalViewNode");
    Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    viewPathToSourceMap.put(
        new PartialPath("root.sg1.d1"), new TimeSeriesViewOperand("root.sg1.d1"));
    CreateLogicalViewNode createLogicalViewNode =
        new CreateLogicalViewNode(planNodeId, viewPathToSourceMap);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    createLogicalViewNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(createLogicalViewNode, deserializedNode);
  }

  @Test
  public void testDeleteLogicalViewNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("DeleteLogicalViewNode");
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.d1.s1"));
    patternTree.appendPathPattern(new PartialPath("root.sg.d2.*"));
    patternTree.constructTree();
    DeleteLogicalViewNode deleteLogicalViewNode =
        new DeleteLogicalViewNode(planNodeId, patternTree);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    deleteLogicalViewNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(deleteLogicalViewNode, deserializedNode);
  }

  @Test
  public void testRollbackLogicalViewBlackListNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("RollbackLogicalViewBlackListNode");
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.d1.s1"));
    patternTree.appendPathPattern(new PartialPath("root.sg.d2.*"));
    patternTree.constructTree();
    RollbackLogicalViewBlackListNode rollbackLogicalViewBlackListNode =
        new RollbackLogicalViewBlackListNode(planNodeId, patternTree);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    rollbackLogicalViewBlackListNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(rollbackLogicalViewBlackListNode, deserializedNode);
  }

  @Test
  public void testCreateAliasSeriesNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("CreateAliasSeriesNode");
    PartialPath oldPath = new PartialPath("root.sg.d1.s1");
    PartialPath newPath = new PartialPath("root.sg.d1.alias_s1");

    // Test without TTimeSeriesInfo
    CreateAliasSeriesNode createAliasSeriesNode =
        new CreateAliasSeriesNode(planNodeId, oldPath, newPath);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    createAliasSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(createAliasSeriesNode, deserializedNode);

    // Test with TTimeSeriesInfo
    TTimeSeriesInfo timeSeriesInfo = createTTimeSeriesInfo(oldPath);
    CreateAliasSeriesNode createAliasSeriesNodeWithInfo =
        new CreateAliasSeriesNode(planNodeId, oldPath, newPath, timeSeriesInfo);
    byteBuffer = ByteBuffer.allocate(1024);
    createAliasSeriesNodeWithInfo.serialize(byteBuffer);
    byteBuffer.flip();
    deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(createAliasSeriesNodeWithInfo, deserializedNode);

    // Test with TTimeSeriesInfo and isRollback=true
    CreateAliasSeriesNode createAliasSeriesNodeRollback =
        new CreateAliasSeriesNode(planNodeId, oldPath, newPath, timeSeriesInfo, true);
    byteBuffer = ByteBuffer.allocate(1024);
    createAliasSeriesNodeRollback.serialize(byteBuffer);
    byteBuffer.flip();
    deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(createAliasSeriesNodeRollback, deserializedNode);
  }

  @Test
  public void testMarkSeriesDisabledNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("MarkSeriesDisabledNode");
    PartialPath oldPath = new PartialPath("root.sg.d1.s1");
    PartialPath newPath = new PartialPath("root.sg.d1.alias_s1");

    // Test without isRollback
    MarkSeriesInvalidNode markSeriesDisabledNode =
        new MarkSeriesInvalidNode(planNodeId, oldPath, newPath);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    markSeriesDisabledNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(markSeriesDisabledNode, deserializedNode);

    // Test with isRollback=true
    MarkSeriesInvalidNode markSeriesDisabledNodeRollback =
        new MarkSeriesInvalidNode(planNodeId, oldPath, newPath, true);
    byteBuffer = ByteBuffer.allocate(1024);
    markSeriesDisabledNodeRollback.serialize(byteBuffer);
    byteBuffer.flip();
    deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(markSeriesDisabledNodeRollback, deserializedNode);
  }

  @Test
  public void testUpdatePhysicalAliasRefNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("UpdatePhysicalAliasRefNode");
    PartialPath physicalPath = new PartialPath("root.sg.d1.s1");
    PartialPath newAliasPath = new PartialPath("root.sg.d1.new_alias");
    PartialPath oldAliasPath = new PartialPath("root.sg.d1.old_alias");

    // Test without oldAliasPath
    UpdatePhysicalAliasRefNode updatePhysicalAliasRefNode =
        new UpdatePhysicalAliasRefNode(planNodeId, physicalPath, newAliasPath);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    updatePhysicalAliasRefNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(updatePhysicalAliasRefNode, deserializedNode);

    // Test with isRollback=true (without oldAliasPath)
    UpdatePhysicalAliasRefNode updatePhysicalAliasRefNodeRollback =
        new UpdatePhysicalAliasRefNode(planNodeId, physicalPath, newAliasPath, true);
    byteBuffer = ByteBuffer.allocate(1024);
    updatePhysicalAliasRefNodeRollback.serialize(byteBuffer);
    byteBuffer.flip();
    deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(updatePhysicalAliasRefNodeRollback, deserializedNode);

    // Test with oldAliasPath and isRollback=true
    UpdatePhysicalAliasRefNode updatePhysicalAliasRefNodeWithOld =
        new UpdatePhysicalAliasRefNode(planNodeId, physicalPath, newAliasPath, oldAliasPath, true);
    byteBuffer = ByteBuffer.allocate(1024);
    updatePhysicalAliasRefNodeWithOld.serialize(byteBuffer);
    byteBuffer.flip();
    deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(updatePhysicalAliasRefNodeWithOld, deserializedNode);
  }

  @Test
  public void testDropAliasSeriesNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("DropAliasSeriesNode");
    PartialPath aliasPath = new PartialPath("root.sg.d1.alias_s1");
    PartialPath physicalPath = new PartialPath("root.sg.d1.s1");

    // Test without physicalPath and TTimeSeriesInfo
    DropAliasSeriesNode dropAliasSeriesNode = new DropAliasSeriesNode(planNodeId, aliasPath);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    dropAliasSeriesNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(dropAliasSeriesNode, deserializedNode);

    // Test with physicalPath and TTimeSeriesInfo and isRollback=true
    TTimeSeriesInfo timeSeriesInfo = createTTimeSeriesInfo(physicalPath);
    DropAliasSeriesNode dropAliasSeriesNodeWithInfo =
        new DropAliasSeriesNode(planNodeId, aliasPath, physicalPath, timeSeriesInfo, true);
    byteBuffer = ByteBuffer.allocate(1024);
    dropAliasSeriesNodeWithInfo.serialize(byteBuffer);
    byteBuffer.flip();
    deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(dropAliasSeriesNodeWithInfo, deserializedNode);
  }

  @Test
  public void testMarkSeriesEnabledNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("MarkSeriesEnabledNode");
    PartialPath physicalPath = new PartialPath("root.sg.d1.s1");
    PartialPath aliasPath = new PartialPath("root.sg.d1.alias_s1");

    // Test without aliasPath and TTimeSeriesInfo
    MarkSeriesEnabledNode markSeriesEnabledNode =
        new MarkSeriesEnabledNode(planNodeId, physicalPath);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    markSeriesEnabledNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(markSeriesEnabledNode, deserializedNode);

    // Test with aliasPath and TTimeSeriesInfo and isRollback=true
    TTimeSeriesInfo timeSeriesInfo = createTTimeSeriesInfo(physicalPath);
    MarkSeriesEnabledNode markSeriesEnabledNodeWithInfo =
        new MarkSeriesEnabledNode(planNodeId, physicalPath, aliasPath, timeSeriesInfo, true);
    byteBuffer = ByteBuffer.allocate(1024);
    markSeriesEnabledNodeWithInfo.serialize(byteBuffer);
    byteBuffer.flip();
    deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(markSeriesEnabledNodeWithInfo, deserializedNode);
  }

  @Test
  public void testUnlockForAliasNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("UnlockForAliasNode");
    PartialPath oldPath = new PartialPath("root.sg.d1.s1");
    PartialPath newPath = new PartialPath("root.sg.d1.alias_s1");

    UnlockForAliasNode unlockForAliasNode = new UnlockForAliasNode(planNodeId, oldPath, newPath);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    unlockForAliasNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(unlockForAliasNode, deserializedNode);
  }

  @Test
  public void testLockAliasNode() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("LockAliasNode");
    PartialPath oldPath = new PartialPath("root.sg.d1.s1");
    PartialPath newPath = new PartialPath("root.sg.d1.alias_s1");

    LockAliasNode lockAliasNode = new LockAliasNode(planNodeId, oldPath, newPath);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    lockAliasNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(lockAliasNode, deserializedNode);
  }

  private TTimeSeriesInfo createTTimeSeriesInfo(PartialPath path) throws Exception {
    TTimeSeriesInfo timeSeriesInfo = new TTimeSeriesInfo();
    // Serialize path to binary
    MeasurementPath measurementPath = new MeasurementPath(path.getFullPath(), TSDataType.INT32);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    measurementPath.serialize(dos);
    timeSeriesInfo.setPath(baos.toByteArray());
    timeSeriesInfo.setDataType(TSDataType.INT32.ordinal());
    timeSeriesInfo.setEncoding(TSEncoding.PLAIN.ordinal());
    timeSeriesInfo.setCompressor(CompressionType.GZIP.ordinal());
    timeSeriesInfo.setMeasurementAlias("alias");
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    timeSeriesInfo.setTags(tags);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("attr1", "value1");
    timeSeriesInfo.setAttributes(attributes);
    return timeSeriesInfo;
  }
}
