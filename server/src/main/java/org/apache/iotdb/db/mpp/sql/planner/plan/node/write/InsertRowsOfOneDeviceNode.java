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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InsertRowsOfOneDeviceNode extends InsertNode implements BatchInsertNode {

  /**
   * Suppose there is an InsertRowsOfOneDeviceNode, which contains 5 InsertRowNodes,
   * insertRowNodeList={InsertRowNode_0, InsertRowNode_1, InsertRowNode_2, InsertRowNode_3,
   * InsertRowNode_4}, then the insertRowNodeIndexList={0, 1, 2, 3, 4} respectively. But when the
   * InsertRowsOfOneDeviceNode is split into two InsertRowsOfOneDeviceNodes according to different
   * storage group in cluster version, suppose that the InsertRowsOfOneDeviceNode_1's
   * insertRowNodeList = {InsertRowNode_0, InsertRowNode_3, InsertRowNode_4}, then
   * InsertRowsOfOneDeviceNode_1's insertRowNodeIndexList = {0, 3, 4}; InsertRowsOfOneDeviceNode_2's
   * insertRowNodeList = {InsertRowNode_1, * InsertRowNode_2} then InsertRowsOfOneDeviceNode_2's
   * insertRowNodeIndexList= {1, 2} respectively;
   */
  private List<Integer> insertRowNodeIndexList;

  /** the InsertRowsNode list */
  private List<InsertRowNode> insertRowNodeList;

  /** record the result of insert rows */
  private Map<Integer, TSStatus> results = new HashMap<>();

  public InsertRowsOfOneDeviceNode(PlanNodeId id) {
    super(id);
    insertRowNodeIndexList = new ArrayList<>();
    insertRowNodeList = new ArrayList<>();
  }

  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  public TSStatus[] getFailingStatus() {
    return StatusUtils.getFailingStatus(results, insertRowNodeList.size());
  }

  public List<Integer> getInsertRowNodeIndexList() {
    return insertRowNodeIndexList;
  }

  public void setInsertRowNodeIndexList(List<Integer> insertRowNodeIndexList) {
    this.insertRowNodeIndexList = insertRowNodeIndexList;
  }

  public List<InsertRowNode> getInsertRowNodeList() {
    return insertRowNodeList;
  }

  public void setInsertRowNodeList(List<InsertRowNode> insertRowNodeList) {
    this.insertRowNodeList = insertRowNodeList;

    if (insertRowNodeList == null || insertRowNodeList.isEmpty()) {
      return;
    }

    devicePath = insertRowNodeList.get(0).getDevicePath();
    isAligned = insertRowNodeList.get(0).isAligned;
    Map<String, TSDataType> measurementsAndDataType = new HashMap<>();
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      List<String> measurements = Arrays.asList(insertRowNode.getMeasurements());
      Map<String, TSDataType> subMap =
          measurements.stream()
              .collect(
                  Collectors.toMap(
                      key -> key, key -> insertRowNode.dataTypes[measurements.indexOf(key)]));
      measurementsAndDataType.putAll(subMap);
    }
    measurements = measurementsAndDataType.keySet().toArray(new String[0]);
    dataTypes = measurementsAndDataType.values().toArray(new TSDataType[0]);
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public boolean validateSchema(SchemaTree schemaTree) {
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      if (!insertRowNode.validateSchema(schemaTree)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void setMeasurementSchemas(SchemaTree schemaTree) {
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      insertRowNode.setMeasurementSchemas(schemaTree);
    }
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    List<WritePlanNode> result = new ArrayList<>();

    Map<TRegionReplicaSet, List<InsertRowNode>> splitMap = new HashMap<>();
    Map<TRegionReplicaSet, List<Integer>> splitMapForIndex = new HashMap<>();

    for (int i = 0; i < insertRowNodeList.size(); i++) {
      InsertRowNode insertRowNode = insertRowNodeList.get(i);
      TRegionReplicaSet dataRegionReplicaSet =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  devicePath.getFullPath(),
                  StorageEngineV2.getTimePartitionSlot(insertRowNode.getTime()));
      List<InsertRowNode> tmpMap =
          splitMap.computeIfAbsent(dataRegionReplicaSet, k -> new ArrayList<>());
      List<Integer> tmpIndexMap =
          splitMapForIndex.computeIfAbsent(dataRegionReplicaSet, k -> new ArrayList<>());

      tmpMap.add(insertRowNode);
      tmpIndexMap.add(insertRowNodeIndexList.get(i));
    }

    for (Map.Entry<TRegionReplicaSet, List<InsertRowNode>> entry : splitMap.entrySet()) {
      InsertRowsOfOneDeviceNode reducedNode = new InsertRowsOfOneDeviceNode(this.getPlanNodeId());
      reducedNode.setInsertRowNodeList(entry.getValue());
      reducedNode.setInsertRowNodeIndexList(splitMapForIndex.get(entry.getKey()));
      reducedNode.setDataRegionReplicaSet(entry.getKey());
      result.add(reducedNode);
    }
    return result;
  }

  public static InsertRowsOfOneDeviceNode deserialize(ByteBuffer byteBuffer) {
    PartialPath devicePath;
    PlanNodeId planNodeId;
    List<InsertRowNode> insertRowNodeList = new ArrayList<>();
    List<Integer> insertRowNodeIndex = new ArrayList<>();

    try {
      devicePath = new PartialPath(ReadWriteIOUtils.readString(byteBuffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertRowsOfOneDeviceNode", e);
    }

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      InsertRowNode insertRowNode = new InsertRowNode(new PlanNodeId(""));
      insertRowNode.setDevicePath(devicePath);
      insertRowNode.setTime(byteBuffer.getLong());
      insertRowNode.deserializeMeasurementsAndValues(byteBuffer);
      insertRowNodeList.add(insertRowNode);
    }
    for (int i = 0; i < size; i++) {
      insertRowNodeIndex.add(byteBuffer.getInt());
    }

    planNodeId = PlanNodeId.deserialize(byteBuffer);
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      insertRowNode.setPlanNodeId(planNodeId);
    }

    InsertRowsOfOneDeviceNode insertRowsOfOneDeviceNode = new InsertRowsOfOneDeviceNode(planNodeId);
    insertRowsOfOneDeviceNode.setInsertRowNodeList(insertRowNodeList);
    insertRowsOfOneDeviceNode.setInsertRowNodeIndexList(insertRowNodeIndex);
    insertRowsOfOneDeviceNode.setDevicePath(devicePath);
    return insertRowsOfOneDeviceNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_ROWS_OF_ONE_DEVICE.serialize(byteBuffer);
    ReadWriteIOUtils.write(devicePath.getFullPath(), byteBuffer);

    byteBuffer.putInt(insertRowNodeList.size());

    for (InsertRowNode node : insertRowNodeList) {
      byteBuffer.putLong(node.getTime());
      node.serializeMeasurementsAndValues(byteBuffer);
    }
    for (Integer index : insertRowNodeIndexList) {
      byteBuffer.putInt(index);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    InsertRowsOfOneDeviceNode that = (InsertRowsOfOneDeviceNode) o;
    return Objects.equals(insertRowNodeIndexList, that.insertRowNodeIndexList)
        && Objects.equals(insertRowNodeList, that.insertRowNodeList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), insertRowNodeIndexList, insertRowNodeList);
  }

  @Override
  public List<PartialPath> getDevicePaths() {
    if (insertRowNodeList == null || insertRowNodeList.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(insertRowNodeList.get(0).devicePath);
  }

  @Override
  public List<String[]> getMeasurementsList() {
    if (insertRowNodeList == null || insertRowNodeList.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(measurements);
  }

  @Override
  public List<TSDataType[]> getDataTypesList() {
    if (insertRowNodeList == null || insertRowNodeList.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(dataTypes);
  }

  @Override
  public List<Boolean> getAlignedList() {
    if (insertRowNodeList == null || insertRowNodeList.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(insertRowNodeList.get(0).isAligned);
  }
}
