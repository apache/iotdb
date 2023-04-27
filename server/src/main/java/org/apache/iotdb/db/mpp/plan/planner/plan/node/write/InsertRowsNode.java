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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InsertRowsNode extends InsertNode implements BatchInsertNode {

  /**
   * Suppose there is an InsertRowsNode, which contains 5 InsertRowNodes,
   * insertRowNodeList={InsertRowNode_0, InsertRowNode_1, InsertRowNode_2, InsertRowNode_3,
   * InsertRowNode_4}, then the insertRowNodeIndexList={0, 1, 2, 3, 4} respectively. But when the
   * InsertRowsNode is split into two InsertRowsNodes according to different database in cluster
   * version, suppose that the InsertRowsNode_1's insertRowNodeList = {InsertRowNode_0,
   * InsertRowNode_3, InsertRowNode_4}, then InsertRowsNode_1's insertRowNodeIndexList = {0, 3, 4};
   * InsertRowsNode_2's insertRowNodeList = {InsertRowNode_1, * InsertRowNode_2} then
   * InsertRowsNode_2's insertRowNodeIndexList= {1, 2} respectively;
   */
  private List<Integer> insertRowNodeIndexList;

  /** the InsertRowsNode list */
  private List<InsertRowNode> insertRowNodeList;

  public InsertRowsNode(PlanNodeId id) {
    super(id);
    insertRowNodeList = new ArrayList<>();
    insertRowNodeIndexList = new ArrayList<>();
  }

  public InsertRowsNode(
      PlanNodeId id, List<Integer> insertRowNodeIndexList, List<InsertRowNode> insertRowNodeList) {
    super(id);
    this.insertRowNodeIndexList = insertRowNodeIndexList;
    this.insertRowNodeList = insertRowNodeList;
  }

  /** record the result of insert rows */
  private Map<Integer, TSStatus> results = new HashMap<>();

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
  }

  public void addOneInsertRowNode(InsertRowNode node, int index) {
    insertRowNodeList.add(node);
    insertRowNodeIndexList.add(index);
  }

  @Override
  public void setSearchIndex(long index) {
    searchIndex = index;
    insertRowNodeList.forEach(plan -> plan.setSearchIndex(index));
  }

  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  public TSStatus[] getFailingStatus() {
    return StatusUtils.getFailingStatus(results, insertRowNodeList.size());
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    InsertRowsNode that = (InsertRowsNode) o;
    return Objects.equals(insertRowNodeIndexList, that.insertRowNodeIndexList)
        && Objects.equals(insertRowNodeList, that.insertRowNodeList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), insertRowNodeIndexList, insertRowNodeList);
  }

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
  public List<ISchemaValidation> getSchemaValidationList() {
    return insertRowNodeList.stream()
        .map(InsertRowNode::getSchemaValidation)
        .collect(Collectors.toList());
  }

  @Override
  public void updateAfterSchemaValidation() throws QueryProcessException {
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      insertRowNode.updateAfterSchemaValidation();
      if (!this.hasFailedMeasurements() && insertRowNode.hasFailedMeasurements()) {
        this.failedMeasurementIndex2Info = insertRowNode.failedMeasurementIndex2Info;
      }
    }
  }

  public static InsertRowsNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId;
    List<InsertRowNode> insertRowNodeList = new ArrayList<>();
    List<Integer> insertRowNodeIndex = new ArrayList<>();

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      InsertRowNode insertRowNode = new InsertRowNode(new PlanNodeId(""));
      insertRowNode.subDeserialize(byteBuffer);
      insertRowNodeList.add(insertRowNode);
    }
    for (int i = 0; i < size; i++) {
      insertRowNodeIndex.add(byteBuffer.getInt());
    }

    planNodeId = PlanNodeId.deserialize(byteBuffer);
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      insertRowNode.setPlanNodeId(planNodeId);
    }

    InsertRowsNode insertRowsNode = new InsertRowsNode(planNodeId);
    insertRowsNode.setInsertRowNodeList(insertRowNodeList);
    insertRowsNode.setInsertRowNodeIndexList(insertRowNodeIndex);
    return insertRowsNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_ROWS.serialize(byteBuffer);

    ReadWriteIOUtils.write(insertRowNodeList.size(), byteBuffer);

    for (InsertRowNode node : insertRowNodeList) {
      node.subSerialize(byteBuffer);
    }
    for (Integer index : insertRowNodeIndexList) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INSERT_ROWS.serialize(stream);

    ReadWriteIOUtils.write(insertRowNodeList.size(), stream);

    for (InsertRowNode node : insertRowNodeList) {
      node.subSerialize(stream);
    }
    for (Integer index : insertRowNodeIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    Map<TRegionReplicaSet, InsertRowsNode> splitMap = new HashMap<>();
    for (int i = 0; i < insertRowNodeList.size(); i++) {
      InsertRowNode insertRowNode = insertRowNodeList.get(i);
      // data region for insert row node
      TRegionReplicaSet dataRegionReplicaSet =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  insertRowNode.devicePath.getFullPath(),
                  TimePartitionUtils.getTimePartition(insertRowNode.getTime()));
      if (splitMap.containsKey(dataRegionReplicaSet)) {
        InsertRowsNode tmpNode = splitMap.get(dataRegionReplicaSet);
        tmpNode.addOneInsertRowNode(insertRowNode, i);
      } else {
        InsertRowsNode tmpNode = new InsertRowsNode(this.getPlanNodeId());
        tmpNode.setDataRegionReplicaSet(dataRegionReplicaSet);
        tmpNode.addOneInsertRowNode(insertRowNode, i);
        splitMap.put(dataRegionReplicaSet, tmpNode);
      }
    }

    return new ArrayList<>(splitMap.values());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRows(this, context);
  }

  @Override
  public long getMinTime() {
    throw new NotImplementedException();
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    throw new NotImplementedException();
  }
}
