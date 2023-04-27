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
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
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

public class InsertMultiTabletsNode extends InsertNode implements BatchInsertNode {

  /**
   * the value is used to indict the parent InsertTabletNode's index when the parent
   * InsertTabletNode is split to multi sub InsertTabletNodes. if the InsertTabletNode have no
   * parent plan, the value is zero;
   *
   * <p>suppose we originally have three InsertTabletNodes in one InsertMultiTabletsNode, then the
   * initial InsertMultiTabletsNode would have the following two attributes:
   *
   * <p>insertTabletNodeList={InsertTabletNode_1,InsertTabletNode_2,InsertTabletNode_3}
   *
   * <p>parentInsetTablePlanIndexList={0,0,0} both have three values.
   *
   * <p>if the InsertTabletNode_1 is split into two sub InsertTabletNodes, InsertTabletNode_2 is
   * split into three sub InsertTabletNodes, InsertTabletNode_3 is split into four sub
   * InsertTabletNodes.
   *
   * <p>InsertTabletNode_1={InsertTabletNode_1_subPlan1, InsertTabletNode_1_subPlan2}
   *
   * <p>InsertTabletNode_2={InsertTabletNode_2_subPlan1, InsertTabletNode_2_subPlan2,
   * InsertTabletNode_2_subPlan3}
   *
   * <p>InsertTabletNode_3={InsertTabletNode_3_subPlan1, InsertTabletNode_3_subPlan2,
   * InsertTabletNode_3_subPlan3, InsertTabletNode_3_subPlan4}
   *
   * <p>those sub plans belong to two different raft data groups, so will generate two new
   * InsertMultiTabletNodes
   *
   * <p>InsertMultiTabletNodet1.insertTabletNodeList={InsertTabletNode_1_subPlan1,
   * InsertTabletNode_3_subPlan1, InsertTabletNode_3_subPlan3, InsertTabletNode_3_subPlan4}
   *
   * <p>InsertMultiTabletNodet1.parentInsetTablePlanIndexList={0,2,2,2}
   *
   * <p>InsertMultiTabletNodet2.insertTabletNodeList={InsertTabletNode_1_subPlan2,
   * InsertTabletNode_2_subPlan1, InsertTabletNode_2_subPlan2, InsertTabletNode_2_subPlan3,
   * InsertTabletNode_3_subPlan2}
   *
   * <p>InsertMultiTabletNodet2.parentInsetTablePlanIndexList={0,1,1,1,2}
   *
   * <p>this is usually used to back-propagate exceptions to the parent plan without losing their
   * proper positions.
   */
  List<Integer> parentInsertTabletNodeIndexList;

  /** the InsertTabletNode list */
  List<InsertTabletNode> insertTabletNodeList;

  /** record the result of insert tablets */
  private final Map<Integer, TSStatus> results = new HashMap<>();

  public InsertMultiTabletsNode(PlanNodeId id) {
    super(id);
    parentInsertTabletNodeIndexList = new ArrayList<>();
    insertTabletNodeList = new ArrayList<>();
  }

  public InsertMultiTabletsNode(
      PlanNodeId id,
      List<Integer> parentInsertTabletNodeIndexList,
      List<InsertTabletNode> insertTabletNodeList) {
    super(id);
    this.parentInsertTabletNodeIndexList = parentInsertTabletNodeIndexList;
    this.insertTabletNodeList = insertTabletNodeList;
  }

  public List<Integer> getParentInsertTabletNodeIndexList() {
    return parentInsertTabletNodeIndexList;
  }

  private void setParentInsertTabletNodeIndexList(List<Integer> parentInsertTabletNodeIndexList) {
    this.parentInsertTabletNodeIndexList = parentInsertTabletNodeIndexList;
  }

  public List<InsertTabletNode> getInsertTabletNodeList() {
    return insertTabletNodeList;
  }

  private void setInsertTabletNodeList(List<InsertTabletNode> insertTabletNodeList) {
    this.insertTabletNodeList = insertTabletNodeList;
  }

  public void addInsertTabletNode(InsertTabletNode node, Integer parentIndex) {
    insertTabletNodeList.add(node);
    parentInsertTabletNodeIndexList.add(parentIndex);
  }

  @Override
  public void setSearchIndex(long index) {
    searchIndex = index;
    insertTabletNodeList.forEach(plan -> plan.setSearchIndex(index));
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    return false;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    Map<TRegionReplicaSet, InsertMultiTabletsNode> splitMap = new HashMap<>();
    for (int i = 0; i < insertTabletNodeList.size(); i++) {
      InsertTabletNode insertTabletNode = insertTabletNodeList.get(i);
      List<WritePlanNode> tmpResult = insertTabletNode.splitByPartition(analysis);
      for (WritePlanNode subNode : tmpResult) {
        TRegionReplicaSet dataRegionReplicaSet = ((InsertNode) subNode).getDataRegionReplicaSet();
        if (splitMap.containsKey(dataRegionReplicaSet)) {
          InsertMultiTabletsNode tmpNode = splitMap.get(dataRegionReplicaSet);
          tmpNode.addInsertTabletNode((InsertTabletNode) subNode, i);
        } else {
          InsertMultiTabletsNode tmpNode = new InsertMultiTabletsNode(this.getPlanNodeId());
          tmpNode.setDataRegionReplicaSet(dataRegionReplicaSet);
          tmpNode.addInsertTabletNode((InsertTabletNode) subNode, i);
          splitMap.put(dataRegionReplicaSet, tmpNode);
        }
      }
    }
    return new ArrayList<>(splitMap.values());
  }

  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  public TSStatus[] getFailingStatus() {
    return StatusUtils.getFailingStatus(results, insertTabletNodeList.size());
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
  public List<ISchemaValidation> getSchemaValidationList() {
    return insertTabletNodeList.stream()
        .map(InsertTabletNode::getSchemaValidation)
        .collect(Collectors.toList());
  }

  public static InsertMultiTabletsNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId;
    List<InsertTabletNode> insertTabletNodeList = new ArrayList<>();
    List<Integer> parentIndex = new ArrayList<>();

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      InsertTabletNode insertTabletNode = new InsertTabletNode(new PlanNodeId(""));
      insertTabletNode.subDeserialize(byteBuffer);
      insertTabletNodeList.add(insertTabletNode);
    }
    for (int i = 0; i < size; i++) {
      parentIndex.add(byteBuffer.getInt());
    }

    planNodeId = PlanNodeId.deserialize(byteBuffer);
    for (InsertTabletNode insertTabletNode : insertTabletNodeList) {
      insertTabletNode.setPlanNodeId(planNodeId);
    }

    InsertMultiTabletsNode insertMultiTabletsNode = new InsertMultiTabletsNode(planNodeId);
    insertMultiTabletsNode.setInsertTabletNodeList(insertTabletNodeList);
    insertMultiTabletsNode.setParentInsertTabletNodeIndexList(parentIndex);
    return insertMultiTabletsNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_MULTI_TABLET.serialize(byteBuffer);

    ReadWriteIOUtils.write(insertTabletNodeList.size(), byteBuffer);

    for (InsertTabletNode node : insertTabletNodeList) {
      node.subSerialize(byteBuffer);
    }
    for (Integer index : parentInsertTabletNodeIndexList) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INSERT_MULTI_TABLET.serialize(stream);

    ReadWriteIOUtils.write(insertTabletNodeList.size(), stream);

    for (InsertTabletNode node : insertTabletNodeList) {
      node.subSerialize(stream);
    }
    for (Integer index : parentInsertTabletNodeIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    InsertMultiTabletsNode that = (InsertMultiTabletsNode) o;
    return Objects.equals(parentInsertTabletNodeIndexList, that.parentInsertTabletNodeIndexList)
        && Objects.equals(insertTabletNodeList, that.insertTabletNodeList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), parentInsertTabletNodeIndexList, insertTabletNodeList);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInsertMultiTablets(this, context);
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
