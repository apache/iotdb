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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RelationalInsertMultiTabletsNode extends InsertMultiTabletsNode {

  public RelationalInsertMultiTabletsNode(PlanNodeId id) {
    super(id);
  }

  public RelationalInsertMultiTabletsNode(
      PlanNodeId id,
      List<Integer> parentInsertTabletNodeIndexList,
      List<InsertTabletNode> insertTabletNodeList) {
    super(id, parentInsertTabletNodeIndexList, insertTabletNodeList);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RELATIONAL_INSERT_MULTI_TABLETS;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    Map<TRegionReplicaSet, RelationalInsertMultiTabletsNode> splitMap = new HashMap<>();
    final List<TEndPoint> redirectNodeList = new ArrayList<>();
    for (int i = 0; i < insertTabletNodeList.size(); i++) {
      InsertTabletNode insertTabletNode = insertTabletNodeList.get(i);
      List<WritePlanNode> tmpResult = insertTabletNode.splitByPartition(analysis);
      redirectNodeList.addAll(analysis.getRedirectNodeList());
      for (WritePlanNode subNode : tmpResult) {
        TRegionReplicaSet dataRegionReplicaSet = ((InsertNode) subNode).getDataRegionReplicaSet();
        RelationalInsertMultiTabletsNode tmpNode = splitMap.get(dataRegionReplicaSet);
        if (tmpNode == null) {
          tmpNode = new RelationalInsertMultiTabletsNode(this.getPlanNodeId());
          tmpNode.setDataRegionReplicaSet(dataRegionReplicaSet);
          tmpNode.setPhysicalTime(getPhysicalTime());
          tmpNode.setNodeId(getNodeId());
          tmpNode.setSyncIndex(getSyncIndex());
          splitMap.put(dataRegionReplicaSet, tmpNode);
        }
        tmpNode.addInsertTabletNode((InsertTabletNode) subNode, i);
      }
    }
    analysis.setRedirectNodeList(redirectNodeList);
    return new ArrayList<>(splitMap.values());
  }

  public static RelationalInsertMultiTabletsNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId;
    List<InsertTabletNode> insertTabletNodeList = new ArrayList<>();
    List<Integer> parentIndex = new ArrayList<>();

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      RelationalInsertTabletNode insertTabletNode =
          new RelationalInsertTabletNode(new PlanNodeId(""));
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

    return new RelationalInsertMultiTabletsNode(planNodeId, parentIndex, insertTabletNodeList);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.RELATIONAL_INSERT_MULTI_TABLETS.serialize(byteBuffer);

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
    PlanNodeType.RELATIONAL_INSERT_MULTI_TABLETS.serialize(stream);

    ReadWriteIOUtils.write(insertTabletNodeList.size(), stream);

    for (InsertTabletNode node : insertTabletNodeList) {
      node.subSerialize(stream);
    }
    for (Integer index : parentInsertTabletNodeIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitRelationalInsertMultiTablets(this, context);
  }
}
