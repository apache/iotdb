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

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertRowsNode extends InsertNode {

  /**
   * Suppose there is an InsertRowsNode, which contains 5 InsertRowNodes,
   * insertRowNodeList={InsertRowNode_0, InsertRowNode_1, InsertRowNode_2, InsertRowNode_3,
   * InsertRowNode_4}, then the insertRowNodeIndexList={0, 1, 2, 3, 4} respectively. But when the
   * InsertRowsNode is split into two InsertRowsNodes according to different storage group in
   * cluster version, suppose that the InsertRowsNode_1's insertRowNodeList = {InsertRowNode_0,
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

  public Map<Integer, TSStatus> getResults() {
    return results;
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

  public static InsertRowsNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @Override
  public List<InsertNode> splitByPartition(Analysis analysis) {
    Map<RegionReplicaSet, InsertRowsNode> splitMap = new HashMap<>();
    for (int i = 0; i < insertRowNodeList.size(); i++) {
      InsertRowNode insertRowNode = insertRowNodeList.get(i);
      // data region for insert row node
      RegionReplicaSet dataRegionReplicaSet =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  insertRowNode.devicePath.getFullPath(),
                  StorageEngine.getTimePartitionSlot(insertRowNode.getTime()));
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
}
