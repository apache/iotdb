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
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.utils.TimePartitionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FastInsertRowsNode extends InsertRowsNode {
  public FastInsertRowsNode(PlanNodeId id) {
    super(id);
  }

  public FastInsertRowsNode(
      PlanNodeId id,
      List<Integer> insertRowNodeIndexList,
      List<InsertRowNode> fastInsertRowNodeList) {
    super(id, insertRowNodeIndexList, fastInsertRowNodeList);
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    Map<TRegionReplicaSet, InsertRowsNode> splitMap = new HashMap<>();
    for (int i = 0; i < getInsertRowNodeList().size(); i++) {
      InsertRowNode insertRowNode = getInsertRowNodeList().get(i);
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
        InsertRowsNode tmpNode = new FastInsertRowsNode(this.getPlanNodeId());
        tmpNode.setDataRegionReplicaSet(dataRegionReplicaSet);
        tmpNode.addOneInsertRowNode(insertRowNode, i);
        splitMap.put(dataRegionReplicaSet, tmpNode);
      }
    }

    return new ArrayList<>(splitMap.values());
  }
}
