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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InsertMultiTabletNode extends InsertNode {

  /**
   * the value is used to indict the parent InsertTabletNode's index when the parent
   * InsertTabletNode is split to multi sub InsertTabletNodes. if the InsertTabletNode have no
   * parent plan, the value is zero;
   *
   * <p>suppose we originally have three InsertTabletNodes in one InsertMultiTabletNode, then the
   * initial InsertMultiTabletNode would have the following two attributes:
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

  Boolean isEnableMultithreading;

  Integer differentStorageGroupsCount;

  public InsertMultiTabletNode(PlanNodeId id) {
    super(id);
    parentInsertTabletNodeIndexList = new ArrayList<>();
    insertTabletNodeList = new ArrayList<>();
  }

  public List<Integer> getParentInsertTabletNodeIndexList() {
    return parentInsertTabletNodeIndexList;
  }

  public void setParentInsertTabletNodeIndexList(List<Integer> parentInsertTabletNodeIndexList) {
    this.parentInsertTabletNodeIndexList = parentInsertTabletNodeIndexList;
  }

  public List<InsertTabletNode> getInsertTabletNodeList() {
    return insertTabletNodeList;
  }

  public void setInsertTabletNodeList(List<InsertTabletNode> insertTabletNodeList) {
    this.insertTabletNodeList = insertTabletNodeList;
  }

  @Override
  public List<InsertNode> splitByPartition(Analysis analysis) {
    return null;
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

  public static InsertMultiTabletNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  public int getDifferentStorageGroupsCount() {
    if (differentStorageGroupsCount == null) {
      Set<String> insertPlanSGSet = new HashSet<>();
      int defaultStorageGroupLevel = new IoTDBConfig().getDefaultStorageGroupLevel();
      for (InsertTabletNode insertTabletNode : insertTabletNodeList) {
        String[] nodes = insertTabletNode.getDevicePath().getNodes();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i <= defaultStorageGroupLevel && i < nodes.length; i++) {
          stringBuilder.append(nodes[i]).append(".");
        }
        insertPlanSGSet.add(stringBuilder.toString());
      }
      differentStorageGroupsCount = insertPlanSGSet.size();
    }
    return differentStorageGroupsCount;
  }

  public boolean isEnableMultiThreading() {
    // If we enable multithreading, we need to consider the loss of switching between threads,
    // so we need to judge the core threads of the thread pool and the size of the tablet.

    // Therefore, we set the number of core threads in the thread pool to
    // min(the number of different sg, availableProcessors()/2),
    // and need columns >= insertMultiTabletEnableMultithreadingColumnThreshold.

    // It should be noted that if the number of sg is large and exceeds twice of the recommended
    // number of CPU threads,
    // it may lead to failure to allocate out of heap memory and NPE.
    // Therefore, we will also turn off multithreading in this case.
    if (isEnableMultithreading == null) {
      int sgSize = getDifferentStorageGroupsCount();
      // SG should be >= 1 so that it will not be locked and degenerate into serial.
      // SG should be <= Runtime.getRuntime().availableProcessors()*2  so that to avoid failure to
      // allocate out of heap memory and NPE
      if (sgSize <= 1 || sgSize >= Runtime.getRuntime().availableProcessors() * 2) {
        isEnableMultithreading = false;
      } else {
        int count = 0;
        for (InsertTabletNode insertTabletNode : insertTabletNodeList) {
          if (insertTabletNode.getColumns().length
              >= IoTDBDescriptor.getInstance()
                  .getConfig()
                  .getInsertMultiTabletEnableMultithreadingColumnThreshold()) {
            count++;
          }
        }
        isEnableMultithreading = count * 2 >= insertTabletNodeList.size();
      }
    }
    return isEnableMultithreading;
  }
}
