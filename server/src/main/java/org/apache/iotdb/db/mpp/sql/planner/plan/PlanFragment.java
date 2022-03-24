/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.sql.planner.plan;

import org.apache.iotdb.commons.partition.DataRegionReplicaSet;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SourceNode;

/** PlanFragment contains a sub-query of distributed query. */
public class PlanFragment {
  private PlanFragmentId id;
  private PlanNode root;

  public PlanFragment(PlanFragmentId id, PlanNode root) {
    this.id = id;
    this.root = root;
  }

  public PlanFragmentId getId() {
    return id;
  }

  public PlanNode getRoot() {
    return root;
  }

  public String toString() {
    return String.format("PlanFragment-%s", getId());
  }

  // Every Fragment should only run in DataRegion.
  // But it can select any one of the Endpoint of the target DataRegion
  // In current version, one PlanFragment should contain at least one SourceNode,
  // and the DataRegions of all SourceNodes should be same in one PlanFragment.
  // So we can use the DataRegion of one SourceNode as the PlanFragment's DataRegion.
  public DataRegionReplicaSet getTargetDataRegion() {
    return getNodeDataRegion(root);
  }

  private DataRegionReplicaSet getNodeDataRegion(PlanNode root) {
    if (root instanceof SourceNode) {
      return ((SourceNode) root).getDataRegionReplicaSet();
    }
    for (PlanNode child : root.getChildren()) {
      DataRegionReplicaSet result = getNodeDataRegion(child);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  public PlanNode getPlanNodeById(PlanNodeId nodeId) {
    return getPlanNodeById(root, nodeId);
  }

  private PlanNode getPlanNodeById(PlanNode root, PlanNodeId nodeId) {
    if (root.getId().equals(nodeId)) {
      return root;
    }
    for (PlanNode child : root.getChildren()) {
      PlanNode node = getPlanNodeById(child, nodeId);
      if (node != null) {
        return node;
      }
    }
    return null;
  }
}
