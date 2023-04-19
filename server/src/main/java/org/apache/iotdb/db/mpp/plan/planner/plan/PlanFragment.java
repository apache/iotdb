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
package org.apache.iotdb.db.mpp.plan.planner.plan;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.SubPlanTypeExtractor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.IPartitionRelatedNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.VirtualSourceNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** PlanFragment contains a sub-query of distributed query. */
public class PlanFragment {
  // TODO once you add field for this class you need to change the serialize and deserialize methods
  private final PlanFragmentId id;
  private PlanNode planNodeTree;

  // map from output column name (for every node) to its datatype
  private TypeProvider typeProvider;

  // indicate whether this PlanFragment is the root of the whole Fragment-Plan-Tree or not
  private boolean isRoot;

  public PlanFragment(PlanFragmentId id, PlanNode planNodeTree) {
    this.id = id;
    this.planNodeTree = planNodeTree;
    this.isRoot = false;
  }

  public PlanFragmentId getId() {
    return id;
  }

  public PlanNode getPlanNodeTree() {
    return planNodeTree;
  }

  public void setPlanNodeTree(PlanNode planNodeTree) {
    this.planNodeTree = planNodeTree;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  public void setTypeProvider(TypeProvider typeProvider) {
    this.typeProvider = typeProvider;
  }

  public void generateTypeProvider(TypeProvider allTypes) {
    this.typeProvider = SubPlanTypeExtractor.extractor(planNodeTree, allTypes);
  }

  public boolean isRoot() {
    return isRoot;
  }

  public void setRoot(boolean root) {
    isRoot = root;
  }

  @Override
  public String toString() {
    return String.format("PlanFragment-%s", getId());
  }

  // Every Fragment related with DataPartition should only run in one DataRegion.
  // But it can select any one of the Endpoint of the target DataRegion
  // In current version, one PlanFragment should contain at least one SourceNode,
  // and the DataRegions of all SourceNodes should be same in one PlanFragment.
  // So we can use the DataRegion of one SourceNode as the PlanFragment's DataRegion.
  public TRegionReplicaSet getTargetRegion() {
    return getNodeRegion(planNodeTree);
  }

  // If a Fragment is not related with DataPartition,
  // it may be related with a specific DataNode.
  // This method return the DataNodeLocation will offer execution of this Fragment.
  public TDataNodeLocation getTargetLocation() {
    return getNodeLocation(planNodeTree);
  }

  private TRegionReplicaSet getNodeRegion(PlanNode root) {
    if (root instanceof IPartitionRelatedNode) {
      return ((IPartitionRelatedNode) root).getRegionReplicaSet();
    }
    for (PlanNode child : root.getChildren()) {
      TRegionReplicaSet result = getNodeRegion(child);
      if (result != null && result != DataPartition.NOT_ASSIGNED) {
        return result;
      }
    }
    return null;
  }

  private TDataNodeLocation getNodeLocation(PlanNode root) {
    if (root instanceof VirtualSourceNode) {
      return ((VirtualSourceNode) root).getDataNodeLocation();
    }
    for (PlanNode child : root.getChildren()) {
      TDataNodeLocation result = getNodeLocation(child);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  public void serialize(ByteBuffer byteBuffer) {
    id.serialize(byteBuffer);
    planNodeTree.serialize(byteBuffer);
    if (typeProvider == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      typeProvider.serialize(byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    id.serialize(stream);
    planNodeTree.serialize(stream);
    if (typeProvider == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      typeProvider.serialize(stream);
    }
  }

  public static PlanFragment deserialize(ByteBuffer byteBuffer) {
    PlanFragment planFragment =
        new PlanFragment(PlanFragmentId.deserialize(byteBuffer), deserializeHelper(byteBuffer));
    byte hasTypeProvider = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasTypeProvider == 1) {
      planFragment.setTypeProvider(TypeProvider.deserialize(byteBuffer));
    }
    return planFragment;
  }

  // deserialize the plan node recursively
  public static PlanNode deserializeHelper(ByteBuffer byteBuffer) {
    PlanNode root = PlanNodeType.deserialize(byteBuffer);
    int childrenCount = byteBuffer.getInt();
    for (int i = 0; i < childrenCount; i++) {
      root.addChild(deserializeHelper(byteBuffer));
    }
    return root;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PlanFragment that = (PlanFragment) o;
    return Objects.equals(id, that.id) && Objects.equals(planNodeTree, that.planNodeTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, planNodeTree);
  }
}
