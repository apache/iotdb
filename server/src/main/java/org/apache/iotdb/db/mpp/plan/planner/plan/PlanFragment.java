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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.IPartitionRelatedNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** PlanFragment contains a sub-query of distributed query. */
public class PlanFragment {
  // TODO once you add field for this class you need to change the serialize and deserialize methods
  private PlanFragmentId id;
  private PlanNode root;
  private TypeProvider typeProvider;

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

  public void setRoot(PlanNode root) {
    this.root = root;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  public void setTypeProvider(TypeProvider typeProvider) {
    this.typeProvider = typeProvider;
  }

  @Override
  public String toString() {
    return String.format("PlanFragment-%s", getId());
  }

  // Every Fragment should only run in DataRegion.
  // But it can select any one of the Endpoint of the target DataRegion
  // In current version, one PlanFragment should contain at least one SourceNode,
  // and the DataRegions of all SourceNodes should be same in one PlanFragment.
  // So we can use the DataRegion of one SourceNode as the PlanFragment's DataRegion.
  public TRegionReplicaSet getTargetRegion() {
    return getNodeRegion(root);
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

  public PlanNode getPlanNodeById(PlanNodeId nodeId) {
    return getPlanNodeById(root, nodeId);
  }

  private PlanNode getPlanNodeById(PlanNode root, PlanNodeId nodeId) {
    if (root.getPlanNodeId().equals(nodeId)) {
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

  public void serialize(ByteBuffer byteBuffer) {
    id.serialize(byteBuffer);
    root.serialize(byteBuffer);
    if (typeProvider == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      typeProvider.serialize(byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    id.serialize(stream);
    root.serialize(stream);
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
    return Objects.equals(id, that.id) && Objects.equals(root, that.root);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, root);
  }
}
