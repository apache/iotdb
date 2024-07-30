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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProcessNode;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class NodeManagementMemoryMergeNode extends ProcessNode {
  private final Set<TSchemaNode> data;

  private PlanNode child;

  public NodeManagementMemoryMergeNode(PlanNodeId id, Set<TSchemaNode> data) {
    super(id);
    this.data = data;
  }

  public Set<TSchemaNode> getData() {
    return data;
  }

  public PlanNode getChild() {
    return child;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.NODE_MANAGEMENT_MEMORY_MERGE;
  }

  @Override
  public PlanNode clone() {
    return new NodeManagementMemoryMergeNode(getPlanNodeId(), this.data);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitNodeManagementMemoryMerge(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.NODE_MANAGEMENT_MEMORY_MERGE.serialize(byteBuffer);
    int size = data.size();
    ReadWriteIOUtils.write(size, byteBuffer);
    data.forEach(node -> ThriftCommonsSerDeUtils.serializeTSchemaNode(node, byteBuffer));
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.NODE_MANAGEMENT_MEMORY_MERGE.serialize(stream);
    int size = data.size();
    ReadWriteIOUtils.write(size, stream);
    data.forEach(node -> ThriftCommonsSerDeUtils.serializeTSchemaNode(node, stream));
  }

  public static NodeManagementMemoryMergeNode deserialize(ByteBuffer byteBuffer) {
    Set<TSchemaNode> data = new HashSet<>();
    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      data.add(ThriftCommonsSerDeUtils.deserializeTSchemaNode(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new NodeManagementMemoryMergeNode(planNodeId, data);
  }

  @Override
  public String toString() {
    return String.format("NodeManagementMemoryMergeNode-%s", this.getPlanNodeId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    NodeManagementMemoryMergeNode that = (NodeManagementMemoryMergeNode) o;
    return Objects.equals(data, that.data) && Objects.equals(child, that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), data, child);
  }
}
