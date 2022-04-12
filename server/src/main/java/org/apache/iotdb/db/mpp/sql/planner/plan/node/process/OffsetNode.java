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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.sql.planner.plan.IOutputPlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * OffsetNode is used to skip top n result from upstream nodes. It uses the default order of
 * upstream nodes
 */
public class OffsetNode extends ProcessNode implements IOutputPlanNode {

  // The limit count
  private PlanNode child;
  private final int offset;

  public OffsetNode(PlanNodeId id, int offset) {
    super(id);
    this.offset = offset;
  }

  public OffsetNode(PlanNodeId id, PlanNode child, int offset) {
    this(id, offset);
    this.child = child;
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
  public PlanNode clone() {
    return new OffsetNode(getPlanNodeId(), offset);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return ((IOutputPlanNode) child).getOutputColumnHeaders();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ((IOutputPlanNode) child).getOutputColumnNames();
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return ((IOutputPlanNode) child).getOutputColumnTypes();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitOffset(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.OFFSET.serialize(byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
  }

  public static OffsetNode deserialize(ByteBuffer byteBuffer) {
    int offset = ReadWriteIOUtils.readInt(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new OffsetNode(planNodeId, offset);
  }

  public PlanNode getChild() {
    return child;
  }

  public int getOffset() {
    return offset;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[OffsetNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("RowOffset: " + this.getOffset());
    return new Pair<>(title, attributes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OffsetNode that = (OffsetNode) o;
    return offset == that.offset && Objects.equals(child, that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(child, offset);
  }
}
