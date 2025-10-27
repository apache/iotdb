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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ShowDiskUsageNode extends VirtualSourceNode {

  public static final List<String> SHOW_DISK_USAGE_HEADER_COLUMNS =
      ImmutableList.of(ColumnHeaderConstant.DATA_NODE_ID, ColumnHeaderConstant.SIZE_IN_BYTES);

  private final PartialPath pathPattern;

  public ShowDiskUsageNode(
      PlanNodeId id, TDataNodeLocation dataNodeLocation, PartialPath pathPattern) {
    super(id, dataNodeLocation);
    this.pathPattern = pathPattern;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for ShowDiskUsageNode");
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.SHOW_DISK_USAGE;
  }

  @Override
  public PlanNode clone() {
    return new ShowDiskUsageNode(getPlanNodeId(), getDataNodeLocation(), pathPattern);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return SHOW_DISK_USAGE_HEADER_COLUMNS;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitShowDiskUsage(this, context);
  }

  // We only use DataNodeLocation when do distributionPlan, so DataNodeLocation is no need to
  // serialize
  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SHOW_DISK_USAGE.serialize(byteBuffer);
    pathPattern.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SHOW_DISK_USAGE.serialize(stream);
    pathPattern.serialize(stream);
  }

  public static ShowDiskUsageNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    PartialPath pathPattern = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    return new ShowDiskUsageNode(planNodeId, null, pathPattern);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ShowDiskUsageNode that = (ShowDiskUsageNode) o;
    return Objects.equals(this.pathPattern, that.pathPattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), pathPattern);
  }

  @Override
  public String toString() {
    return String.format(
        "ShowDiskUsageNode-%s: [pathPattern: %s]", this.getPlanNodeId(), pathPattern);
  }
}
