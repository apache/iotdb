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
package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class LastQueryNode extends MultiChildProcessNode {

  // the ordering of timeseries in the result of last query
  // which is set to null if there is no need to sort
  private Ordering timeseriesOrdering;

  // if children contains LastTransformNode, this variable is only used in distribute plan
  private boolean containsLastTransformNode;

  public LastQueryNode(
      PlanNodeId id, @Nullable Ordering timeseriesOrdering, boolean containsLastTransformNode) {
    super(id);
    this.timeseriesOrdering = timeseriesOrdering;
    this.containsLastTransformNode = containsLastTransformNode;
  }

  public LastQueryNode(
      PlanNodeId id,
      List<PlanNode> children,
      @Nullable Ordering timeseriesOrdering,
      boolean containsLastTransformNode) {
    super(id, children);
    this.timeseriesOrdering = timeseriesOrdering;
    this.containsLastTransformNode = containsLastTransformNode;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    children.add(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.LAST_QUERY;
  }

  @Override
  public PlanNode clone() {
    return new LastQueryNode(getPlanNodeId(), timeseriesOrdering, containsLastTransformNode);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return LAST_QUERY_HEADER_COLUMNS;
  }

  @Override
  public String toString() {
    return String.format("LastQueryNode-%s", this.getPlanNodeId());
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
    LastQueryNode that = (LastQueryNode) o;
    if (timeseriesOrdering == null) {
      return that.timeseriesOrdering == null;
    }
    return timeseriesOrdering.equals(that.timeseriesOrdering);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeseriesOrdering);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLastQuery(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY.serialize(byteBuffer);
    if (timeseriesOrdering == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ReadWriteIOUtils.write(timeseriesOrdering.ordinal(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY.serialize(stream);
    if (timeseriesOrdering == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(timeseriesOrdering.ordinal(), stream);
    }
  }

  public static LastQueryNode deserialize(ByteBuffer byteBuffer) {
    byte needOrderByTimeseries = ReadWriteIOUtils.readByte(byteBuffer);
    Ordering timeseriesOrdering = null;
    if (needOrderByTimeseries == 1) {
      timeseriesOrdering = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryNode(planNodeId, timeseriesOrdering, false);
  }

  @Override
  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  public Ordering getTimeseriesOrdering() {
    return timeseriesOrdering;
  }

  public void setTimeseriesOrdering(Ordering timeseriesOrdering) {
    this.timeseriesOrdering = timeseriesOrdering;
  }

  public boolean isContainsLastTransformNode() {
    return this.containsLastTransformNode;
  }

  public void setContainsLastTransformNode() {
    this.containsLastTransformNode = true;
  }

  public boolean needOrderByTimeseries() {
    return timeseriesOrdering != null;
  }
}
