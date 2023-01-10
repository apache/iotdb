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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class LastQueryNode extends MultiChildProcessNode {

  private final Filter timeFilter;

  // The result output order, which could sort by sensor and time.
  // The size of this list is 2 and the first SortItem in this list has higher priority.
  private OrderByParameter mergeOrderParameter;

  public LastQueryNode(PlanNodeId id, Filter timeFilter, OrderByParameter mergeOrderParameter) {
    super(id);
    this.timeFilter = timeFilter;
    this.mergeOrderParameter = mergeOrderParameter;
  }

  public LastQueryNode(
      PlanNodeId id,
      List<PlanNode> children,
      Filter timeFilter,
      OrderByParameter mergeOrderParameter) {
    super(id, children);
    this.timeFilter = timeFilter;
    this.mergeOrderParameter = mergeOrderParameter;
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
  public PlanNode clone() {
    return new LastQueryNode(getPlanNodeId(), timeFilter, mergeOrderParameter);
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
    return String.format("LastQueryNode-%s:[TimeFilter: %s]", this.getPlanNodeId(), timeFilter);
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
    return Objects.equals(timeFilter, that.timeFilter)
        && mergeOrderParameter.equals(that.mergeOrderParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeFilter, mergeOrderParameter);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLastQuery(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY.serialize(byteBuffer);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      timeFilter.serialize(byteBuffer);
    }
    mergeOrderParameter.serializeAttributes(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY.serialize(stream);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      timeFilter.serialize(stream);
    }
    mergeOrderParameter.serializeAttributes(stream);
  }

  public static LastQueryNode deserialize(ByteBuffer byteBuffer) {
    Filter timeFilter = null;
    if (ReadWriteIOUtils.readByte(byteBuffer) == 1) {
      timeFilter = FilterFactory.deserialize(byteBuffer);
    }
    OrderByParameter mergeOrderParameter = OrderByParameter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryNode(planNodeId, timeFilter, mergeOrderParameter);
  }

  @Override
  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  @Nullable
  public Filter getTimeFilter() {
    return timeFilter;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  public void setMergeOrderParameter(OrderByParameter mergeOrderParameter) {
    this.mergeOrderParameter = mergeOrderParameter;
  }
}
