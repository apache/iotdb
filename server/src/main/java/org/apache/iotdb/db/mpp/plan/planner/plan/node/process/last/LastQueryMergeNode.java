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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class LastQueryMergeNode extends MultiChildProcessNode {

  // The result output order, which could sort by sensor and time.
  // The size of this list is 2 and the first SortItem in this list has higher priority.
  private final OrderByParameter mergeOrderParameter;

  public LastQueryMergeNode(PlanNodeId id, OrderByParameter mergeOrderParameter) {
    super(id);
    this.mergeOrderParameter = mergeOrderParameter;
  }

  public LastQueryMergeNode(
      PlanNodeId id, List<PlanNode> children, OrderByParameter mergeOrderParameter) {
    super(id, children);
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
    return new LastQueryMergeNode(getPlanNodeId(), mergeOrderParameter);
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
    return String.format(
        "LastQueryMergeNode-%s:[OrderByParameter: %s]", this.getPlanNodeId(), mergeOrderParameter);
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
    LastQueryMergeNode that = (LastQueryMergeNode) o;
    return mergeOrderParameter.equals(that.mergeOrderParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrderParameter);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLastQueryMerge(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY_MERGE.serialize(byteBuffer);
    mergeOrderParameter.serializeAttributes(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY_MERGE.serialize(stream);
    mergeOrderParameter.serializeAttributes(stream);
  }

  public static LastQueryMergeNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter mergeOrderParameter = OrderByParameter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryMergeNode(planNodeId, mergeOrderParameter);
  }

  @Override
  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }
}
