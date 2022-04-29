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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.query.expression.Expression;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** The FilterNode is responsible to filter the RowRecord from TsBlock. */
public class FilterNode extends ProcessNode {

  private PlanNode child;

  private final Expression predicate;

  public FilterNode(PlanNodeId id, Expression predicate) {
    super(id);
    this.predicate = predicate;
  }

  public FilterNode(PlanNodeId id, PlanNode child, Expression predicate) {
    this(id, predicate);
    this.child = child;
  }

  public Expression getPredicate() {
    return predicate;
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
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public PlanNode clone() {
    return new FilterNode(getPlanNodeId(), predicate);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILTER.serialize(byteBuffer);
    Expression.serialize(predicate, byteBuffer);
  }

  public static FilterNode deserialize(ByteBuffer byteBuffer) {
    Expression predicate = Expression.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FilterNode(planNodeId, predicate);
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
    FilterNode that = (FilterNode) o;
    return child.equals(that.child) && predicate.equals(that.predicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), child, predicate);
  }
}
