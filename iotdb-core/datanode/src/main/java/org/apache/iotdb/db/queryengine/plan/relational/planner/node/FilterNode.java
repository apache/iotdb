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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FilterNode extends SingleChildProcessNode {
  private Expression predicate;

  public FilterNode(PlanNodeId id, PlanNode child, Expression predicate) {
    super(id, child);
    this.predicate = predicate;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  public PlanNode clone() {
    return new FilterNode(id, null, predicate);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_FILTER_NODE.serialize(byteBuffer);
    Expression.serialize(predicate, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_FILTER_NODE.serialize(stream);
    Expression.serialize(predicate, stream);
  }

  public static FilterNode deserialize(ByteBuffer byteBuffer) {
    Expression predicate = Expression.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FilterNode(planNodeId, null, predicate);
  }

  public Expression getPredicate() {
    return predicate;
  }

  public void setPredicate(Expression predicate) {
    this.predicate = predicate;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return child.getOutputSymbols();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new FilterNode(id, Iterables.getOnlyElement(newChildren), predicate);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    FilterNode filterNode = (FilterNode) o;
    return Objects.equal(predicate, filterNode.predicate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), predicate);
  }

  @Override
  public String toString() {
    return "FilterNode-" + this.getPlanNodeId();
  }
}
