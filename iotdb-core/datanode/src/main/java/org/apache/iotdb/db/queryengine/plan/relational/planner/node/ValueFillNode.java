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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;

import com.google.common.collect.Iterables;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class ValueFillNode extends FillNode {
  private final Literal filledValue;

  public ValueFillNode(PlanNodeId id, PlanNode child, Literal filledValue) {
    super(id, child);
    this.filledValue = filledValue;
  }

  public Literal getFilledValue() {
    return filledValue;
  }

  @Override
  public PlanNode clone() {
    return new ValueFillNode(id, null, filledValue);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitValueFill(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_VALUE_FILL_NODE.serialize(byteBuffer);
    Expression.serialize(filledValue, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_VALUE_FILL_NODE.serialize(stream);
    Expression.serialize(filledValue, stream);
  }

  public static ValueFillNode deserialize(ByteBuffer byteBuffer) {
    Literal filledValue = (Literal) Expression.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ValueFillNode(planNodeId, null, filledValue);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new ValueFillNode(id, Iterables.getOnlyElement(newChildren), filledValue);
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
    ValueFillNode that = (ValueFillNode) o;
    return Objects.equals(filledValue, that.filledValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), filledValue);
  }

  @Override
  public String toString() {
    return "ValueFillNode-" + this.getPlanNodeId();
  }
}
