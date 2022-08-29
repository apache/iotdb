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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Objects;

public class FilterNode extends TransformNode {

  private final Expression predicate;

  public FilterNode(
      PlanNodeId id,
      PlanNode childPlanNode,
      Expression[] outputExpressions,
      Expression predicate,
      boolean keepNull,
      ZoneId zoneId,
      Ordering scanOrder) {
    super(id, childPlanNode, outputExpressions, keepNull, zoneId, scanOrder);
    this.predicate = predicate;
  }

  public FilterNode(
      PlanNodeId id,
      Expression[] outputExpressions,
      Expression predicate,
      boolean keepNull,
      ZoneId zoneId,
      Ordering scanOrder) {
    super(id, outputExpressions, keepNull, zoneId, scanOrder);
    this.predicate = predicate;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  public PlanNode clone() {
    return new FilterNode(
        getPlanNodeId(), outputExpressions, predicate, keepNull, zoneId, scanOrder);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILTER.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputExpressions.length, byteBuffer);
    for (Expression expression : outputExpressions) {
      Expression.serialize(expression, byteBuffer);
    }
    Expression.serialize(predicate, byteBuffer);
    ReadWriteIOUtils.write(keepNull, byteBuffer);
    ReadWriteIOUtils.write(zoneId.getId(), byteBuffer);
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FILTER.serialize(stream);
    ReadWriteIOUtils.write(outputExpressions.length, stream);
    for (Expression expression : outputExpressions) {
      Expression.serialize(expression, stream);
    }
    Expression.serialize(predicate, stream);
    ReadWriteIOUtils.write(keepNull, stream);
    ReadWriteIOUtils.write(zoneId.getId(), stream);
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
  }

  public static FilterNode deserialize(ByteBuffer byteBuffer) {
    int outputExpressionsLength = ReadWriteIOUtils.readInt(byteBuffer);
    Expression[] outputExpressions = new Expression[outputExpressionsLength];
    for (int i = 0; i < outputExpressionsLength; ++i) {
      outputExpressions[i] = Expression.deserialize(byteBuffer);
    }
    Expression predicate = Expression.deserialize(byteBuffer);
    boolean keepNull = ReadWriteIOUtils.readBool(byteBuffer);
    ZoneId zoneId = ZoneId.of(Objects.requireNonNull(ReadWriteIOUtils.readString(byteBuffer)));
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FilterNode(planNodeId, outputExpressions, predicate, keepNull, zoneId, scanOrder);
  }

  public Expression getPredicate() {
    return predicate;
  }

  @Override
  public String toString() {
    return "FilterNode-" + this.getPlanNodeId();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FilterNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    FilterNode that = (FilterNode) o;
    return predicate.equals(that.predicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), predicate);
  }
}
