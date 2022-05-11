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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TransformNode extends ProcessNode {

  protected PlanNode childPlanNode;

  protected final Expression[] outputExpressions;
  protected final boolean keepNull;
  protected final ZoneId zoneId;

  private List<String> outputColumnNames;

  public TransformNode(
      PlanNodeId id,
      PlanNode childPlanNode,
      Expression[] outputExpressions,
      boolean keepNull,
      ZoneId zoneId) {
    super(id);
    this.childPlanNode = childPlanNode;
    this.outputExpressions = outputExpressions;
    this.keepNull = keepNull;
    this.zoneId = zoneId;
  }

  public TransformNode(
      PlanNodeId id, Expression[] outputExpressions, boolean keepNull, ZoneId zoneId) {
    super(id);
    this.outputExpressions = outputExpressions;
    this.keepNull = keepNull;
    this.zoneId = zoneId;
  }

  @Override
  public final List<PlanNode> getChildren() {
    return ImmutableList.of(childPlanNode);
  }

  @Override
  public final void addChild(PlanNode childPlanNode) {
    this.childPlanNode = childPlanNode;
  }

  @Override
  public final int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public final List<String> getOutputColumnNames() {
    if (outputColumnNames == null) {
      outputColumnNames = new ArrayList<>();
      for (Expression expression : outputExpressions) {
        outputColumnNames.add(expression.toString());
      }
    }
    return outputColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTransform(this, context);
  }

  @Override
  public PlanNode clone() {
    return new TransformNode(getPlanNodeId(), outputExpressions, keepNull, zoneId);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TRANSFORM.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputExpressions.length, byteBuffer);
    for (Expression expression : outputExpressions) {
      Expression.serialize(expression, byteBuffer);
    }
    ReadWriteIOUtils.write(keepNull, byteBuffer);
    ReadWriteIOUtils.write(zoneId.getId(), byteBuffer);
  }

  public static TransformNode deserialize(ByteBuffer byteBuffer) {
    int outputExpressionsLength = ReadWriteIOUtils.readInt(byteBuffer);
    Expression[] outputExpressions = new Expression[outputExpressionsLength];
    for (int i = 0; i < outputExpressionsLength; ++i) {
      outputExpressions[i] = Expression.deserialize(byteBuffer);
    }
    boolean keepNull = ReadWriteIOUtils.readBool(byteBuffer);
    ZoneId zoneId = ZoneId.of(Objects.requireNonNull(ReadWriteIOUtils.readString(byteBuffer)));
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new TransformNode(planNodeId, outputExpressions, keepNull, zoneId);
  }

  public final Expression[] getOutputExpressions() {
    return outputExpressions;
  }

  public final boolean isKeepNull() {
    return keepNull;
  }

  public final ZoneId getZoneId() {
    return zoneId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TransformNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TransformNode that = (TransformNode) o;
    if (!Arrays.equals(outputExpressions, that.outputExpressions)) {
      return false;
    }
    if (!childPlanNode.equals(that.childPlanNode)) {
      return false;
    }
    return keepNull == that.keepNull && zoneId.equals(that.zoneId);
    //    return keepNull == that.keepNull
    //        && childPlanNode.equals(that.childPlanNode)
    //        && Arrays.equals(outputExpressions, that.outputExpressions)
    //        && zoneId.equals(that.zoneId);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), childPlanNode, keepNull, zoneId);
    result = 31 * result + Arrays.hashCode(outputExpressions);
    return result;
  }
}
