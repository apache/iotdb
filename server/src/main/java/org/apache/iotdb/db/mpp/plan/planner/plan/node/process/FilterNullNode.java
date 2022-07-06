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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FilterNullParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.FilterNullPolicy;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** FilterNullNode is used to discard specific rows from upstream node. */
public class FilterNullNode extends ProcessNode {

  private final FilterNullParameter filterNullParameter;

  private PlanNode child;

  public FilterNullNode(PlanNodeId id, FilterNullParameter filterNullParameter) {
    super(id);
    this.filterNullParameter = filterNullParameter;
  }

  public FilterNullNode(
      PlanNodeId id, FilterNullPolicy filterNullPolicy, List<Expression> filterNullColumns) {
    super(id);
    this.filterNullParameter = new FilterNullParameter(filterNullPolicy, filterNullColumns);
  }

  public FilterNullNode(
      PlanNodeId id,
      PlanNode child,
      FilterNullPolicy filterNullPolicy,
      List<Expression> filterNullColumns) {
    this(id, filterNullPolicy, filterNullColumns);
    this.child = child;
  }

  public FilterNullNode(PlanNodeId id, PlanNode child, FilterNullParameter filterNullParameter) {
    this(id, filterNullParameter);
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
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public PlanNode clone() {
    return new FilterNullNode(getPlanNodeId(), filterNullParameter);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilterNull(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILTER_NULL.serialize(byteBuffer);
    filterNullParameter.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FILTER_NULL.serialize(stream);
    filterNullParameter.serialize(stream);
  }

  public static FilterNullNode deserialize(ByteBuffer byteBuffer) {
    FilterNullParameter filterNullParameter = FilterNullParameter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FilterNullNode(planNodeId, filterNullParameter);
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
    FilterNullNode that = (FilterNullNode) o;
    return filterNullParameter.equals(that.filterNullParameter) && child.equals(that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), filterNullParameter, child);
  }
}
