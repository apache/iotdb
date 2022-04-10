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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.sql.planner.plan.IOutputPlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** WithoutNode is used to discard specific rows from upstream node. */
public class FilterNullNode extends ProcessNode implements IOutputPlanNode {

  // The policy to discard the result from upstream operator
  private final FilterNullPolicy discardPolicy;

  private final List<String> filterNullColumnNames;

  private PlanNode child;

  public FilterNullNode(
      PlanNodeId id, FilterNullPolicy policy, List<String> filterNullColumnNames) {
    super(id);
    this.discardPolicy = policy;
    this.filterNullColumnNames = filterNullColumnNames;
  }

  public FilterNullNode(
      PlanNodeId id,
      PlanNode child,
      FilterNullPolicy discardPolicy,
      List<String> filterNullColumnNames) {
    this(id, discardPolicy, filterNullColumnNames);
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
  public PlanNode clone() {
    return new FilterNullNode(getPlanNodeId(), discardPolicy, filterNullColumnNames);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return ((IOutputPlanNode) child).getOutputColumnHeaders();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ((IOutputPlanNode) child).getOutputColumnNames();
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return ((IOutputPlanNode) child).getOutputColumnTypes();
  }

  public FilterNullPolicy getDiscardPolicy() {
    return discardPolicy;
  }

  public List<String> getFilterNullColumnNames() {
    return filterNullColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilterNull(this, context);
  }

  public static FilterNullNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[FilterNullNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("FilterNullPolicy: " + this.getDiscardPolicy());
    attributes.add("FilterNullColumnNames: " + this.getFilterNullColumnNames());
    return new Pair<>(title, attributes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FilterNullNode that = (FilterNullNode) o;
    return discardPolicy == that.discardPolicy
        && Objects.equals(child, that.child)
        && Objects.equals(filterNullColumnNames, that.filterNullColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(discardPolicy, child, filterNullColumnNames);
  }
}
