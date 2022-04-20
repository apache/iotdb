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
import org.apache.iotdb.db.mpp.common.FilterNullParameter;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.InputLocation;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** FilterNullNode is used to discard specific rows from upstream node. */
public class FilterNullNode extends ProcessNode {

  FilterNullParameter filterNullParameter;

  private PlanNode child;

  public FilterNullNode(PlanNodeId id, FilterNullParameter filterNullParameter) {
    super(id);
    this.filterNullParameter = filterNullParameter;
  }

  public FilterNullNode(
      PlanNodeId id, FilterNullPolicy filterNullPolicy, List<InputLocation> filterNullColumns) {
    super(id);
    this.filterNullParameter = new FilterNullParameter(filterNullPolicy, filterNullColumns);
  }

  public FilterNullNode(
      PlanNodeId id,
      PlanNode child,
      FilterNullPolicy filterNullPolicy,
      List<InputLocation> filterNullColumns) {
    super(id);
    this.child = child;
    this.filterNullParameter = new FilterNullParameter(filterNullPolicy, filterNullColumns);
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
  public PlanNode clone() {
    return new FilterNullNode(getPlanNodeId(), filterNullParameter);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return child.getOutputColumnHeaders();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return child.getOutputColumnTypes();
  }

  public FilterNullPolicy getDiscardPolicy() {
    return filterNullParameter.getFilterNullPolicy();
  }

  public List<InputLocation> getFilterNullColumns() {
    return filterNullParameter.getFilterNullColumns();
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

  public static FilterNullNode deserialize(ByteBuffer byteBuffer) {
    FilterNullParameter filterNullParameter = FilterNullParameter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FilterNullNode(planNodeId, filterNullParameter);
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[FilterNullNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("FilterNullPolicy: " + this.getDiscardPolicy());
    attributes.add("FilterNullColumns: " + this.getFilterNullColumns());
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
    return Objects.equals(filterNullParameter, that.filterNullParameter)
        && Objects.equals(child, that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(child, filterNullParameter);
  }
}
