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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** The FilterNode is responsible to filter the RowRecord from TsBlock. */
public class FilterNode extends ProcessNode implements IOutputPlanNode {

  private PlanNode child;

  private final IExpression predicate;

  private List<ColumnHeader> columnHeaders;

  public FilterNode(PlanNodeId id, IExpression predicate) {
    super(id);
    this.predicate = predicate;
  }

  public FilterNode(
      PlanNodeId id, PlanNode child, IExpression predicate, List<String> outputColumnNames) {
    this(id, predicate);
    this.child = child;
    this.columnHeaders =
        ((IOutputPlanNode) child)
            .getOutputColumnHeaders().stream()
                .filter(columnHeader -> outputColumnNames.contains(columnHeader.getColumnName()))
                .collect(Collectors.toList());
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
    return new FilterNode(getPlanNodeId(), predicate);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return columnHeaders;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return columnHeaders.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return columnHeaders.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  public static FilterNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  public IExpression getPredicate() {
    return predicate;
  }

  public PlanNode getChild() {
    return child;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[FilterNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("QueryFilter: " + this.getPredicate());
    attributes.add("outputColumnNames: " + this.getOutputColumnNames());
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

    FilterNode that = (FilterNode) o;
    return Objects.equals(child, that.child)
        && Objects.equals(predicate, that.predicate)
        && Objects.equals(columnHeaders, that.columnHeaders);
  }

  @Override
  public int hashCode() {
    return Objects.hash(child, predicate, columnHeaders);
  }
}
