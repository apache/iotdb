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
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.InputLocation;
import org.apache.iotdb.db.mpp.sql.planner.plan.OutputColumn;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This node is responsible for join two or more TsBlock. The join algorithm is like outer join by
 * timestamp column. It will join two or more TsBlock by Timestamp column. The output result of
 * TimeJoinOperator is sorted by timestamp
 */
// TODO: define the TimeJoinMergeNode for distributed plan
public class TimeJoinNode extends ProcessNode {

  // This parameter indicates the order when executing multiway merge sort.
  private final OrderBy mergeOrder;

  // The policy to decide whether a row should be discarded
  // The without policy is able to be push down to the TimeJoinOperator because we can know whether
  // a row contains
  // null or not.
  private FilterNullPolicy filterNullPolicy = FilterNullPolicy.NO_FILTER;

  // indicate each output column should use which value column of which input TsBlock and the
  // overlapped situation
  private List<OutputColumn> outputColumns = new ArrayList<>();

  private List<PlanNode> children;

  public TimeJoinNode(PlanNodeId id, OrderBy mergeOrder) {
    super(id);
    this.mergeOrder = mergeOrder;
    this.children = new ArrayList<>();
  }

  public TimeJoinNode(PlanNodeId id, OrderBy mergeOrder, List<PlanNode> children) {
    this(id, mergeOrder);
    this.children = children;
    initOutputColumns();
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public PlanNode clone() {
    // TODO: (xingtanzjr)
    TimeJoinNode cloneNode = new TimeJoinNode(getPlanNodeId(), this.mergeOrder);
    cloneNode.outputColumns = this.outputColumns;
    return cloneNode;
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  private void initOutputColumns() {
    for (int tsBlockIndex = 0; tsBlockIndex < children.size(); tsBlockIndex++) {
      List<ColumnHeader> childColumnHeaders = children.get(tsBlockIndex).getOutputColumnHeaders();
      for (int valueColumnIndex = 0;
          valueColumnIndex < childColumnHeaders.size();
          valueColumnIndex++) {
        ColumnHeader columnHeader = childColumnHeaders.get(valueColumnIndex);
        InputLocation inputLocation = new InputLocation(tsBlockIndex, valueColumnIndex);
        outputColumns.add(new OutputColumn(columnHeader, inputLocation));
      }
    }
  }

  @Override
  public List<OutputColumn> getOutputColumns() {
    return outputColumns;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return outputColumns.stream().map(OutputColumn::getColumnHeader).collect(Collectors.toList());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumns.stream()
        .map(OutputColumn::getColumnHeader)
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return outputColumns.stream()
        .map(OutputColumn::getColumnHeader)
        .map(ColumnHeader::getColumnType)
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTimeJoin(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIME_JOIN.serialize(byteBuffer);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(filterNullPolicy.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(outputColumns.size(), byteBuffer);
    for (OutputColumn outputColumn : outputColumns) {
      outputColumn.serialize(byteBuffer);
    }
  }

  public static TimeJoinNode deserialize(ByteBuffer byteBuffer) {
    OrderBy orderBy = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    FilterNullPolicy filterNullPolicy =
        FilterNullPolicy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    int outputColumnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<OutputColumn> outputColumns = new ArrayList<>();
    for (int i = 0; i < outputColumnSize; i++) {
      outputColumns.add(OutputColumn.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    TimeJoinNode timeJoinNode = new TimeJoinNode(planNodeId, orderBy);
    timeJoinNode.outputColumns.addAll(outputColumns);
    timeJoinNode.filterNullPolicy = filterNullPolicy;

    return timeJoinNode;
  }

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  public OrderBy getMergeOrder() {
    return mergeOrder;
  }

  public FilterNullPolicy getFilterNullPolicy() {
    return filterNullPolicy;
  }

  public void setWithoutPolicy(FilterNullPolicy filterNullPolicy) {
    this.filterNullPolicy = filterNullPolicy;
  }

  public String toString() {
    return "TimeJoinNode-" + this.getPlanNodeId();
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[TimeJoinNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("MergeOrder: " + (this.getMergeOrder() == null ? "null" : this.getMergeOrder()));
    attributes.add(
        "FilterNullPolicy: "
            + (this.getFilterNullPolicy() == null ? "null" : this.getFilterNullPolicy()));
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

    TimeJoinNode that = (TimeJoinNode) o;
    return mergeOrder == that.mergeOrder
        && filterNullPolicy == that.filterNullPolicy
        && Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mergeOrder, filterNullPolicy, children);
  }
}
