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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.FilterNullParameter;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.OutputColumn;
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
  private FilterNullParameter filterNullParameter;

  // indicate each output column should use which value column of which input TsBlock and the
  // overlapped situation
  // size of outputColumns must be equal to the size of columnHeaders
  private List<OutputColumn> outputColumns = new ArrayList<>();

  // column name and datatype of each output column
  private List<ColumnHeader> outputColumnHeaders = new ArrayList<>();

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
    TimeJoinNode node = new TimeJoinNode(getPlanNodeId(), this.mergeOrder);
    node.outputColumnHeaders = this.outputColumnHeaders;
    node.outputColumns = this.outputColumns;
    return node;
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  public List<OutputColumn> getOutputColumns() {
    return outputColumns;
  }

  public void initOutputColumns() {
    outputColumns.clear();
    outputColumnHeaders.clear();
    for (int tsBlockIndex = 0; tsBlockIndex < children.size(); tsBlockIndex++) {
      List<ColumnHeader> childColumnHeaders = children.get(tsBlockIndex).getOutputColumnHeaders();
      for (int valueColumnIndex = 0;
          valueColumnIndex < childColumnHeaders.size();
          valueColumnIndex++) {
        InputLocation inputLocation = new InputLocation(tsBlockIndex, valueColumnIndex);
        outputColumns.add(new OutputColumn(inputLocation));
      }
      outputColumnHeaders.addAll(childColumnHeaders);
    }
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return outputColumnHeaders;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return outputColumnHeaders.stream()
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
    if (filterNullParameter == null) {
      ReadWriteIOUtils.write(true, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
      filterNullParameter.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(outputColumns.size(), byteBuffer);
    for (OutputColumn outputColumn : outputColumns) {
      outputColumn.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(outputColumnHeaders.size(), byteBuffer);
    for (ColumnHeader columnHeader : outputColumnHeaders) {
      columnHeader.serialize(byteBuffer);
    }
  }

  public static TimeJoinNode deserialize(ByteBuffer byteBuffer) {
    OrderBy orderBy = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    FilterNullParameter filterNullParameter = null;
    if (!ReadWriteIOUtils.readIsNull(byteBuffer)) {
      filterNullParameter = FilterNullParameter.deserialize(byteBuffer);
    }
    int outputColumnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<OutputColumn> outputColumns = new ArrayList<>(outputColumnSize);
    for (int i = 0; i < outputColumnSize; i++) {
      outputColumns.add(OutputColumn.deserialize(byteBuffer));
    }
    int outputColumnHeadersSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<ColumnHeader> outputColumnHeaders = new ArrayList<>(outputColumnHeadersSize);
    for (int i = 0; i < outputColumnHeadersSize; i++) {
      outputColumnHeaders.add(ColumnHeader.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    TimeJoinNode timeJoinNode = new TimeJoinNode(planNodeId, orderBy);
    timeJoinNode.outputColumns.addAll(outputColumns);
    timeJoinNode.outputColumnHeaders.addAll(outputColumnHeaders);
    timeJoinNode.setFilterNullParameter(filterNullParameter);

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

  public FilterNullParameter getFilterNullParameter() {
    return filterNullParameter;
  }

  public void setFilterNullParameter(FilterNullParameter filterNullParameter) {
    this.filterNullParameter = filterNullParameter;
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
            + (this.getFilterNullParameter() == null
                ? "null"
                : this.getFilterNullParameter().getFilterNullPolicy()));
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
        && Objects.equals(filterNullParameter, that.filterNullParameter)
        && Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mergeOrder, filterNullParameter, children);
  }
}
