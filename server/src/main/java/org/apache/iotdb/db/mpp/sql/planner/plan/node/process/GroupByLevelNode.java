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
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.OutputColumn;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This node is responsible for the final aggregation merge operation. It will process the data from
 * TsBlock row by row. For one row, it will rollup the fields which have the same aggregate function
 * and belong to one bucket. Here, that two columns belong to one bucket means the partial paths of
 * device after rolling up in specific level are the same.
 *
 * <p>For example, let's say there are two columns `root.sg.d1.s1` and `root.sg.d2.s1`.
 *
 * <p>If the group by level parameter is [0, 1], then these two columns will belong to one bucket
 * and the bucket name is `root.sg.*.s1`.
 *
 * <p>If the group by level parameter is [0, 2], then these two columns will not belong to one
 * bucket. And the total buckets are `root.*.d1.s1` and `root.*.d2.s1`
 */
@Deprecated // TODO: delete later
public class GroupByLevelNode extends ProcessNode {

  private PlanNode child;

  @Deprecated private Map<ColumnHeader, ColumnHeader> groupedPathMap;

  private final int[] groupByLevels;
  // The list of aggregation functions, each aggregation will be output as one column of result
  // TsBlock
  private List<AggregationType> aggregateFuncList = new ArrayList<>();

  // indicate each output column should use which value column of which input TsBlock and the
  // overlapped situation
  private List<OutputColumn> outputColumns = new ArrayList<>();

  // column name and datatype of each output column
  private List<ColumnHeader> outputColumnHeaders;

  public GroupByLevelNode(
      PlanNodeId id,
      PlanNode child,
      int[] groupByLevels,
      Map<ColumnHeader, ColumnHeader> groupedPathMap) {
    super(id);
    this.child = child;
    this.groupByLevels = groupByLevels;
    this.groupedPathMap = groupedPathMap;
    this.outputColumnHeaders =
        groupedPathMap.values().stream().distinct().collect(Collectors.toList());
  }

  public GroupByLevelNode(
      PlanNodeId id,
      PlanNode child,
      int[] groupByLevels,
      List<AggregationType> aggregateFuncList,
      List<OutputColumn> outputColumns) {
    super(id);
    this.child = child;
    this.groupByLevels = groupByLevels;
    this.aggregateFuncList = aggregateFuncList;
    this.outputColumns = outputColumns;
    // TODO: init outputColumnHeaders
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
    throw new NotImplementedException("Clone of GroupByLevelNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  public int[] getGroupByLevels() {
    return groupByLevels;
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
    return visitor.visitGroupByLevel(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.GROUP_BY_LEVEL.serialize(byteBuffer);
    ReadWriteIOUtils.write(groupByLevels.length, byteBuffer);
    for (int level : groupByLevels) {
      ReadWriteIOUtils.write(level, byteBuffer);
    }
    ReadWriteIOUtils.write(aggregateFuncList.size(), byteBuffer);
    for (AggregationType aggregationType : aggregateFuncList) {
      ReadWriteIOUtils.write(aggregationType.ordinal(), byteBuffer);
    }
    ReadWriteIOUtils.write(outputColumns.size(), byteBuffer);
    for (OutputColumn outputColumn : outputColumns) {
      outputColumn.serialize(byteBuffer);
    }
  }

  public static GroupByLevelNode deserialize(ByteBuffer byteBuffer) {
    int groupByLevelSize = ReadWriteIOUtils.readInt(byteBuffer);
    int[] groupByLevels = new int[groupByLevelSize];
    for (int i = 0; i < groupByLevelSize; i++) {
      groupByLevels[i] = ReadWriteIOUtils.readInt(byteBuffer);
    }
    int aggregateFuncListSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<AggregationType> aggregateFuncList = new ArrayList<>(aggregateFuncListSize);
    for (int i = 0; i < aggregateFuncListSize; i++) {
      aggregateFuncList.add(AggregationType.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
    }
    int outputColumnsSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<OutputColumn> outputColumns = new ArrayList<>(outputColumnsSize);
    for (int i = 0; i < outputColumnsSize; i++) {
      outputColumns.add(OutputColumn.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new GroupByLevelNode(planNodeId, null, groupByLevels, aggregateFuncList, outputColumns);
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[GroupByLevelNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("GroupByLevels: " + Arrays.toString(this.getGroupByLevels()));
    attributes.add("ColumnNames: " + this.getOutputColumnNames());
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

    GroupByLevelNode that = (GroupByLevelNode) o;
    return Objects.equals(child, that.child)
        && Arrays.equals(groupByLevels, that.groupByLevels)
        && Objects.equals(aggregateFuncList, that.aggregateFuncList)
        && Objects.equals(outputColumns, that.outputColumns);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), aggregateFuncList, outputColumns);
    result = 31 * result + Arrays.hashCode(groupByLevels);
    return result;
  }
}
