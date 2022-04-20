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

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.OutputColumn;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This node is used to aggregate required series from multiple sources. The source data will be
 * input as a TsBlock, it may be raw data or partial aggregation result. This node will output the
 * final series aggregated result represented by TsBlock.
 */
public class AggregateNode extends ProcessNode {

  // The map from columns to corresponding aggregation functions on that column.
  //    KEY: The index of a column in the input {@link TsBlock}.
  //    VALUE: Aggregation functions on this column.
  // (Currently, we only support one series in the aggregation function.)
  @Deprecated private final Map<PartialPath, Set<AggregationType>> aggregateFuncMap;

  // The list of aggregation functions, each FunctionExpression will be output as one column of
  // result TsBlock
  private List<FunctionExpression> aggregateFuncList;

  // The parameter of `group by time`.
  // Its value will be null if there is no `group by time` clause.
  private final GroupByTimeParameter groupByTimeParameter;

  // indicate each output column should use which value column of which input TsBlock and the
  // overlapped situation
  private final List<OutputColumn> outputColumns = new ArrayList<>();

  private PlanNode child;

  public AggregateNode(
      PlanNodeId id,
      PlanNode child,
      Map<PartialPath, Set<AggregationType>> aggregateFuncMap,
      GroupByTimeParameter groupByTimeParameter) {
    super(id);
    this.child = child;
    this.aggregateFuncMap = aggregateFuncMap;
    this.groupByTimeParameter = groupByTimeParameter;
    initOutputColumns();
  }

  private void initOutputColumns() {}

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
    throw new NotImplementedException("Clone of AggregateNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
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
    return visitor.visitRowBasedSeriesAggregate(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.AGGREGATE.serialize(byteBuffer);
    // TODO serialize groupByTimeParameter，because it is unsure
    ReadWriteIOUtils.write(aggregateFuncMap.size(), byteBuffer);
    for (Map.Entry<PartialPath, Set<AggregationType>> e : aggregateFuncMap.entrySet()) {
      e.getKey().serialize(byteBuffer);
      ReadWriteIOUtils.write(e.getValue().size(), byteBuffer);
      for (AggregationType aggregationType : e.getValue()) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        try {
          aggregationType.serializeTo(dataOutputStream);
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
        byteBuffer.put(byteArrayOutputStream.toByteArray());
      }
    }
  }

  public static AggregateNode deserialize(ByteBuffer byteBuffer) {
    // TODO deserialize groupByTimeParameter， because it is unsure
    Map<PartialPath, Set<AggregationType>> aggregateFuncMap = new HashMap<>();
    int mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < mapSize; i++) {
      PartialPath partialPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      int setSize = ReadWriteIOUtils.readInt(byteBuffer);
      Set<AggregationType> aggregationTypes = new HashSet<>();
      for (int j = 0; j < setSize; j++) {
        aggregationTypes.add(AggregationType.deserialize(byteBuffer));
      }
      aggregateFuncMap.put(partialPath, aggregationTypes);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AggregateNode(planNodeId, null, aggregateFuncMap, null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AggregateNode that = (AggregateNode) o;
    return Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && Objects.equals(aggregateFuncMap, that.aggregateFuncMap)
        && Objects.equals(child, that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupByTimeParameter, aggregateFuncMap, child);
  }
}
