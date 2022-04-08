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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.source;

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.query.expression.ExpressionType;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * SeriesAggregateOperator is responsible to do the aggregation calculation for one series. It will
 * read the target series and calculate the aggregation result by the aggregation digest or raw data
 * of this series.
 *
 * <p>The aggregation result will be represented as a TsBlock
 *
 * <p>This operator will split data of the target series into many groups by time range and do the
 * aggregation calculation for each group. Each result will be one row of the result TsBlock. The
 * timestamp of each row is the start time of the time range group.
 *
 * <p>If there is no time range split parameter, the result TsBlock will only contain one row, which
 * represent the whole aggregation result of this series. And the timestamp will be 0, which is
 * meaningless.
 */
public class SeriesAggregateScanNode extends SourceNode {

  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  private GroupByTimeParameter groupByTimeParameter;

  // The aggregation function, which contains the function name and related series.
  // (Currently we only support one series in the aggregation function)
  // TODO: need consider whether it is suitable the aggregation function using FunctionExpression
  private FunctionExpression aggregateFunc;

  private Filter filter;

  private String columnName;

  // The id of DataRegion where the node will run
  private RegionReplicaSet regionReplicaSet;

  public SeriesAggregateScanNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of SeriesAggregateScanNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of(columnName);
  }

  public SeriesAggregateScanNode(PlanNodeId id, FunctionExpression aggregateFunc) {
    this(id);
    this.aggregateFunc = aggregateFunc;
  }

  public SeriesAggregateScanNode(
      PlanNodeId id, FunctionExpression aggregateFunc, GroupByTimeParameter groupByTimeParameter) {
    this(id, aggregateFunc);
    this.groupByTimeParameter = groupByTimeParameter;
  }

  @Override
  public void open() throws Exception {}

  @Override
  public RegionReplicaSet getDataRegionReplicaSet() {
    return this.regionReplicaSet;
  }

  @Override
  public void setDataRegionReplicaSet(RegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public String getDeviceName() {
    return aggregateFunc.getPaths().get(0).getDevice();
  }

  @Override
  protected String getExpressionString() {
    return aggregateFunc.getExpressionString();
  }

  @Override
  public void close() throws Exception {}

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesAggregate(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SERIES_AGGREGATE_SCAN.serialize(byteBuffer);
    // TODO serialize groupByTimeParameter
    aggregateFunc.serialize(byteBuffer); //  aggregateFunc to be consider
    filter.serialize(byteBuffer);
    ReadWriteIOUtils.write(columnName, byteBuffer);
    regionReplicaSet.serializeImpl(byteBuffer);
  }

  public static SeriesAggregateScanNode deserialize(ByteBuffer byteBuffer) {
    // TODO serialize groupByTimeParameter

    //  aggregateFunc to be consider
    FunctionExpression functionExpression =
        (FunctionExpression) ExpressionType.deserialize(byteBuffer);
    Filter filter = FilterFactory.deserialize(byteBuffer);
    String columnName = ReadWriteIOUtils.readString(byteBuffer);
    RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
    try {
      regionReplicaSet.deserializeImpl(byteBuffer);
    } catch (IOException e) {
      e.printStackTrace();
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    SeriesAggregateScanNode seriesAggregateScanNode = new SeriesAggregateScanNode(planNodeId);
    seriesAggregateScanNode.columnName = columnName;
    seriesAggregateScanNode.regionReplicaSet = regionReplicaSet;
    seriesAggregateScanNode.filter = filter;
    seriesAggregateScanNode.aggregateFunc = functionExpression;
    return seriesAggregateScanNode;
  }

  // This method is used when do the PredicatePushDown.
  // The filter is not put in the constructor because the filter is only clear in the predicate
  // push-down stage
  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[SeriesAggregateScanNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add("AggregateFunction: " + this.getExpressionString());
    return new Pair<>(title, attributes);
  }
}
