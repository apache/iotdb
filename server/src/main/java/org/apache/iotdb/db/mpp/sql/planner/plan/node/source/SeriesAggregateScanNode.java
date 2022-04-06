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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;

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
  private PartialPath seriesPath;
  private List<AggregationType> aggregateFuncList;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private OrderBy scanOrder = OrderBy.TIMESTAMP_ASC;

  private Filter filter;

  private String columnName;

  // The id of DataRegion where the node will run
  private RegionReplicaSet regionReplicaSet;

  public SeriesAggregateScanNode(PlanNodeId id) {
    super(id);
  }

  public SeriesAggregateScanNode(
      PlanNodeId id,
      PartialPath seriesPath,
      List<AggregationType> aggregateFuncList,
      OrderBy scanOrder) {
    this(id);
    this.seriesPath = seriesPath;
    this.aggregateFuncList = aggregateFuncList;
    this.scanOrder = scanOrder;
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
    return seriesPath.getDevice();
  }

  @Override
  protected String getExpressionString() {
    return getAggregateFuncList().toString();
  }

  @Override
  public void close() throws Exception {}

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesAggregate(this, context);
  }

  public static SeriesAggregateScanNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  // This method is used when do the PredicatePushDown.
  // The filter is not put in the constructor because the filter is only clear in the predicate
  // push-down stage
  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public PartialPath getSeriesPath() {
    return seriesPath;
  }

  public List<AggregationType> getAggregateFuncList() {
    return aggregateFuncList;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[SeriesAggregateScanNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add("AggregateFunctions: " + this.getExpressionString());
    return new Pair<>(title, attributes);
  }
}
