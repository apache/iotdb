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
package org.apache.iotdb.db.mpp.plan.planner.plan.node;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.ChildNodesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.ChildPathsSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByTimeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;

public abstract class PlanVisitor<R, C> {

  public R process(PlanNode node, C context) {
    return node.accept(this, context);
  }

  public abstract R visitPlan(PlanNode node, C context);

  public R visitSeriesScan(SeriesScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSeriesAggregationScan(SeriesAggregationScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitAlignedSeriesScan(AlignedSeriesScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitAlignedSeriesAggregationScan(AlignedSeriesAggregationScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeviceView(DeviceViewNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeviceMerge(DeviceMergeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFill(FillNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFilter(FilterNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFilterNull(FilterNullNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitGroupByLevel(GroupByLevelNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitGroupByTime(GroupByTimeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLimit(LimitNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitOffset(OffsetNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRowBasedSeriesAggregate(AggregationNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSort(SortNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitProject(ProjectNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTimeJoin(TimeJoinNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitExchange(ExchangeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSchemaQueryMerge(SchemaQueryMergeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSchemaQueryScan(SchemaQueryScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTimeSeriesSchemaScan(TimeSeriesSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDevicesSchemaScan(DevicesSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDevicesCount(DevicesCountNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTimeSeriesCount(TimeSeriesCountNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLevelTimeSeriesCount(LevelTimeSeriesCountNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitCountMerge(CountSchemaMergeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFragmentSink(FragmentSinkNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitCreateTimeSeries(CreateTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSchemaFetchMerge(SchemaFetchMergeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSchemaFetchScan(SchemaFetchScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitCreateAlignedTimeSeries(CreateAlignedTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitCreateMultiTimeSeries(CreateMultiTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitAlterTimeSeries(AlterTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTransform(TransformNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeleteRegion(DeleteRegionNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInsertRow(InsertRowNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInsertTablet(InsertTabletNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInsertRows(InsertRowsNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInsertMultiTablets(InsertMultiTabletsNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInsertRowsOfOneDevice(InsertRowsOfOneDeviceNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitChildPathsSchemaScan(ChildPathsSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitChildNodesSchemaScan(ChildNodesSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitNodeManagementMemoryMerge(NodeManagementMemoryMergeNode node, C context) {
    return visitPlan(node, context);
  }
}
