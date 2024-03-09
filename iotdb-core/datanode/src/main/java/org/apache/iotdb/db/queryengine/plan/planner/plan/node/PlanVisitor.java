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
package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodePathsConvertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodePathsCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.PreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.RollbackPreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.RollbackSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.ConstructLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.DeleteLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.RollbackLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedConfigSchemaNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWriteSchemaNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationMergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryTransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;

@SuppressWarnings("java:S6539") // suppress "Monster class" warning
public abstract class PlanVisitor<R, C> {

  public R process(PlanNode node, C context) {
    return node.accept(this, context);
  }

  public abstract R visitPlan(PlanNode node, C context);

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Query Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // source --------------------------------------------------------------------------------------

  public R visitSourceNode(SourceNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSeriesScanSource(SeriesScanSourceNode node, C context) {
    return visitSourceNode(node, context);
  }

  public R visitSeriesScan(SeriesScanNode node, C context) {
    return visitSeriesScanSource(node, context);
  }

  public R visitAlignedSeriesScan(AlignedSeriesScanNode node, C context) {
    return visitSeriesScanSource(node, context);
  }

  public R visitSeriesAggregationSourceNode(SeriesAggregationSourceNode node, C context) {
    return visitSourceNode(node, context);
  }

  public R visitSeriesAggregationScan(SeriesAggregationScanNode node, C context) {
    return visitSeriesAggregationSourceNode(node, context);
  }

  public R visitAlignedSeriesAggregationScan(AlignedSeriesAggregationScanNode node, C context) {
    return visitSeriesAggregationSourceNode(node, context);
  }

  public R visitLastQueryScan(LastQueryScanNode node, C context) {
    return visitSourceNode(node, context);
  }

  public R visitAlignedLastQueryScan(AlignedLastQueryScanNode node, C context) {
    return visitSourceNode(node, context);
  }

  // single child --------------------------------------------------------------------------------

  public R visitSingleChildProcess(SingleChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFill(FillNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitFilter(FilterNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitSlidingWindowAggregation(SlidingWindowAggregationNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitLimit(LimitNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitOffset(OffsetNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitSort(SortNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitProject(ProjectNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitExchange(ExchangeNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitTransform(TransformNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitInto(IntoNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitDeviceViewInto(DeviceViewIntoNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitColumnInject(ColumnInjectNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitSingleDeviceView(SingleDeviceViewNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  // two child -----------------------------------------------------------------------------------

  public R visitTwoChildProcess(TwoChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLeftOuterTimeJoin(LeftOuterTimeJoinNode node, C context) {
    return visitTwoChildProcess(node, context);
  }

  // multi child --------------------------------------------------------------------------------

  public R visitMultiChildProcess(MultiChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeviceView(DeviceViewNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitAggregationMergeSort(AggregationMergeSortNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitDeviceMerge(DeviceMergeNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitGroupByLevel(GroupByLevelNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitGroupByTag(GroupByTagNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitAggregation(AggregationNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitFullOuterTimeJoin(FullOuterTimeJoinNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitInnerTimeJoin(InnerTimeJoinNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitLastQuery(LastQueryNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitLastQueryMerge(LastQueryMergeNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitLastQueryCollect(LastQueryCollectNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitLastQueryTransform(LastQueryTransformNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitMergeSort(MergeSortNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitTopK(TopKNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitHorizontallyConcat(HorizontallyConcatNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  // others -----------------------------------------------------------------------------------

  public R visitShowQueries(ShowQueriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitIdentitySink(IdentitySinkNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitShuffleSink(ShuffleSinkNode node, C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Schema Write & Query Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public R visitSchemaQueryMerge(SchemaQueryMergeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSchemaQueryScan(SchemaQueryScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSchemaQueryOrderByHeat(SchemaQueryOrderByHeatNode node, C context) {
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

  public R visitInternalCreateTimeSeries(InternalCreateTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitActivateTemplate(ActivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPreDeactivateTemplate(PreDeactivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRollbackPreDeactivateTemplate(RollbackPreDeactivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeactivateTemplate(DeactivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInternalBatchActivateTemplate(InternalBatchActivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInternalCreateMultiTimeSeries(InternalCreateMultiTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitNodePathsSchemaScan(NodePathsSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitNodeManagementMemoryMerge(NodeManagementMemoryMergeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitNodePathConvert(NodePathsConvertNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitNodePathsCount(NodePathsCountNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeleteTimeseries(DeleteTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitConstructSchemaBlackList(ConstructSchemaBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRollbackSchemaBlackList(RollbackSchemaBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitBatchActivateTemplate(BatchActivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitCreateLogicalView(CreateLogicalViewNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitConstructLogicalViewBlackList(ConstructLogicalViewBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRollbackLogicalViewBlackList(RollbackLogicalViewBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeleteLogicalView(DeleteLogicalViewNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitAlterLogicalView(AlterLogicalViewNode node, C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Write Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

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

  public R visitDeleteData(DeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Pipe Enriched Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public R visitPipeEnrichedInsert(PipeEnrichedInsertNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedDeleteData(PipeEnrichedDeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedWriteSchema(PipeEnrichedWriteSchemaNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedConfigSchema(PipeEnrichedConfigSchemaNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitExplainAnalyze(ExplainAnalyzeNode node, C context) {
    return visitPlan(node, context);
  }
}
