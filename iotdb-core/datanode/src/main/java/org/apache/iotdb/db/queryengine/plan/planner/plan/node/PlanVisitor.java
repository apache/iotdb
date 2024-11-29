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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DeviceSchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DevicesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsConvertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SeriesSchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.PreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.RollbackPreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.RollbackSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.ConstructLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.DeleteLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.RollbackLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedNonWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AI.InferenceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ActiveRegionScanMergeNode;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.RegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.GroupReference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.ConstructTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDevicesInBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.RollbackTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableAttributeColumnDropNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeCommitUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableNodeLocationAddNode;

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

  public R visitRegionScan(RegionScanNode node, C context) {
    return visitSourceNode(node, context);
  }

  public R visitDeviceRegionScan(DeviceRegionScanNode node, C context) {
    return visitRegionScan(node, context);
  }

  public R visitTimeSeriesRegionScan(TimeseriesRegionScanNode node, C context) {
    return visitRegionScan(node, context);
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

  public R visitInference(InferenceNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitExplainAnalyze(ExplainAnalyzeNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitRawDataAggregation(RawDataAggregationNode node, C context) {
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

  public R visitRegionMerge(ActiveRegionScanMergeNode node, C context) {
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

  public R visitSeriesSchemaFetchScan(SeriesSchemaFetchScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeviceSchemaFetchScan(DeviceSchemaFetchScanNode node, C context) {
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

  public R visitCreateOrUpdateTableDevice(
      final CreateOrUpdateTableDeviceNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitTableDeviceAttributeUpdate(
      final TableDeviceAttributeUpdateNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitTableDeviceFetch(final TableDeviceFetchNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitTableDeviceQueryScan(final TableDeviceQueryScanNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitTableDeviceQueryCount(final TableDeviceQueryCountNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitTableDeviceAttributeCommit(
      final TableDeviceAttributeCommitUpdateNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitTableNodeLocationAdd(final TableNodeLocationAddNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitDeleteTableDevice(final DeleteTableDeviceNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitTableAttributeColumnDrop(final TableAttributeColumnDropNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitConstructTableDevicesBlackList(
      final ConstructTableDevicesBlackListNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitRollbackTableDevicesBlackList(
      final RollbackTableDevicesBlackListNode node, final C context) {
    return visitPlan(node, context);
  }

  public R visitDeleteTableDevicesInBlackList(
      final DeleteTableDevicesInBlackListNode node, final C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Write Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public R visitInsertRow(InsertRowNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRelationalInsertRow(RelationalInsertRowNode node, C context) {
    return visitInsertRow(node, context);
  }

  public R visitRelationalInsertRows(RelationalInsertRowsNode node, C context) {
    return visitInsertRows(node, context);
  }

  public R visitInsertTablet(InsertTabletNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRelationalInsertTablet(RelationalInsertTabletNode node, C context) {
    return visitInsertTablet(node, context);
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

  public R visitDeleteData(RelationalDeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Pipe Related Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public R visitPipeEnrichedInsertNode(PipeEnrichedInsertNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedDeleteDataNode(PipeEnrichedDeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedWritePlanNode(PipeEnrichedWritePlanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedNonWritePlanNode(PipeEnrichedNonWritePlanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeOperateSchemaQueueNode(PipeOperateSchemaQueueNode node, C context) {
    return visitPlan(node, context);
  }

  // =============================== Used for Table Model ====================================
  public R visitFilter(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitTableScan(TableScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitProject(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitLimit(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitOffset(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitMergeSort(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitExplainAnalyze(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode node,
      C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitOutput(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitCollect(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitGapFill(GapFillNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitFill(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.FillNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitPreviousFill(PreviousFillNode node, C context) {
    return visitFill(node, context);
  }

  public R visitLinearFill(LinearFillNode node, C context) {
    return visitFill(node, context);
  }

  public R visitValueFill(ValueFillNode node, C context) {
    return visitFill(node, context);
  }

  public R visitSort(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitStreamSort(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitTopK(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  public R visitJoin(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode node, C context) {
    return visitTwoChildProcess(node, context);
  }

  public R visitGroupReference(GroupReference node, C context) {
    return visitPlan(node, context);
  }

  public R visitAggregation(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode node,
      C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitTableExchange(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  public R visitAggregationTableScan(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode node,
      C context) {
    return visitTableScan(node, context);
  }
}
