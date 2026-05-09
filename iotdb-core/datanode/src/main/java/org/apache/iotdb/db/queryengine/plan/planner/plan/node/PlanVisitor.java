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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterEncodingCompressorNode;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.CollectNode;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryTransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.RegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowDiskUsageNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CteScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.NonAlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
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
public interface PlanVisitor<R, C> extends ICoreQueryPlanVisitor<R, C> {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Query Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // source --------------------------------------------------------------------------------------

  default R visitCteScan(CteScanNode node, C context) {
    return visitSourceNode(node, context);
  }

  default R visitSeriesScanSource(SeriesScanSourceNode node, C context) {
    return visitSourceNode(node, context);
  }

  default R visitSeriesScan(SeriesScanNode node, C context) {
    return visitSeriesScanSource(node, context);
  }

  default R visitAlignedSeriesScan(AlignedSeriesScanNode node, C context) {
    return visitSeriesScanSource(node, context);
  }

  default R visitSeriesAggregationSourceNode(SeriesAggregationSourceNode node, C context) {
    return visitSourceNode(node, context);
  }

  default R visitSeriesAggregationScan(SeriesAggregationScanNode node, C context) {
    return visitSeriesAggregationSourceNode(node, context);
  }

  default R visitAlignedSeriesAggregationScan(AlignedSeriesAggregationScanNode node, C context) {
    return visitSeriesAggregationSourceNode(node, context);
  }

  default R visitLastQueryScan(LastQueryScanNode node, C context) {
    return visitSourceNode(node, context);
  }

  default R visitRegionScan(RegionScanNode node, C context) {
    return visitSourceNode(node, context);
  }

  default R visitDeviceRegionScan(DeviceRegionScanNode node, C context) {
    return visitRegionScan(node, context);
  }

  default R visitTimeSeriesRegionScan(TimeseriesRegionScanNode node, C context) {
    return visitRegionScan(node, context);
  }

  // single child --------------------------------------------------------------------------------

  default R visitFill(FillNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitFilter(FilterNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitSlidingWindowAggregation(SlidingWindowAggregationNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitLimit(LimitNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitOffset(OffsetNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitSort(SortNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitProject(ProjectNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitExchange(ExchangeNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitTransform(TransformNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitInto(IntoNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitInto(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitDeviceViewInto(DeviceViewIntoNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitColumnInject(ColumnInjectNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitSingleDeviceView(SingleDeviceViewNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitInference(InferenceNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitExplainAnalyze(ExplainAnalyzeNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitRawDataAggregation(RawDataAggregationNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  // two child -----------------------------------------------------------------------------------

  default R visitLeftOuterTimeJoin(LeftOuterTimeJoinNode node, C context) {
    return visitTwoChildProcess(node, context);
  }

  // multi child --------------------------------------------------------------------------------

  default R visitDeviceView(DeviceViewNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitAggregationMergeSort(AggregationMergeSortNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitDeviceMerge(DeviceMergeNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitGroupByLevel(GroupByLevelNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitGroupByTag(GroupByTagNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitAggregation(AggregationNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitFullOuterTimeJoin(FullOuterTimeJoinNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitInnerTimeJoin(InnerTimeJoinNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitLastQuery(LastQueryNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitLastQueryMerge(LastQueryMergeNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitLastQueryCollect(LastQueryCollectNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitLastQueryTransform(LastQueryTransformNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitMergeSort(MergeSortNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitCollect(CollectNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitTopK(TopKNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitHorizontallyConcat(HorizontallyConcatNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitRegionMerge(ActiveRegionScanMergeNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  // others -----------------------------------------------------------------------------------

  default R visitShowQueries(ShowQueriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitShowDiskUsage(ShowDiskUsageNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitIdentitySink(IdentitySinkNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitShuffleSink(ShuffleSinkNode node, C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Schema Write & Query Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  default R visitSchemaQueryMerge(SchemaQueryMergeNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitSchemaQueryScan(SchemaQueryScanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitSchemaQueryOrderByHeat(SchemaQueryOrderByHeatNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitTimeSeriesSchemaScan(TimeSeriesSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDevicesSchemaScan(DevicesSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDevicesCount(DevicesCountNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitTimeSeriesCount(TimeSeriesCountNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitLevelTimeSeriesCount(LevelTimeSeriesCountNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitCountMerge(CountSchemaMergeNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitCreateTimeSeries(CreateTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitSchemaFetchMerge(SchemaFetchMergeNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitSeriesSchemaFetchScan(SeriesSchemaFetchScanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDeviceSchemaFetchScan(DeviceSchemaFetchScanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitCreateAlignedTimeSeries(CreateAlignedTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitCreateMultiTimeSeries(CreateMultiTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitAlterTimeSeries(AlterTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitInternalCreateTimeSeries(InternalCreateTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitActivateTemplate(ActivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitPreDeactivateTemplate(PreDeactivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitRollbackPreDeactivateTemplate(RollbackPreDeactivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDeactivateTemplate(DeactivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitInternalBatchActivateTemplate(InternalBatchActivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitInternalCreateMultiTimeSeries(InternalCreateMultiTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitNodePathsSchemaScan(NodePathsSchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitNodeManagementMemoryMerge(NodeManagementMemoryMergeNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitNodePathConvert(NodePathsConvertNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitNodePathsCount(NodePathsCountNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDeleteTimeseries(DeleteTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitAlterEncodingCompressor(AlterEncodingCompressorNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitAlterTimeSeriesDataType(AlterTimeSeriesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitConstructSchemaBlackList(ConstructSchemaBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitRollbackSchemaBlackList(RollbackSchemaBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitBatchActivateTemplate(BatchActivateTemplateNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitCreateLogicalView(CreateLogicalViewNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitConstructLogicalViewBlackList(ConstructLogicalViewBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitRollbackLogicalViewBlackList(RollbackLogicalViewBlackListNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDeleteLogicalView(DeleteLogicalViewNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitAlterLogicalView(AlterLogicalViewNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitCreateOrUpdateTableDevice(
      final CreateOrUpdateTableDeviceNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitTableDeviceAttributeUpdate(
      final TableDeviceAttributeUpdateNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitTableDeviceFetch(final TableDeviceFetchNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitTableDeviceQueryScan(final TableDeviceQueryScanNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitTableDeviceQueryCount(final TableDeviceQueryCountNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitTableDeviceAttributeCommit(
      final TableDeviceAttributeCommitUpdateNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitTableNodeLocationAdd(final TableNodeLocationAddNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitDeleteTableDevice(final DeleteTableDeviceNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitTableAttributeColumnDrop(
      final TableAttributeColumnDropNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitConstructTableDevicesBlackList(
      final ConstructTableDevicesBlackListNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitRollbackTableDevicesBlackList(
      final RollbackTableDevicesBlackListNode node, final C context) {
    return visitPlan(node, context);
  }

  default R visitDeleteTableDevicesInBlackList(
      final DeleteTableDevicesInBlackListNode node, final C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Write Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  default R visitInsertRow(InsertRowNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitRelationalInsertRow(RelationalInsertRowNode node, C context) {
    return visitInsertRow(node, context);
  }

  default R visitRelationalInsertRows(RelationalInsertRowsNode node, C context) {
    return visitInsertRows(node, context);
  }

  default R visitInsertTablet(InsertTabletNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitRelationalInsertTablet(RelationalInsertTabletNode node, C context) {
    return visitInsertTablet(node, context);
  }

  default R visitInsertRows(InsertRowsNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitInsertMultiTablets(InsertMultiTabletsNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitInsertRowsOfOneDevice(InsertRowsOfOneDeviceNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDeleteData(DeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDeleteData(RelationalDeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitWriteObjectFile(ObjectNode node, C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Pipe Related Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  default R visitPipeEnrichedInsertNode(PipeEnrichedInsertNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitPipeEnrichedDeleteDataNode(PipeEnrichedDeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitPipeEnrichedWritePlanNode(PipeEnrichedWritePlanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitPipeEnrichedNonWritePlanNode(PipeEnrichedNonWritePlanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitPipeOperateSchemaQueueNode(PipeOperateSchemaQueueNode node, C context) {
    return visitPlan(node, context);
  }

  // =============================== Used for Table Model ====================================

  default R visitTableScan(TableScanNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitDeviceTableScan(DeviceTableScanNode node, C context) {
    return visitTableScan(node, context);
  }

  default R visitInformationSchemaTableScan(InformationSchemaTableScanNode node, C context) {
    return visitTableScan(node, context);
  }

  default R visitExplainAnalyze(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode node,
      C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitCopyTo(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitTableExchange(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitAggregationTableScan(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode node,
      C context) {
    return visitDeviceTableScan(node, context);
  }

  default R visitTreeDeviceViewScan(TreeDeviceViewScanNode node, C context) {
    return visitDeviceTableScan(node, context);
  }

  default R visitAggregationTreeDeviceViewScan(AggregationTreeDeviceViewScanNode node, C context) {
    return visitAggregationTableScan(node, context);
  }

  default R visitAlignedAggregationTreeDeviceViewScan(
      AlignedAggregationTreeDeviceViewScanNode node, C context) {
    return visitAggregationTreeDeviceViewScan(node, context);
  }

  default R visitNonAlignedAggregationTreeDeviceViewScan(
      NonAlignedAggregationTreeDeviceViewScanNode node, C context) {
    return visitAggregationTreeDeviceViewScan(node, context);
  }

  default R visitTreeAlignedDeviceViewScan(TreeAlignedDeviceViewScanNode node, C context) {
    return visitTreeDeviceViewScan(node, context);
  }

  default R visitTreeNonAlignedDeviceViewScan(TreeNonAlignedDeviceViewScanNode node, C context) {
    return visitTreeDeviceViewScan(node, context);
  }
}
