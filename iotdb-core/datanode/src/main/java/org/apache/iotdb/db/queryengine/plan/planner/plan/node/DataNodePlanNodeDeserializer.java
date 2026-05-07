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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.CommonPlanNodeDeserializer;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DeviceSchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DevicesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.LogicalViewSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsConvertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.PathsUsingTemplateScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryOrderByHeatNode;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowDiskUsageNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.NonAlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableDiskUsageInformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataNodePlanNodeDeserializer extends CommonPlanNodeDeserializer {
  public static final DataNodePlanNodeDeserializer INSTANCE = new DataNodePlanNodeDeserializer();

  @Override
  public PlanNode deserializeFromWAL(DataInputStream stream) throws IOException {
    short nodeType = stream.readShort();
    switch (nodeType) {
      case 13:
        return InsertTabletNode.deserializeFromWAL(stream);
      case 14:
        return InsertRowNode.deserializeFromWAL(stream);
      case 15:
        return InsertRowsNode.deserializeFromWAL(stream);
      case 44:
        return DeleteDataNode.deserializeFromWAL(stream);
      case 97:
        return ContinuousSameSearchIndexSeparatorNode.deserializeFromWAL(stream);
      case 2000:
        return RelationalInsertTabletNode.deserializeFromWAL(stream);
      case 2001:
        return RelationalInsertRowNode.deserializeFromWAL(stream);
      case 2002:
        return RelationalInsertRowsNode.deserializeFromWAL(stream);
      case 2003:
        return RelationalDeleteDataNode.deserializeFromWAL(stream);
      case 2004:
        return ObjectNode.deserializeFromWAL(stream);
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }

  @Override
  public PlanNode deserializeFromWAL(ByteBuffer buffer) {
    short nodeType = buffer.getShort();
    switch (nodeType) {
      case 13:
        return InsertTabletNode.deserializeFromWAL(buffer);
      case 14:
        return InsertRowNode.deserializeFromWAL(buffer);
      case 15:
        return InsertRowsNode.deserializeFromWAL(buffer);
      case 44:
        return DeleteDataNode.deserializeFromWAL(buffer);
      case 97:
        return ContinuousSameSearchIndexSeparatorNode.deserializeFromWAL(buffer);
      case 2000:
        return RelationalInsertTabletNode.deserializeFromWAL(buffer);
      case 2001:
        return RelationalInsertRowNode.deserializeFromWAL(buffer);
      case 2002:
        return RelationalInsertRowsNode.deserializeFromWAL(buffer);
      case 2003:
        return RelationalDeleteDataNode.deserializeFromWAL(buffer);
      case 2004:
        return ObjectNode.deserialize(buffer);
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }

  @Override
  public PlanNode deserialize(ByteBuffer buffer) {
    short nodeType = buffer.getShort();
    return deserialize(buffer, nodeType);
  }

  @Override
  public PlanNode deserialize(ByteBuffer buffer, short nodeType) {
    switch (nodeType) {
      case 0:
        return AggregationNode.deserialize(buffer);
      case 1:
        return DeviceViewNode.deserialize(buffer);
      case 2:
        return FillNode.deserialize(buffer);
      case 3:
        return FilterNode.deserialize(buffer);
      case 5:
        return GroupByLevelNode.deserialize(buffer);
      case 6:
        return LimitNode.deserialize(buffer);
      case 7:
        return OffsetNode.deserialize(buffer);
      case 8:
        return SortNode.deserialize(buffer);
      case 9:
        return FullOuterTimeJoinNode.deserialize(buffer);
      case 11:
        return SeriesScanNode.deserialize(buffer);
      case 12:
        return SeriesAggregationScanNode.deserialize(buffer);
      case 13:
        return InsertTabletNode.deserialize(buffer);
      case 14:
        return InsertRowNode.deserialize(buffer);
      case 15:
        return InsertRowsNode.deserialize(buffer);
      case 16:
        return InsertRowsOfOneDeviceNode.deserialize(buffer);
      case 17:
        return InsertMultiTabletsNode.deserialize(buffer);
      case 18:
        return DevicesSchemaScanNode.deserialize(buffer);
      case 19:
        return CreateTimeSeriesNode.deserialize(buffer);
      case 20:
        return ExchangeNode.deserialize(buffer);
      case 21:
        return AlterTimeSeriesNode.deserialize(buffer);
      case 22:
        return CreateAlignedTimeSeriesNode.deserialize(buffer);
      case 23:
        return TimeSeriesSchemaScanNode.deserialize(buffer);
      case 24:
        return SeriesSchemaFetchScanNode.deserialize(buffer);
      case 25:
        return SchemaQueryMergeNode.deserialize(buffer);
      case 26:
        return SchemaQueryOrderByHeatNode.deserialize(buffer);
      case 27:
        return DevicesCountNode.deserialize(buffer);
      case 28:
        return TimeSeriesCountNode.deserialize(buffer);
      case 29:
        return LevelTimeSeriesCountNode.deserialize(buffer);
      case 30:
        return CountSchemaMergeNode.deserialize(buffer);
      case 31:
        return SlidingWindowAggregationNode.deserialize(buffer);
      case 32:
        return ProjectNode.deserialize(buffer);
      case 33:
        return AlignedSeriesScanNode.deserialize(buffer);
      case 34:
        return AlignedSeriesAggregationScanNode.deserialize(buffer);
      case 35:
        return DeviceMergeNode.deserialize(buffer);
      case 36:
        return SchemaFetchMergeNode.deserialize(buffer);
      case 37:
        return TransformNode.deserialize(buffer);
      case 39:
        return CreateMultiTimeSeriesNode.deserialize(buffer);
      case 40:
        return NodePathsSchemaScanNode.deserialize(buffer);
      case 41:
        return NodePathsConvertNode.deserialize(buffer);
      case 42:
        return NodeManagementMemoryMergeNode.deserialize(buffer);
      case 44:
        return DeleteDataNode.deserialize(buffer);
      case 45:
        return DeleteTimeSeriesNode.deserialize(buffer);
      case 46:
      case 47:
        throw new UnsupportedOperationException("This LastQueryScanNode is deprecated");
      case 48:
        return LastQueryNode.deserialize(buffer);
      case 49:
        return LastQueryMergeNode.deserialize(buffer);
      case 50:
        return LastQueryCollectNode.deserialize(buffer);
      case 51:
        return NodePathsCountNode.deserialize(buffer);
      case 52:
        return InternalCreateTimeSeriesNode.deserialize(buffer);
      case 53:
        return ActivateTemplateNode.deserialize(buffer);
      case 54:
        return PathsUsingTemplateScanNode.deserialize(buffer);
      case 55:
        return LoadTsFilePieceNode.deserialize(buffer);
      case 56:
        return ConstructSchemaBlackListNode.deserialize(buffer);
      case 57:
        return RollbackSchemaBlackListNode.deserialize(buffer);
      case 58:
        return GroupByTagNode.deserialize(buffer);
      case 59:
        return PreDeactivateTemplateNode.deserialize(buffer);
      case 60:
        return RollbackPreDeactivateTemplateNode.deserialize(buffer);
      case 61:
        return DeactivateTemplateNode.deserialize(buffer);
      case 62:
        return IntoNode.deserialize(buffer);
      case 63:
        return DeviceViewIntoNode.deserialize(buffer);
      case 64:
        return HorizontallyConcatNode.deserialize(buffer);
      case 65:
        return SingleDeviceViewNode.deserialize(buffer);
      case 66:
        return org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode
            .deserialize(buffer);
      case 67:
        return ShowQueriesNode.deserialize(buffer);
      case 68:
        return InternalBatchActivateTemplateNode.deserialize(buffer);
      case 69:
        return InternalCreateMultiTimeSeriesNode.deserialize(buffer);
      case 70:
        return IdentitySinkNode.deserialize(buffer);
      case 71:
        return ShuffleSinkNode.deserialize(buffer);
      case 72:
        return BatchActivateTemplateNode.deserialize(buffer);
      case 73:
        return CreateLogicalViewNode.deserialize(buffer);
      case 74:
        return ConstructLogicalViewBlackListNode.deserialize(buffer);
      case 75:
        return RollbackLogicalViewBlackListNode.deserialize(buffer);
      case 76:
        return DeleteLogicalViewNode.deserialize(buffer);
      case 77:
        return LogicalViewSchemaScanNode.deserialize(buffer);
      case 78:
        return AlterLogicalViewNode.deserialize(buffer);
      case 79:
        return PipeEnrichedInsertNode.deserialize(buffer);
      case 80:
        return InferenceNode.deserialize(buffer);
      case 81:
        return LastQueryTransformNode.deserialize(buffer);
      case 82:
        return TopKNode.deserialize(buffer);
      case 83:
        return ColumnInjectNode.deserialize(buffer);
      case 84:
        return PipeEnrichedDeleteDataNode.deserialize(buffer);
      case 85:
        return PipeEnrichedWritePlanNode.deserialize(buffer);
      case 86:
        return PipeEnrichedNonWritePlanNode.deserialize(buffer);
      case 87:
        return InnerTimeJoinNode.deserialize(buffer);
      case 88:
        return LeftOuterTimeJoinNode.deserialize(buffer);
      case 89:
        return AggregationMergeSortNode.deserialize(buffer);
      case 90:
        throw new UnsupportedOperationException("ExplainAnalyzeNode should not be serialized");
      case 91:
        return PipeOperateSchemaQueueNode.deserialize(buffer);
      case 92:
        return RawDataAggregationNode.deserialize(buffer);
      case 93:
        return DeviceRegionScanNode.deserialize(buffer);
      case 94:
        return TimeseriesRegionScanNode.deserialize(buffer);
      case 95:
        return ActiveRegionScanMergeNode.deserialize(buffer);
      case 96:
        return DeviceSchemaFetchScanNode.deserialize(buffer);
      case 97:
        throw new UnsupportedOperationException(
            "You should never see ContinuousSameSearchIndexSeparatorNode in this function, because ContinuousSameSearchIndexSeparatorNode should never be used in network transmission.");
      case 98:
        return LastQueryScanNode.deserialize(buffer);
      case 99:
        return AlterEncodingCompressorNode.deserialize(buffer);
      // 100 - 106 are occupied
      case 107:
        return ShowDiskUsageNode.deserialize(buffer);
      case 108:
        return CollectNode.deserialize(buffer);
      case 902:
        return CreateOrUpdateTableDeviceNode.deserialize(buffer);
      case 903:
        return TableDeviceQueryScanNode.deserialize(buffer);
      case 904:
        return TableDeviceFetchNode.deserialize(buffer);
      case 905:
        return DeleteTableDeviceNode.deserialize(buffer);
      case 906:
        return TableDeviceQueryCountNode.deserialize(buffer);
      case 907:
        return TableDeviceAttributeUpdateNode.deserialize(buffer);
      case 908:
        return TableDeviceAttributeCommitUpdateNode.deserialize(buffer);
      case 909:
        return TableNodeLocationAddNode.deserialize(buffer);
      case 910:
        return ConstructTableDevicesBlackListNode.deserialize(buffer);
      case 911:
        return RollbackTableDevicesBlackListNode.deserialize(buffer);
      case 912:
        return DeleteTableDevicesInBlackListNode.deserialize(buffer);
      case 913:
        return TableAttributeColumnDropNode.deserialize(buffer);
      case 1000:
        return org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode
            .deserialize(buffer);
      case 1016:
        return org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode
            .deserialize(buffer);
      case 1018:
        return org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode
            .deserialize(buffer);
      case 1019:
        throw new UnsupportedOperationException("ExplainAnalyzeNode should not be deserialized");
      case 1021:
        return InformationSchemaTableScanNode.deserialize(buffer);
      case 1022:
        throw new UnsupportedOperationException(
            "AggregationTreeDeviceViewScanNode should not be deserialized");
      case 1023:
        return TreeAlignedDeviceViewScanNode.deserialize(buffer);
      case 1024:
        return TreeNonAlignedDeviceViewScanNode.deserialize(buffer);
      case 1033:
        return org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode.deserialize(
            buffer);
      case 1040:
        return TableDiskUsageInformationSchemaTableScanNode.deserialize(buffer);
      case 1041:
        return AlignedAggregationTreeDeviceViewScanNode.deserialize(buffer);
      case 1042:
        return NonAlignedAggregationTreeDeviceViewScanNode.deserialize(buffer);
      case 2000:
        return RelationalInsertTabletNode.deserialize(buffer);
      case 2001:
        return RelationalInsertRowNode.deserialize(buffer);
      case 2002:
        return RelationalInsertRowsNode.deserialize(buffer);
      case 2003:
        return RelationalDeleteDataNode.deserialize(buffer);
      case 2004:
        return ObjectNode.deserialize(buffer);
      default:
        return super.deserialize(buffer, nodeType);
    }
  }

  public PlanNode deserializeWithTemplate(ByteBuffer buffer, TypeProvider typeProvider) {
    short nodeType = buffer.getShort();
    switch (nodeType) {
      case 1:
        return DeviceViewNode.deserializeUseTemplate(buffer, typeProvider);
      case 3:
        return FilterNode.deserializeUseTemplate(buffer, typeProvider);
      case 32:
        return ProjectNode.deserializeUseTemplate(buffer, typeProvider);
      case 33:
        return AlignedSeriesScanNode.deserializeUseTemplate(buffer, typeProvider);
      case 34:
        return AlignedSeriesAggregationScanNode.deserializeUseTemplate(buffer, typeProvider);
      case 65:
        return SingleDeviceViewNode.deserializeUseTemplate(buffer, typeProvider);
      default:
        return deserialize(buffer, nodeType);
    }
  }
}
