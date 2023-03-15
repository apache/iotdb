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
package org.apache.iotdb.db.mpp.plan.planner.plan.node;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsConvertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.PathsUsingTemplateScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InvalidateSchemaCacheNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.PreDeactivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.RollbackPreDeactivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.RollbackSchemaBlackListNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum PlanNodeType {
  AGGREGATE((short) 0),
  DEVICE_VIEW((short) 1),
  FILL((short) 2),
  FILTER((short) 3),
  FILTER_NULL((short) 4),
  GROUP_BY_LEVEL((short) 5),
  LIMIT((short) 6),
  OFFSET((short) 7),
  SORT((short) 8),
  TIME_JOIN((short) 9),
  FRAGMENT_SINK((short) 10),
  SERIES_SCAN((short) 11),
  SERIES_AGGREGATE_SCAN((short) 12),
  INSERT_TABLET((short) 13),
  INSERT_ROW((short) 14),
  INSERT_ROWS((short) 15),
  INSERT_ROWS_OF_ONE_DEVICE((short) 16),
  INSERT_MULTI_TABLET((short) 17),
  DEVICES_SCHEMA_SCAN((short) 18),
  CREATE_TIME_SERIES((short) 19),
  EXCHANGE((short) 20),
  ALTER_TIME_SERIES((short) 21),
  CREATE_ALIGNED_TIME_SERIES((short) 22),
  TIME_SERIES_SCHEMA_SCAN((short) 23),
  SCHEMA_FETCH_SCAN((short) 24),
  SCHEMA_QUERY_MERGE((short) 25),
  SCHEMA_QUERY_ORDER_BY_HEAT((short) 26),
  DEVICES_COUNT((short) 27),
  TIME_SERIES_COUNT((short) 28),
  LEVEL_TIME_SERIES_COUNT((short) 29),
  COUNT_MERGE((short) 30),
  SLIDING_WINDOW_AGGREGATION((short) 31),
  PROJECT((short) 32),
  ALIGNED_SERIES_SCAN((short) 33),
  ALIGNED_SERIES_AGGREGATE_SCAN((short) 34),
  DEVICE_MERGE((short) 35),
  SCHEMA_FETCH_MERGE((short) 36),
  TRANSFORM((short) 37),
  CREATE_MULTI_TIME_SERIES((short) 39),
  NODE_PATHS_SCAN((short) 40),
  NODE_PATHS_CONVERT((short) 41),
  NODE_MANAGEMENT_MEMORY_MERGE((short) 42),
  INVALIDATE_SCHEMA_CACHE((short) 43),
  DELETE_DATA((short) 44),
  DELETE_TIMESERIES((short) 45),
  LAST_QUERY_SCAN((short) 46),
  ALIGNED_LAST_QUERY_SCAN((short) 47),
  LAST_QUERY((short) 48),
  LAST_QUERY_MERGE((short) 49),
  LAST_QUERY_COLLECT((short) 50),
  NODE_PATHS_COUNT((short) 51),
  INTERNAL_CREATE_TIMESERIES((short) 52),
  ACTIVATE_TEMPLATE((short) 53),
  PATHS_USING_TEMPLATE_SCAN((short) 54),
  LOAD_TSFILE((short) 55),
  CONSTRUCT_SCHEMA_BLACK_LIST_NODE((short) 56),
  ROLLBACK_SCHEMA_BLACK_LIST_NODE((short) 57),
  GROUP_BY_TAG((short) 58),
  PRE_DEACTIVATE_TEMPLATE_NODE((short) 59),
  ROLLBACK_PRE_DEACTIVATE_TEMPLATE_NODE((short) 60),
  DEACTIVATE_TEMPLATE_NODE((short) 61),
  INTO((short) 62),
  DEVICE_VIEW_INTO((short) 63),
  VERTICALLY_CONCAT((short) 64),
  SINGLE_DEVICE_VIEW((short) 65),
  MERGE_SORT((short) 66),
  SHOW_QUERIES((short) 67),
  INTERNAL_BATCH_ACTIVATE_TEMPLATE((short) 68),
  INTERNAL_CREATE_MULTI_TIMESERIES((short) 69),
  IDENTITY_SINK((short) 70),
  SHUFFLE_SINK((short) 71);

  public static final int BYTES = Short.BYTES;

  private final short nodeType;

  PlanNodeType(short nodeType) {
    this.nodeType = nodeType;
  }

  public short getNodeType() {
    return nodeType;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(nodeType, buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(nodeType, stream);
  }

  public static PlanNode deserializeFromWAL(DataInputStream stream) throws IOException {
    short nodeType = stream.readShort();
    switch (nodeType) {
      case 13:
        return InsertTabletNode.deserializeFromWAL(stream);
      case 14:
        return InsertRowNode.deserializeFromWAL(stream);
      case 44:
        return DeleteDataNode.deserializeFromWAL(stream);
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }

  public static PlanNode deserializeFromWAL(ByteBuffer buffer) {
    short nodeType = buffer.getShort();
    switch (nodeType) {
      case 13:
        return InsertTabletNode.deserializeFromWAL(buffer);
      case 14:
        return InsertRowNode.deserializeFromWAL(buffer);
      case 44:
        return DeleteDataNode.deserializeFromWAL(buffer);
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    short nodeType = buffer.getShort();
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
        return TimeJoinNode.deserialize(buffer);
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
        return SchemaFetchScanNode.deserialize(buffer);
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
      case 43:
        return InvalidateSchemaCacheNode.deserialize(buffer);
      case 44:
        return DeleteDataNode.deserialize(buffer);
      case 45:
        return DeleteTimeSeriesNode.deserialize(buffer);
      case 46:
        return LastQueryScanNode.deserialize(buffer);
      case 47:
        return AlignedLastQueryScanNode.deserialize(buffer);
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
        return MergeSortNode.deserialize(buffer);
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
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }
}
