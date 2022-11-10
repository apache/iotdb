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

package org.apache.iotdb.db.mpp.common.header;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ColumnHeaderConstant {

  // column names for query statement
  public static final String COLUMN_TIME = "Time";
  public static final String COLUMN_VALUE = "Value";
  public static final String COLUMN_DEVICE = "Device";

  // column names for schema statement
  public static final String COLUMN_DATABASE = "Database";
  public static final String COLUMN_TIMESERIES = "Timeseries";
  public static final String COLUMN_ALIAS = "Alias";
  public static final String COLUMN_DATATYPE = "DataType";
  public static final String COLUMN_ENCODING = "Encoding";
  public static final String COLUMN_COMPRESSION = "Compression";
  public static final String COLUMN_DEVICES = "Devices";
  public static final String COLUMN_TAGS = "Tags";
  public static final String COLUMN_ATTRIBUTES = "Attributes";
  public static final String COLUMN_IS_ALIGNED = "IsAligned";
  public static final String COLUMN_COUNT = "Count";
  public static final String COLUMN_TTL = "TTL(ms)";
  public static final String COLUMN_SCHEMA_REPLICATION_FACTOR = "SchemaReplicationFactor";
  public static final String COLUMN_DATA_REPLICATION_FACTOR = "DataReplicationFactor";
  public static final String COLUMN_TIME_PARTITION_INTERVAL = "TimePartitionInterval";
  public static final String COLUMN_CHILDPATHS = "ChildPaths";
  public static final String COLUMN_NODETYPES = "NodeTypes";
  public static final String COLUMN_CHILDNODES = "ChildNodes";
  public static final String COLUMN_VERSION = "Version";
  public static final String COLUMN_BUILD_INFO = "BuildInfo";
  public static final String COLUMN_PATHS = "Paths";

  // column names for count statement
  public static final String COLUMN_COLUMN = "Column";
  public static final String COLUMN_COUNT_DEVICES = "Count(Devices)";
  public static final String COLUMN_COUNT_NODES = "Count(Nodes)";
  public static final String COLUMN_COUNT_TIMESERIES = "Count(Timeseries)";
  public static final String COLUMN_COUNT_DATABASE = "Count(Database)";

  // column names for show cluster and show cluster details statements
  public static final String COLUMN_NODE_ID = "NodeID";
  public static final String COLUMN_NODE_TYPE = "NodeType";
  public static final String COLUMN_STATUS = "Status";
  public static final String COLUMN_HOST = "Host";
  public static final String COLUMN_INTERNAL_ADDRESS = "InternalAddress";
  public static final String COLUMN_INTERNAL_PORT = "InternalPort";
  public static final String COLUMN_CONFIG_CONSENSUS_PORT = "ConfigConsensusPort";
  public static final String COLUMN_RPC_ADDRESS = "RpcAddress";
  public static final String COLUMN_RPC_PORT = "RpcPort";
  public static final String COLUMN_DATA_CONSENSUS_PORT = "DataConsensusPort";
  public static final String COLUMN_SCHEMA_CONSENSUS_PORT = "SchemaConsensusPort";
  public static final String COLUMN_MPP_PORT = "MppPort";

  // column names for show functions statement
  public static final String COLUMN_FUNCTION_NAME = "FunctionName";
  public static final String COLUMN_FUNCTION_TYPE = "FunctionType";
  public static final String COLUMN_FUNCTION_CLASS = "ClassName(UDF)";

  // column names for show triggers statement
  public static final String COLUMN_TRIGGER_NAME = "TriggerName";
  public static final String COLUMN_TRIGGER_EVENT = "Event";
  public static final String COLUMN_TRIGGER_TYPE = "Type";
  public static final String COLUMN_TRIGGER_STATE = "State";
  public static final String COLUMN_TRIGGER_PATTERN = "PathPattern";
  public static final String COLUMN_TRIGGER_CLASSNAME = "ClassName";
  public static final String COLUMN_TRIGGER_LOCATION = "NodeID";

  // column names for show region statement
  public static final String COLUMN_REGION_ID = "RegionId";
  public static final String COLUMN_TYPE = "Type";
  public static final String COLUMN_DATANODE_ID = "DataNodeId";
  public static final String COLUMN_SERIES_SLOT_ID = "SeriesSlotId";
  public static final String COLUMN_TIME_SLOT_ID = "TimeSlotId";
  public static final String COLUMN_ROLE = "Role";

  // column names for show datanodes
  public static final String COLUMN_DATA_REGION_NUM = "DataRegionNum";
  public static final String COLUMN_SCHEMA_REGION_NUM = "SchemaRegionNum";

  // column names for show schema template statement
  public static final String COLUMN_TEMPLATE_NAME = "TemplateName";

  // column names for show pipe sink type
  public static final String COLUMN_PIPESINK_TYPE = "Type";

  // column names for show pipe sink
  public static final String COLUMN_PIPESINK_NAME = "Name";
  public static final String COLUMN_PIPESINK_ATTRIBUTES = "Attributes";

  // column names for show pipe
  public static final String COLUMN_PIPE_CREATE_TIME = "CreateTime";
  public static final String COLUMN_PIPE_NAME = "Name";
  public static final String COLUMN_PIPE_ROLE = "Role";
  public static final String COLUMN_PIPE_REMOTE = "Remote";
  public static final String COLUMN_PIPE_STATUS = "Status";
  public static final String COLUMN_PIPE_ATTRIBUTES = "Attributes";
  public static final String COLUMN_PIPE_MESSAGE = "Message";

  // column names for select into
  public static final String COLUMN_SOURCE_DEVICE = "SourceDevice";
  public static final String COLUMN_SOURCE_COLUMN = "SourceColumn";
  public static final String COLUMN_TARGET_TIMESERIES = "TargetTimeseries";
  public static final String COLUMN_WRITTEN = "Written";

  // column names for show cq
  public static final String COLUMN_CQ_ID = "CqId";
  public static final String COLUMN_QUERY = "Query";
  public static final String COLUMN_STATE = "State";

  public static final List<ColumnHeader> lastQueryColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_VALUE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATATYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> showTimeSeriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_ALIAS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATATYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_ENCODING, TSDataType.TEXT),
          new ColumnHeader(COLUMN_COMPRESSION, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TAGS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_ATTRIBUTES, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesWithSgColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_DEVICES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_IS_ALIGNED, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_DEVICES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_IS_ALIGNED, TSDataType.TEXT));

  public static final List<ColumnHeader> showTTLColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.INT64));

  public static final List<ColumnHeader> showStorageGroupColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.INT64),
          new ColumnHeader(COLUMN_SCHEMA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(COLUMN_DATA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(COLUMN_TIME_PARTITION_INTERVAL, TSDataType.INT64),
          new ColumnHeader(COLUMN_SCHEMA_REGION_NUM, TSDataType.INT32),
          new ColumnHeader(COLUMN_DATA_REGION_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> showChildPathsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_CHILDPATHS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_NODETYPES, TSDataType.TEXT));

  public static final List<ColumnHeader> showNodesInSchemaTemplateHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_CHILDNODES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATATYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_ENCODING, TSDataType.TEXT),
          new ColumnHeader(COLUMN_COMPRESSION, TSDataType.TEXT));

  public static final List<ColumnHeader> showChildNodesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_CHILDNODES, TSDataType.TEXT));

  public static final List<ColumnHeader> showVersionColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_VERSION, TSDataType.TEXT),
          new ColumnHeader(COLUMN_BUILD_INFO, TSDataType.TEXT));

  public static final List<ColumnHeader> showPathsUsingTemplateHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_PATHS, TSDataType.TEXT));

  public static final List<ColumnHeader> showPathSetTemplateHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_PATHS, TSDataType.TEXT));

  public static final List<ColumnHeader> countDevicesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_COUNT_DEVICES, TSDataType.INT32));

  public static final List<ColumnHeader> countNodesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_COUNT_NODES, TSDataType.INT32));

  public static final List<ColumnHeader> countLevelTimeSeriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_COLUMN, TSDataType.TEXT),
          new ColumnHeader(COLUMN_COUNT_TIMESERIES, TSDataType.INT32));

  public static final List<ColumnHeader> countTimeSeriesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_COUNT_TIMESERIES, TSDataType.INT32));

  public static final List<ColumnHeader> countStorageGroupColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_COUNT_DATABASE, TSDataType.INT32));

  public static final List<ColumnHeader> showRegionColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_REGION_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_SERIES_SLOT_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_TIME_SLOT_ID, TSDataType.INT64),
          new ColumnHeader(COLUMN_DATANODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_HOST, TSDataType.TEXT),
          new ColumnHeader(COLUMN_RPC_PORT, TSDataType.INT32),
          new ColumnHeader(COLUMN_ROLE, TSDataType.TEXT));

  public static final List<ColumnHeader> showDataNodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_HOST, TSDataType.TEXT),
          new ColumnHeader(COLUMN_RPC_PORT, TSDataType.INT32),
          new ColumnHeader(COLUMN_DATA_REGION_NUM, TSDataType.INT32),
          new ColumnHeader(COLUMN_SCHEMA_REGION_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> showConfigNodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_HOST, TSDataType.TEXT),
          new ColumnHeader(COLUMN_INTERNAL_PORT, TSDataType.INT32),
          new ColumnHeader(COLUMN_ROLE, TSDataType.TEXT));

  public static final List<ColumnHeader> showClusterColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_NODE_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_INTERNAL_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_INTERNAL_PORT, TSDataType.INT32));

  public static final List<ColumnHeader> showClusterDetailsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_NODE_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_INTERNAL_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_INTERNAL_PORT, TSDataType.INT32),
          new ColumnHeader(COLUMN_CONFIG_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(COLUMN_RPC_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_RPC_PORT, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATA_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(COLUMN_SCHEMA_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(COLUMN_MPP_PORT, TSDataType.TEXT));

  public static final List<ColumnHeader> showFunctionsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_FUNCTION_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_FUNCTION_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_FUNCTION_CLASS, TSDataType.TEXT));

  public static final List<ColumnHeader> showTriggersColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_TRIGGER_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TRIGGER_EVENT, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TRIGGER_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TRIGGER_STATE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TRIGGER_PATTERN, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TRIGGER_CLASSNAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TRIGGER_LOCATION, TSDataType.TEXT));

  public static final List<ColumnHeader> showSchemaTemplateHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_TEMPLATE_NAME, TSDataType.TEXT));

  public static final List<ColumnHeader> showPipeSinkTypeColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_PIPESINK_TYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> showPipeSinkColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_PIPESINK_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPESINK_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPESINK_ATTRIBUTES, TSDataType.TEXT));

  public static final List<ColumnHeader> showPipeColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_PIPE_CREATE_TIME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPE_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPE_ROLE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPE_REMOTE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPE_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPE_ATTRIBUTES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PIPE_MESSAGE, TSDataType.TEXT));

  public static final List<ColumnHeader> selectIntoColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_SOURCE_COLUMN, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TARGET_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_WRITTEN, TSDataType.INT32));

  public static final List<ColumnHeader> selectIntoAlignByDeviceColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_SOURCE_DEVICE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_SOURCE_COLUMN, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TARGET_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_WRITTEN, TSDataType.INT32));

  public static final List<ColumnHeader> getRegionIdColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_REGION_ID, TSDataType.INT32));

  public static final List<ColumnHeader> getTimeSlotListColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_TIME_SLOT_ID, TSDataType.INT64));

  public static final List<ColumnHeader> getSeriesSlotListColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_SERIES_SLOT_ID, TSDataType.INT32));

  public static final List<ColumnHeader> showContinuousQueriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_CQ_ID, TSDataType.TEXT),
          new ColumnHeader(COLUMN_QUERY, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STATE, TSDataType.TEXT));
}
