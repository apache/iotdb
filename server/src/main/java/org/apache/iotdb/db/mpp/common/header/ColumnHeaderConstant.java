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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ColumnHeaderConstant {
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  // column names for query statement
  public static final String TIME = "Time";
  public static final String VALUE = "Value";
  public static final String DEVICE = "Device";

  // column names for schema statement
  public static final String DATABASE = "Database";
  public static final String TIMESERIES = "Timeseries";
  public static final String ALIAS = "Alias";
  public static final String DATATYPE = "DataType";
  public static final String ENCODING = "Encoding";
  public static final String COMPRESSION = "Compression";
  public static final String TAGS = "Tags";
  public static final String ATTRIBUTES = "Attributes";
  public static final String DEADBAND = "Deadband";
  public static final String DEADBAND_PARAMETERS = "DeadbandParameters";
  public static final String IS_ALIGNED = "IsAligned";
  public static final String COUNT = "Count";
  public static final String COLUMN_TTL = "TTL(ms)";
  public static final String SCHEMA_REPLICATION_FACTOR = "SchemaReplicationFactor";
  public static final String DATA_REPLICATION_FACTOR = "DataReplicationFactor";
  public static final String TIME_PARTITION_INTERVAL = "TimePartitionInterval";
  public static final String CHILD_PATHS = "ChildPaths";
  public static final String NODE_TYPES = "NodeTypes";
  public static final String CHILD_NODES = "ChildNodes";
  public static final String VERSION = "Version";
  public static final String BUILD_INFO = "BuildInfo";
  public static final String PATHS = "Paths";

  // column names for count statement
  public static final String COLUMN = "Column";
  public static final String COUNT_DEVICES = "count(devices)";
  public static final String COUNT_NODES = "count(nodes)";
  public static final String COUNT_TIMESERIES = "count(timeseries)";
  public static final String COUNT_DATABASE = "count(database)";

  // column names for show cluster and show cluster details statements
  public static final String NODE_ID = "NodeID";
  public static final String NODE_TYPE = "NodeType";
  public static final String STATUS = "Status";
  public static final String HOST = "Host";
  public static final String INTERNAL_ADDRESS = "InternalAddress";
  public static final String INTERNAL_PORT = "InternalPort";
  public static final String CONFIG_CONSENSUS_PORT = "ConfigConsensusPort";
  public static final String RPC_ADDRESS = "RpcAddress";
  public static final String RPC_PORT = "RpcPort";
  public static final String DATA_CONSENSUS_PORT = "DataConsensusPort";
  public static final String SCHEMA_CONSENSUS_PORT = "SchemaConsensusPort";
  public static final String MPP_PORT = "MppPort";

  // column names for show functions statement
  public static final String FUNCTION_NAME = "FunctionName";
  public static final String FUNCTION_TYPE = "FunctionType";
  public static final String CLASS_NAME_UDF = "ClassName(UDF)";

  // column names for show triggers statement
  public static final String TRIGGER_NAME = "TriggerName";
  public static final String EVENT = "Event";
  public static final String STATE = "State";
  public static final String PATH_PATTERN = "PathPattern";
  public static final String CLASS_NAME = "ClassName";

  // column names for show region statement
  public static final String REGION_ID = "RegionId";
  public static final String TYPE = "Type";
  public static final String DATA_NODE_ID = "DataNodeId";
  public static final String SERIES_SLOT_NUM = "SeriesSlotNum";
  public static final String TIME_SLOT_NUM = "TimeSlotNum";
  public static final String ROLE = "Role";

  // column names for show datanodes
  public static final String DATA_REGION_NUM = "DataRegionNum";
  public static final String SCHEMA_REGION_NUM = "SchemaRegionNum";

  // column names for show schema template statement
  public static final String TEMPLATE_NAME = "TemplateName";

  // column names for show pipe sink
  public static final String NAME = "Name";

  // column names for show pipe
  public static final String CREATE_TIME = "CreateTime";
  public static final String REMOTE = "Remote";
  public static final String MESSAGE = "Message";

  // column names for select into
  public static final String SOURCE_DEVICE = "SourceDevice";
  public static final String SOURCE_COLUMN = "SourceColumn";
  public static final String TARGET_TIMESERIES = "TargetTimeseries";
  public static final String WRITTEN = "Written";

  // column names for show cq
  public static final String CQID = "CQId";
  public static final String QUERY = "Query";

  public static final List<ColumnHeader> lastQueryColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(VALUE, TSDataType.TEXT),
          new ColumnHeader(DATATYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> showTimeSeriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(ALIAS, TSDataType.TEXT),
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(DATATYPE, TSDataType.TEXT),
          new ColumnHeader(ENCODING, TSDataType.TEXT),
          new ColumnHeader(COMPRESSION, TSDataType.TEXT),
          new ColumnHeader(TAGS, TSDataType.TEXT),
          new ColumnHeader(ATTRIBUTES, TSDataType.TEXT),
          new ColumnHeader(DEADBAND, TSDataType.TEXT),
          new ColumnHeader(DEADBAND_PARAMETERS, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesWithSgColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DEVICE, TSDataType.TEXT),
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(IS_ALIGNED, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DEVICE, TSDataType.TEXT), new ColumnHeader(IS_ALIGNED, TSDataType.TEXT));

  public static final List<ColumnHeader> showTTLColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.INT64));

  public static final List<ColumnHeader> showStorageGroupColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.INT64),
          new ColumnHeader(SCHEMA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(DATA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(TIME_PARTITION_INTERVAL, TSDataType.INT64),
          new ColumnHeader(SCHEMA_REGION_NUM, TSDataType.INT32),
          new ColumnHeader(DATA_REGION_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> showChildPathsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(CHILD_PATHS, TSDataType.TEXT),
          new ColumnHeader(NODE_TYPES, TSDataType.TEXT));

  public static final List<ColumnHeader> showNodesInSchemaTemplateHeaders =
      ImmutableList.of(
          new ColumnHeader(CHILD_NODES, TSDataType.TEXT),
          new ColumnHeader(DATATYPE, TSDataType.TEXT),
          new ColumnHeader(ENCODING, TSDataType.TEXT),
          new ColumnHeader(COMPRESSION, TSDataType.TEXT));

  public static final List<ColumnHeader> showChildNodesColumnHeaders =
      ImmutableList.of(new ColumnHeader(CHILD_NODES, TSDataType.TEXT));

  public static final List<ColumnHeader> showVersionColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(VERSION, TSDataType.TEXT),
          new ColumnHeader(BUILD_INFO, TSDataType.TEXT));

  public static final List<ColumnHeader> showPathsUsingTemplateHeaders =
      ImmutableList.of(new ColumnHeader(PATHS, TSDataType.TEXT));

  public static final List<ColumnHeader> showPathSetTemplateHeaders =
      ImmutableList.of(new ColumnHeader(PATHS, TSDataType.TEXT));

  public static final List<ColumnHeader> countDevicesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COUNT_DEVICES, TSDataType.INT64));

  public static final List<ColumnHeader> countNodesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COUNT_NODES, TSDataType.INT64));

  public static final List<ColumnHeader> countLevelTimeSeriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN, TSDataType.TEXT),
          new ColumnHeader(COUNT_TIMESERIES, TSDataType.INT64));

  public static final List<ColumnHeader> countTimeSeriesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COUNT_TIMESERIES, TSDataType.INT64));

  public static final List<ColumnHeader> countStorageGroupColumnHeaders =
      ImmutableList.of(new ColumnHeader(COUNT_DATABASE, TSDataType.INT32));

  public static final List<ColumnHeader> showRegionColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(REGION_ID, TSDataType.INT32),
          new ColumnHeader(TYPE, TSDataType.TEXT),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(SERIES_SLOT_NUM, TSDataType.INT32),
          new ColumnHeader(TIME_SLOT_NUM, TSDataType.INT64),
          new ColumnHeader(DATA_NODE_ID, TSDataType.INT32),
          new ColumnHeader(RPC_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(RPC_PORT, TSDataType.INT32),
          new ColumnHeader(ROLE, TSDataType.TEXT));

  public static final List<ColumnHeader> showDataNodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(NODE_ID, TSDataType.INT32),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(RPC_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(RPC_PORT, TSDataType.INT32),
          new ColumnHeader(DATA_REGION_NUM, TSDataType.INT32),
          new ColumnHeader(SCHEMA_REGION_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> showConfigNodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(NODE_ID, TSDataType.INT32),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_PORT, TSDataType.INT32),
          new ColumnHeader(ROLE, TSDataType.TEXT));

  public static final List<ColumnHeader> showClusterColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(NODE_ID, TSDataType.INT32),
          new ColumnHeader(NODE_TYPE, TSDataType.TEXT),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_PORT, TSDataType.INT32));

  public static final List<ColumnHeader> showClusterDetailsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(NODE_ID, TSDataType.INT32),
          new ColumnHeader(NODE_TYPE, TSDataType.TEXT),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_PORT, TSDataType.INT32),
          new ColumnHeader(CONFIG_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(RPC_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(RPC_PORT, TSDataType.TEXT),
          new ColumnHeader(DATA_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(SCHEMA_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(MPP_PORT, TSDataType.TEXT));

  public static final List<ColumnHeader> showFunctionsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(FUNCTION_NAME, TSDataType.TEXT),
          new ColumnHeader(FUNCTION_TYPE, TSDataType.TEXT),
          new ColumnHeader(CLASS_NAME_UDF, TSDataType.TEXT));

  public static final List<ColumnHeader> showTriggersColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TRIGGER_NAME, TSDataType.TEXT),
          new ColumnHeader(EVENT, TSDataType.TEXT),
          new ColumnHeader(TYPE, TSDataType.TEXT),
          new ColumnHeader(STATE, TSDataType.TEXT),
          new ColumnHeader(PATH_PATTERN, TSDataType.TEXT),
          new ColumnHeader(CLASS_NAME, TSDataType.TEXT),
          new ColumnHeader(NODE_ID, TSDataType.TEXT));

  public static final List<ColumnHeader> showSchemaTemplateHeaders =
      ImmutableList.of(new ColumnHeader(TEMPLATE_NAME, TSDataType.TEXT));

  public static final List<ColumnHeader> showPipeSinkTypeColumnHeaders =
      ImmutableList.of(new ColumnHeader(TYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> showPipeSinkColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(NAME, TSDataType.TEXT),
          new ColumnHeader(TYPE, TSDataType.TEXT),
          new ColumnHeader(ATTRIBUTES, TSDataType.TEXT));

  public static final List<ColumnHeader> showPipeColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(CREATE_TIME, TSDataType.TEXT),
          new ColumnHeader(NAME, TSDataType.TEXT),
          new ColumnHeader(ROLE, TSDataType.TEXT),
          new ColumnHeader(REMOTE, TSDataType.TEXT),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(ATTRIBUTES, TSDataType.TEXT),
          new ColumnHeader(MESSAGE, TSDataType.TEXT));

  public static final List<ColumnHeader> selectIntoColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(SOURCE_COLUMN, TSDataType.TEXT),
          new ColumnHeader(TARGET_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(WRITTEN, TSDataType.INT32));

  public static final List<ColumnHeader> selectIntoAlignByDeviceColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(SOURCE_DEVICE, TSDataType.TEXT),
          new ColumnHeader(SOURCE_COLUMN, TSDataType.TEXT),
          new ColumnHeader(TARGET_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(WRITTEN, TSDataType.INT32));

  public static final List<ColumnHeader> getRegionIdColumnHeaders =
      ImmutableList.of(new ColumnHeader(REGION_ID, TSDataType.INT32));

  public static final List<ColumnHeader> getTimeSlotListColumnHeaders =
      ImmutableList.of(new ColumnHeader(TIME_SLOT_NUM, TSDataType.INT64));

  public static final List<ColumnHeader> getSeriesSlotListColumnHeaders =
      ImmutableList.of(new ColumnHeader(SERIES_SLOT_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> showContinuousQueriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(CQID, TSDataType.TEXT),
          new ColumnHeader(QUERY, TSDataType.TEXT),
          new ColumnHeader(STATE, TSDataType.TEXT));
}
