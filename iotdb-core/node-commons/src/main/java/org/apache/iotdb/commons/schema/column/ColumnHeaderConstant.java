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

package org.apache.iotdb.commons.schema.column;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;

import java.util.List;

public class ColumnHeaderConstant {

  private ColumnHeaderConstant() {
    // forbidding instantiation
  }

  // column names for query statement
  public static final String TIME = "Time";
  public static final String ENDTIME = "__endTime";
  public static final String VALUE = "Value";
  public static final String DEVICE = "Device";
  public static final String DEVICE_ID = "DeviceId";
  public static final String DEVICE_TYPE = "DeviceType";
  public static final String EXPLAIN_ANALYZE = "Explain Analyze";

  // column names for schema statement
  public static final String DATABASE = "Database";
  public static final String TIMESERIES = "Timeseries";
  public static final String ALIAS = "Alias";
  public static final String DATATYPE = "DataType";
  public static final String ENCODING = "Encoding";
  public static final String COMPRESSION = "Compression";
  public static final String TAGS = "Tags";
  public static final String ATTRIBUTES = "Attributes";
  public static final String NOTES = "Notes";
  public static final String DEADBAND = "Deadband";
  public static final String DEADBAND_PARAMETERS = "DeadbandParameters";
  public static final String IS_ALIGNED = "IsAligned";
  public static final String TEMPLATE = "Template";

  public static final String COUNT = "Count";
  public static final String COLUMN_TTL = "TTL(ms)";
  public static final String SCHEMA_REPLICATION_FACTOR = "SchemaReplicationFactor";
  public static final String DATA_REPLICATION_FACTOR = "DataReplicationFactor";
  public static final String TIME_PARTITION_ORIGIN = "TimePartitionOrigin";
  public static final String TIME_PARTITION_INTERVAL = "TimePartitionInterval";
  public static final String SCHEMA_REGION_GROUP_NUM = "SchemaRegionGroupNum";
  public static final String MIN_SCHEMA_REGION_GROUP_NUM = "MinSchemaRegionGroupNum";
  public static final String MAX_SCHEMA_REGION_GROUP_NUM = "MaxSchemaRegionGroupNum";
  public static final String DATA_REGION_GROUP_NUM = "DataRegionGroupNum";
  public static final String MIN_DATA_REGION_GROUP_NUM = "MinDataRegionGroupNum";
  public static final String MAX_DATA_REGION_GROUP_NUM = "MaxDataRegionGroupNum";
  public static final String CHILD_PATHS = "ChildPaths";
  public static final String NODE_TYPES = "NodeTypes";
  public static final String CHILD_NODES = "ChildNodes";
  public static final String VERSION = "Version";
  public static final String BUILD_INFO = "BuildInfo";
  public static final String PATHS = "Paths";
  public static final String PATH = "Path";
  public static final String VARIABLE = "Variable";
  public static final String SCOPE = "Scope";

  // column names for count statement
  public static final String COLUMN = "Column";
  public static final String COUNT_DEVICES = "count(devices)";
  public static final String COUNT_NODES = "count(nodes)";
  public static final String COUNT_TIMESERIES = "count(timeseries)";
  public static final String COUNT_DATABASE = "count(database)";
  public static final String COUNT_INSTANCES = "Count(instances)";

  // column names for show cluster and show cluster details statements
  public static final String NODE_ID = "NodeID";
  public static final String NODE_TYPE = "NodeType";
  public static final String STATUS = "Status";
  public static final String INTERNAL_ADDRESS = "InternalAddress";
  public static final String INTERNAL_PORT = "InternalPort";
  public static final String CONFIG_CONSENSUS_PORT = "ConfigConsensusPort";
  public static final String RPC_ADDRESS = "RpcAddress";
  public static final String RPC_PORT = "RpcPort";
  public static final String DATA_CONSENSUS_PORT = "DataConsensusPort";
  public static final String SCHEMA_CONSENSUS_PORT = "SchemaConsensusPort";
  public static final String MPP_PORT = "MppPort";

  // column names for show clusterId statement
  public static final String CLUSTER_ID = "ClusterId";

  // column names for verify connection statement
  public static final String SERVICE_PROVIDER = "ServiceProvider";
  public static final String SENDER = "Sender";
  public static final String CONNECTION = "Connection";

  // column names for show functions statement
  public static final String FUNCTION_NAME = "FunctionName";
  public static final String FUNCTION_TYPE = "FunctionType";
  public static final String CLASS_NAME_UDF = "ClassName(UDF)";
  public static final String FUNCTION_STATE = "State";

  // column names for show triggers statement
  public static final String TRIGGER_NAME = "TriggerName";
  public static final String EVENT = "Event";
  public static final String STATE = "State";
  public static final String MODEL_TYPE = "ModelType";
  public static final String CONFIGS = "Configs";
  public static final String PATH_PATTERN = "PathPattern";
  public static final String CLASS_NAME = "ClassName";

  // column names for show services statement
  public static final String SERVICE_NAME = "ServiceName";
  public static final String SERVICE_TYPE = "ServiceType";

  public static final String SERVICE_NAME_TABLE_MODEL = "service_name";
  public static final String SERVICE_TYPE_TABLE_MODEL = "service_type";

  // column names for show pipe plugins statement
  public static final String PLUGIN_NAME = "PluginName";
  public static final String PLUGIN_TYPE = "PluginType";
  public static final String PLUGIN_JAR = "PluginJar";

  // column names for show topics statement
  public static final String TOPIC_NAME = "TopicName";
  public static final String TOPIC_CONFIGS = "TopicConfigs";

  // column names for show subscriptions statement
  public static final String CONSUMER_GROUP_NAME = "ConsumerGroupName";
  public static final String SUBSCRIBED_CONSUMERS = "SubscribedConsumers";
  public static final String SUBSCRIPTION_ID = "SubscriptionID";

  // show cluster status
  public static final String NODE_TYPE_CONFIG_NODE = "ConfigNode";
  public static final String NODE_TYPE_DATA_NODE = "DataNode";
  public static final String NODE_TYPE_AI_NODE = "AINode";
  public static final String COLUMN_CLUSTER_NAME = "ClusterName";
  public static final String CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS =
      "ConfigNodeConsensusProtocolClass";
  public static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "DataRegionConsensusProtocolClass";
  public static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "SchemaRegionConsensusProtocolClass";
  public static final String SERIES_SLOT_NUM = "SeriesSlotNum";
  public static final String SERIES_SLOT_EXECUTOR_CLASS = "SeriesSlotExecutorClass";
  public static final String SCHEMA_REGION_PER_DATA_NODE = "SchemaRegionPerDataNode";
  public static final String DATA_REGION_PER_DATA_NODE = "DataRegionPerDataNode";
  public static final String READ_CONSISTENCY_LEVEL = "ReadConsistencyLevel";
  public static final String DISK_SPACE_WARNING_THRESHOLD = "DiskSpaceWarningThreshold";

  public static final String TIMESTAMP_PRECISION = "TimestampPrecision";

  // column names for show region statement
  public static final String REGION_ID = "RegionId";
  public static final String TYPE = "Type";
  public static final String DATA_NODE_ID = "DataNodeId";
  public static final String TIME_SLOT_NUM = "TimeSlotNum";
  public static final String SERIES_SLOT_ID = "SeriesSlotId";
  public static final String TIME_PARTITION = "TimePartition";
  public static final String COUNT_TIME_PARTITION = "count(timePartition)";
  public static final String START_TIME = "StartTime";
  public static final String ROLE = "Role";
  public static final String MAX_SESSION_PER_USER = "MaxSessionPerUser";
  public static final String MIN_SESSION_PER_USER = "MinSessionPerUser";
  public static final String CREATE_TIME = "CreateTime";
  public static final String TSFILE_SIZE = "TsFileSize";
  public static final String COMPRESSION_RATIO = "CompressionRatio";

  // column names for show datanodes
  public static final String SCHEMA_REGION_NUM = "SchemaRegionNum";
  public static final String DATA_REGION_NUM = "DataRegionNum";

  // column names for show device template statement
  public static final String TEMPLATE_NAME = "TemplateName";

  // column names for show pipe sink
  public static final String NAME = "Name";

  // column names for show pipe
  public static final String ID = "ID";
  public static final String CREATION_TIME = "CreationTime";
  public static final String PIPE_EXTRACTOR = "PipeSource";
  public static final String PIPE_PROCESSOR = "PipeProcessor";
  public static final String PIPE_CONNECTOR = "PipeSink";
  public static final String EXCEPTION_MESSAGE = "ExceptionMessage";
  public static final String REMAINING_EVENT_COUNT = "RemainingEventCount";
  public static final String ESTIMATED_REMAINING_SECONDS = "EstimatedRemainingSeconds";

  // column names for select into
  public static final String SOURCE_DEVICE = "SourceDevice";
  public static final String SOURCE_COLUMN = "SourceColumn";
  public static final String TARGET_TIMESERIES = "TargetTimeseries";
  public static final String WRITTEN = "Written";
  public static final String ROWS = "Rows";

  // column names for show cq
  public static final String CQID = "CQId";
  public static final String QUERY = "Query";

  // column names for show query processlist
  public static final String QUERY_ID = "QueryId";
  public static final String ELAPSED_TIME = "ElapsedTime";
  public static final String STATEMENT = "Statement";

  // column names for show idle connection
  public static final String DATANODE_ID = "datanode_id";
  public static final String USERID = "user_id";
  public static final String SESSION_ID = "session_id";
  public static final String USER_NAME = "user_name";
  public static final String LAST_ACTIVE_TIME = "last_active_time";
  public static final String CLIENT_IP = "client_ip";

  public static final String QUERY_ID_TABLE_MODEL = "query_id";
  public static final String DATA_NODE_ID_TABLE_MODEL = "datanode_id";
  public static final String START_TIME_TABLE_MODEL = "start_time";
  public static final String ELAPSED_TIME_TABLE_MODEL = "elapsed_time";

  // column names for current_queries and queries_costs_histogram
  public static final String STATE_TABLE_MODEL = "state";
  public static final String END_TIME_TABLE_MODEL = "end_time";
  public static final String COST_TIME = "cost_time";
  public static final String STATEMENT_TABLE_MODEL = "statement";
  public static final String USER_TABLE_MODEL = "user";
  public static final String BIN = "bin";
  public static final String NUMS = "nums";

  public static final String TABLE_NAME_TABLE_MODEL = "table_name";
  public static final String TABLE_TYPE_TABLE_MODEL = "table_type";
  public static final String COLUMN_NAME_TABLE_MODEL = "column_name";

  public static final String SCHEMA_REPLICATION_FACTOR_TABLE_MODEL = "schema_replication_factor";
  public static final String DATA_REPLICATION_FACTOR_TABLE_MODEL = "data_replication_factor";
  public static final String TIME_PARTITION_INTERVAL_TABLE_MODEL = "time_partition_interval";
  public static final String SCHEMA_REGION_GROUP_NUM_TABLE_MODEL = "schema_region_group_num";
  public static final String DATA_REGION_GROUP_NUM_TABLE_MODEL = "data_region_group_num";

  public static final String REGION_ID_TABLE_MODEL = "region_id";
  public static final String DATANODE_ID_TABLE_MODEL = "datanode_id";
  public static final String SERIES_SLOT_NUM_TABLE_MODEL = "series_slot_num";
  public static final String TIME_SLOT_NUM_TABLE_MODEL = "time_slot_num";
  public static final String RPC_ADDRESS_TABLE_MODEL = "rpc_address";
  public static final String RPC_PORT_TABLE_MODEL = "rpc_port";
  public static final String INTERNAL_ADDRESS_TABLE_MODEL = "internal_address";
  public static final String CREATE_TIME_TABLE_MODEL = "create_time";
  public static final String TS_FILE_SIZE_BYTES_TABLE_MODEL = "tsfile_size_bytes";
  public static final String COMPRESSION_RATIO_TABLE_MODEL = "compression_ratio";

  public static final String CREATION_TIME_TABLE_MODEL = "creation_time";
  public static final String PIPE_SOURCE_TABLE_MODEL = "pipe_source";
  public static final String PIPE_PROCESSOR_TABLE_MODEL = "pipe_processor";
  public static final String PIPE_SINK_TABLE_MODEL = "pipe_sink";
  public static final String EXCEPTION_MESSAGE_TABLE_MODEL = "exception_message";
  public static final String REMAINING_EVENT_COUNT_TABLE_MODEL = "remaining_event_count";
  public static final String ESTIMATED_REMAINING_SECONDS_TABLE_MODEL =
      "estimated_remaining_seconds";

  public static final String PLUGIN_NAME_TABLE_MODEL = "plugin_name";
  public static final String PLUGIN_TYPE_TABLE_MODEL = "plugin_type";
  public static final String CLASS_NAME_TABLE_MODEL = "class_name";
  public static final String PLUGIN_JAR_TABLE_MODEL = "plugin_jar";

  public static final String TOPIC_NAME_TABLE_MODEL = "topic_name";
  public static final String TOPIC_CONFIGS_TABLE_MODEL = "topic_configs";

  public static final String CONSUMER_GROUP_NAME_TABLE_MODEL = "consumer_group_name";
  public static final String SUBSCRIBED_CONSUMERS_TABLE_MODEL = "subscribed_consumers";

  public static final String VIEW_DEFINITION_TABLE_MODEL = "view_definition";

  public static final String MODEL_ID_TABLE_MODEL = "model_id";
  public static final String MODEL_TYPE_TABLE_MODEL = "model_type";

  public static final String FUNCTION_NAME_TABLE_MODEL = "function_table";
  public static final String FUNCTION_TYPE_TABLE_MODEL = "function_type";
  public static final String CLASS_NAME_UDF_TABLE_MODEL = "class_name(udf)";

  public static final String NODE_ID_TABLE_MODEL = "node_id";
  public static final String NODE_TYPE_TABLE_MODEL = "node_type";
  public static final String INTERNAL_PORT_TABLE_MODEL = "internal_port";
  public static final String BUILD_INFO_TABLE_MODEL = "build_info";

  public static final String CONFIG_CONSENSUS_PORT_TABLE_MODEL = "config_consensus_port";

  public static final String DATA_REGION_NUM_TABLE_MODEL = "data_region_num";
  public static final String SCHEMA_REGION_NUM_TABLE_MODEL = "schema_region_num";
  public static final String MPP_PORT_TABLE_MODEL = "mpp_port";
  public static final String SCHEMA_CONSENSUS_PORT_TABLE_MODEL = "schema_consensus_port";
  public static final String DATA_CONSENSUS_PORT_TABLE_MODEL = "data_consensus_port";

  // column names for show space quota
  public static final String QUOTA_TYPE = "QuotaType";
  public static final String LIMIT = "Limit";
  public static final String USED = "Used";

  // column names for show throttle quota
  public static final String USER = "User";
  public static final String USER_ID = "UserId";
  public static final String READ_WRITE = "Read/Write";

  // column names for show models/trials
  public static final String MODEL_ID = "ModelId";

  // column names for views (e.g. logical view)
  public static final String VIEW_TYPE = "ViewType";
  public static final String SOURCE = "Source";

  // column names for show current timestamp
  public static final String CURRENT_TIMESTAMP = "CurrentTimestamp";

  // column names for keywords
  public static final String WORD = "word";
  public static final String RESERVED = "reserved";

  // column names for table query
  public static final String COLUMN_NAME = "ColumnName";
  public static final String COLUMN_DATA_TYPE = "DataType";
  public static final String COLUMN_CATEGORY = "Category";
  public static final String TABLE_NAME = "TableName";
  public static final String PRIVILEGES = "Privileges";
  public static final String COMMENT = "Comment";
  public static final String TABLE_TYPE = "TableType";

  public static final String VIEW = "View";
  public static final String CREATE_VIEW = "Create View";
  public static final String TABLE = "Table";
  public static final String CREATE_TABLE = "Create Table";

  public static final String GRANT_OPTION = "GrantOption";

  public static final String CURRENT_USER = "CurrentUser";

  public static final String CURRENT_DATABASE = "CurrentDatabase";

  public static final String CURRENT_SQL_DIALECT = "CurrentSqlDialect";

  public static final String SHOW_CONFIGURATIONS_NAME = "name";
  public static final String SHOW_CONFIGURATIONS_VALUE = "value";
  public static final String SHOW_CONFIGURATIONS_DEFAULT_VALUE = "default_value";
  public static final String SHOW_CONFIGURATIONS_DESCRIPTION = "description";

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
          new ColumnHeader(DEADBAND_PARAMETERS, TSDataType.TEXT),
          new ColumnHeader(VIEW_TYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesWithSgColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DEVICE, TSDataType.TEXT),
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(IS_ALIGNED, TSDataType.TEXT),
          new ColumnHeader(TEMPLATE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DEVICE, TSDataType.TEXT),
          new ColumnHeader(IS_ALIGNED, TSDataType.TEXT),
          new ColumnHeader(TEMPLATE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.TEXT));
  public static final List<ColumnHeader> showTTLColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DEVICE, TSDataType.TEXT), new ColumnHeader(COLUMN_TTL, TSDataType.TEXT));

  public static final List<ColumnHeader> showDatabasesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(SCHEMA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(DATA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(TIME_PARTITION_ORIGIN, TSDataType.INT64),
          new ColumnHeader(TIME_PARTITION_INTERVAL, TSDataType.INT64));

  public static final List<ColumnHeader> showDatabasesDetailColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(SCHEMA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(DATA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(TIME_PARTITION_ORIGIN, TSDataType.INT64),
          new ColumnHeader(TIME_PARTITION_INTERVAL, TSDataType.INT64),
          new ColumnHeader(SCHEMA_REGION_GROUP_NUM, TSDataType.INT32),
          new ColumnHeader(MIN_SCHEMA_REGION_GROUP_NUM, TSDataType.INT32),
          new ColumnHeader(MAX_SCHEMA_REGION_GROUP_NUM, TSDataType.INT32),
          new ColumnHeader(DATA_REGION_GROUP_NUM, TSDataType.INT32),
          new ColumnHeader(MIN_DATA_REGION_GROUP_NUM, TSDataType.INT32),
          new ColumnHeader(MAX_DATA_REGION_GROUP_NUM, TSDataType.INT32));

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
          new ColumnHeader(INTERNAL_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(ROLE, TSDataType.TEXT),
          new ColumnHeader(CREATE_TIME, TSDataType.TEXT),
          new ColumnHeader(TSFILE_SIZE, TSDataType.TEXT),
          new ColumnHeader(COMPRESSION_RATIO, TSDataType.DOUBLE));

  public static final List<ColumnHeader> showAINodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(NODE_ID, TSDataType.INT32),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(INTERNAL_PORT, TSDataType.INT32));

  public static final List<ColumnHeader> showDataNodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(NODE_ID, TSDataType.INT32),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(RPC_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(RPC_PORT, TSDataType.INT32),
          new ColumnHeader(DATA_REGION_NUM, TSDataType.INT32),
          new ColumnHeader(SCHEMA_REGION_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> showAvailableUrlsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(RPC_ADDRESS, TSDataType.TEXT),
          new ColumnHeader(RPC_PORT, TSDataType.INT32));

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
          new ColumnHeader(INTERNAL_PORT, TSDataType.INT32),
          new ColumnHeader(VERSION, TSDataType.TEXT),
          new ColumnHeader(BUILD_INFO, TSDataType.TEXT));

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
          new ColumnHeader(MPP_PORT, TSDataType.TEXT),
          new ColumnHeader(SCHEMA_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(DATA_CONSENSUS_PORT, TSDataType.TEXT),
          new ColumnHeader(VERSION, TSDataType.TEXT),
          new ColumnHeader(BUILD_INFO, TSDataType.TEXT));

  public static final List<ColumnHeader> showClusterIdColumnHeaders =
      ImmutableList.of(new ColumnHeader(CLUSTER_ID, TSDataType.TEXT));

  public static final List<ColumnHeader> testConnectionColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(SERVICE_PROVIDER, TSDataType.TEXT),
          new ColumnHeader(SENDER, TSDataType.TEXT),
          new ColumnHeader(CONNECTION, TSDataType.TEXT));

  public static final List<ColumnHeader> showVariablesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(VARIABLE, TSDataType.TEXT), new ColumnHeader(VALUE, TSDataType.TEXT));

  public static final List<ColumnHeader> showFunctionsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(FUNCTION_NAME, TSDataType.TEXT),
          new ColumnHeader(FUNCTION_TYPE, TSDataType.TEXT),
          new ColumnHeader(CLASS_NAME_UDF, TSDataType.TEXT),
          new ColumnHeader(FUNCTION_STATE, TSDataType.TEXT));

  public static final List<ColumnHeader> showTriggersColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TRIGGER_NAME, TSDataType.TEXT),
          new ColumnHeader(EVENT, TSDataType.TEXT),
          new ColumnHeader(TYPE, TSDataType.TEXT),
          new ColumnHeader(STATE, TSDataType.TEXT),
          new ColumnHeader(PATH_PATTERN, TSDataType.TEXT),
          new ColumnHeader(CLASS_NAME, TSDataType.TEXT),
          new ColumnHeader(NODE_ID, TSDataType.TEXT));

  public static final List<ColumnHeader> showExternalServiceColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(SERVICE_NAME, TSDataType.STRING),
          new ColumnHeader(DATA_NODE_ID, TSDataType.INT32),
          new ColumnHeader(STATE, TSDataType.STRING));

  public static final List<ColumnHeader> showPipePluginsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(PLUGIN_NAME, TSDataType.TEXT),
          new ColumnHeader(PLUGIN_TYPE, TSDataType.TEXT),
          new ColumnHeader(CLASS_NAME, TSDataType.TEXT),
          new ColumnHeader(PLUGIN_JAR, TSDataType.TEXT));

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
          new ColumnHeader(ID, TSDataType.TEXT),
          new ColumnHeader(CREATION_TIME, TSDataType.TEXT),
          new ColumnHeader(STATE, TSDataType.TEXT),
          new ColumnHeader(PIPE_EXTRACTOR, TSDataType.TEXT),
          new ColumnHeader(PIPE_PROCESSOR, TSDataType.TEXT),
          new ColumnHeader(PIPE_CONNECTOR, TSDataType.TEXT),
          new ColumnHeader(EXCEPTION_MESSAGE, TSDataType.TEXT),
          new ColumnHeader(REMAINING_EVENT_COUNT, TSDataType.TEXT),
          new ColumnHeader(ESTIMATED_REMAINING_SECONDS, TSDataType.TEXT));

  public static final List<ColumnHeader> showTopicColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TOPIC_NAME, TSDataType.TEXT),
          new ColumnHeader(TOPIC_CONFIGS, TSDataType.TEXT));

  public static final List<ColumnHeader> showSubscriptionColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(SUBSCRIPTION_ID, TSDataType.TEXT),
          new ColumnHeader(TOPIC_NAME, TSDataType.TEXT),
          new ColumnHeader(CONSUMER_GROUP_NAME, TSDataType.TEXT),
          new ColumnHeader(SUBSCRIBED_CONSUMERS, TSDataType.TEXT));

  public static final List<ColumnHeader> selectIntoColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(SOURCE_COLUMN, TSDataType.TEXT),
          new ColumnHeader(TARGET_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(WRITTEN, TSDataType.INT64));

  public static final List<ColumnHeader> selectIntoAlignByDeviceColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(SOURCE_DEVICE, TSDataType.TEXT),
          new ColumnHeader(SOURCE_COLUMN, TSDataType.TEXT),
          new ColumnHeader(TARGET_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(WRITTEN, TSDataType.INT64));

  public static final List<ColumnHeader> selectIntoTableColumnHeaders =
      ImmutableList.of(new ColumnHeader(ROWS, TSDataType.INT64));

  public static final List<ColumnHeader> getRegionIdColumnHeaders =
      ImmutableList.of(new ColumnHeader(REGION_ID, TSDataType.INT32));

  public static final List<ColumnHeader> getTimeSlotListColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TIME_PARTITION, TSDataType.INT64),
          new ColumnHeader(START_TIME, TSDataType.TEXT));

  public static final List<ColumnHeader> countTimeSlotListColumnHeaders =
      ImmutableList.of(new ColumnHeader(COUNT_TIME_PARTITION, TSDataType.INT64));

  public static final List<ColumnHeader> getSeriesSlotListColumnHeaders =
      ImmutableList.of(new ColumnHeader(SERIES_SLOT_ID, TSDataType.INT32));

  public static final List<ColumnHeader> showContinuousQueriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(CQID, TSDataType.TEXT),
          new ColumnHeader(QUERY, TSDataType.TEXT),
          new ColumnHeader(STATE, TSDataType.TEXT));

  public static final List<ColumnHeader> showQueriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(QUERY_ID, TSDataType.TEXT),
          new ColumnHeader(DATA_NODE_ID, TSDataType.INT32),
          new ColumnHeader(ELAPSED_TIME, TSDataType.FLOAT),
          new ColumnHeader(STATEMENT, TSDataType.TEXT));

  public static final List<ColumnHeader> showSpaceQuotaColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(QUOTA_TYPE, TSDataType.TEXT),
          new ColumnHeader(LIMIT, TSDataType.TEXT),
          new ColumnHeader(USED, TSDataType.TEXT));

  public static final List<ColumnHeader> showThrottleQuotaColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(USER, TSDataType.TEXT),
          new ColumnHeader(QUOTA_TYPE, TSDataType.TEXT),
          new ColumnHeader(LIMIT, TSDataType.TEXT),
          new ColumnHeader(READ_WRITE, TSDataType.TEXT));

  public static final List<ColumnHeader> showModelsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(MODEL_ID, TSDataType.TEXT),
          new ColumnHeader(MODEL_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_CATEGORY, TSDataType.TEXT),
          new ColumnHeader(STATE, TSDataType.TEXT));

  public static final List<ColumnHeader> showLoadedModelsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DEVICE_ID, TSDataType.TEXT),
          new ColumnHeader(MODEL_ID, TSDataType.TEXT),
          new ColumnHeader(COUNT_INSTANCES, TSDataType.INT32));

  public static final List<ColumnHeader> showAIDevicesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DEVICE_ID, TSDataType.TEXT),
          new ColumnHeader(DEVICE_TYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> showLogicalViewColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(DATATYPE, TSDataType.TEXT),
          new ColumnHeader(TAGS, TSDataType.TEXT),
          new ColumnHeader(ATTRIBUTES, TSDataType.TEXT),
          new ColumnHeader(VIEW_TYPE, TSDataType.TEXT),
          new ColumnHeader(SOURCE, TSDataType.TEXT));

  public static final List<ColumnHeader> showDBColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.TEXT),
          new ColumnHeader(SCHEMA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(DATA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(TIME_PARTITION_INTERVAL, TSDataType.INT64));

  public static final List<ColumnHeader> showDBDetailsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(DATABASE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.TEXT),
          new ColumnHeader(SCHEMA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(DATA_REPLICATION_FACTOR, TSDataType.INT32),
          new ColumnHeader(TIME_PARTITION_INTERVAL, TSDataType.INT64),
          new ColumnHeader(SCHEMA_REGION_GROUP_NUM, TSDataType.INT32),
          new ColumnHeader(DATA_REGION_GROUP_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> describeTableColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATA_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_CATEGORY, TSDataType.TEXT));

  public static final List<ColumnHeader> describeTableDetailsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_DATA_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_CATEGORY, TSDataType.TEXT),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(COMMENT, TSDataType.TEXT));

  public static final List<ColumnHeader> showCreateViewColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(VIEW, TSDataType.TEXT), new ColumnHeader(CREATE_VIEW, TSDataType.TEXT));

  public static final List<ColumnHeader> showCreateTableColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TABLE, TSDataType.TEXT),
          new ColumnHeader(CREATE_TABLE, TSDataType.TEXT));

  public static final List<ColumnHeader> LIST_USER_COLUMN_HEADERS =
      ImmutableList.of(
          new ColumnHeader(USER_ID, TSDataType.INT64), new ColumnHeader(USER, TSDataType.TEXT));

  public static final List<ColumnHeader> showTablesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TABLE_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.TEXT));

  public static final List<ColumnHeader> showTablesDetailsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(TABLE_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.TEXT),
          new ColumnHeader(STATUS, TSDataType.TEXT),
          new ColumnHeader(COMMENT, TSDataType.TEXT),
          new ColumnHeader(TABLE_TYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> LIST_USER_OR_ROLE_PRIVILEGES_COLUMN_HEADERS =
      ImmutableList.of(
          new ColumnHeader(ROLE, TSDataType.TEXT),
          new ColumnHeader(SCOPE, TSDataType.TEXT),
          new ColumnHeader(PRIVILEGES, TSDataType.TEXT),
          new ColumnHeader(GRANT_OPTION, TSDataType.BOOLEAN));

  public static final List<ColumnHeader> SHOW_CURRENT_USER_COLUMN_HEADERS =
      ImmutableList.of(new ColumnHeader(CURRENT_USER, TSDataType.STRING));

  public static final List<ColumnHeader> SHOW_CURRENT_DATABASE_COLUMN_HEADERS =
      ImmutableList.of(new ColumnHeader(CURRENT_DATABASE, TSDataType.STRING));

  public static final List<ColumnHeader> SHOW_CURRENT_SQL_DIALECT_COLUMN_HEADERS =
      ImmutableList.of(new ColumnHeader(CURRENT_SQL_DIALECT, TSDataType.STRING));

  public static final List<ColumnHeader> SHOW_CURRENT_TIMESTAMP_COLUMN_HEADERS =
      ImmutableList.of(new ColumnHeader(CURRENT_TIMESTAMP, TSDataType.TIMESTAMP));

  public static final List<ColumnHeader> SHOW_CONFIGURATIONS_COLUMN_HEADERS =
      ImmutableList.of(
          new ColumnHeader(SHOW_CONFIGURATIONS_NAME, TSDataType.TEXT),
          new ColumnHeader(SHOW_CONFIGURATIONS_VALUE, TSDataType.TEXT),
          new ColumnHeader(SHOW_CONFIGURATIONS_DEFAULT_VALUE, TSDataType.TEXT));

  public static final List<ColumnHeader> SHOW_CONFIGURATIONS_COLUMN_HEADERS_WITH_DESCRIPTION =
      ImmutableList.of(
          new ColumnHeader(SHOW_CONFIGURATIONS_NAME, TSDataType.TEXT),
          new ColumnHeader(SHOW_CONFIGURATIONS_VALUE, TSDataType.TEXT),
          new ColumnHeader(SHOW_CONFIGURATIONS_DEFAULT_VALUE, TSDataType.TEXT),
          new ColumnHeader(SHOW_CONFIGURATIONS_DESCRIPTION, TSDataType.TEXT));
}
