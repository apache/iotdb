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
  public static final String COLUMN_VALUE = "value";
  public static final String COLUMN_DEVICE = "Device";

  // column names for schema statement
  public static final String COLUMN_STORAGE_GROUP = "storage group";
  public static final String COLUMN_TIMESERIES = "timeseries";
  public static final String COLUMN_TIMESERIES_ALIAS = "alias";
  public static final String COLUMN_TIMESERIES_DATATYPE = "dataType";
  public static final String COLUMN_TIMESERIES_ENCODING = "encoding";
  public static final String COLUMN_TIMESERIES_COMPRESSION = "compression";
  public static final String COLUMN_DEVICES = "devices";
  public static final String COLUMN_TAGS = "tags";
  public static final String COLUMN_ATTRIBUTES = "attributes";
  public static final String COLUMN_IS_ALIGNED = "isAligned";
  public static final String COLUMN_COUNT = "count";
  public static final String COLUMN_TTL = "ttl";
  public static final String COLUMN_SCHEMA_REPLICATION_FACTOR = "SchemaReplicationFactor";
  public static final String COLUMN_DATA_REPLICATION_FACTOR = "DataReplicationFactor";
  public static final String COLUMN_TIME_PARTITION_INTERVAL = "TimePartitionInterval";
  public static final String COLUMN_CHILDPATHS = "child paths";
  public static final String COLUMN_NODETYPES = "node types";
  public static final String COLUMN_CHILDNODES = "child nodes";
  public static final String COLUMN_VERSION = "version";
  public static final String COLUMN_PATHS = "paths";

  // column names for count statement
  public static final String COLUMN_COLUMN = "column";
  public static final String COLUMN_COUNT_DEVICES = "count(devices)";
  public static final String COLUMN_COUNT_NODES = "count(nodes)";
  public static final String COLUMN_COUNT_TIMESERIES = "count(timeseries)";
  public static final String COLUMN_COUNT_STORAGE_GROUP = "count(storage group)";

  // column names for show cluster statement
  public static final String COLUMN_NODE_ID = "NodeID";
  public static final String COLUMN_NODE_TYPE = "NodeType";
  public static final String COLUMN_STATUS = "Status";
  public static final String COLUMN_HOST = "Host";
  public static final String COLUMN_PORT = "Port";

  // column names for show functions statement
  public static final String COLUMN_FUNCTION_NAME = "function name";
  public static final String COLUMN_FUNCTION_TYPE = "function type";
  public static final String COLUMN_FUNCTION_CLASS = "class name (UDF)";

  // column names for show region statement
  public static final String COLUMN_REGION_ID = "RegionId";
  public static final String COLUMN_TYPE = "Type";
  public static final String COLUMN_DATANODE_ID = "DataNodeId";
  public static final String COLUMN_SERIES_SLOTS = "SeriesSlots";
  public static final String COLUMN_TIME_SLOTS = "TimeSlots";
  public static final String COLUMN_ROLE = "Role";

  // column names for show datanodes
  public static final String COLUMN_DATA_REGION_NUM = "DataRegionNum";
  public static final String COLUMN_SCHEMA_REGION_NUM = "SchemaRegionNum";

  // column names for show schema template statement
  public static final String COLUMN_TEMPLATE_NAME = "template name";

  public static final List<ColumnHeader> lastQueryColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_VALUE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TIMESERIES_DATATYPE, TSDataType.TEXT));

  public static final List<ColumnHeader> showTimeSeriesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_TIMESERIES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TIMESERIES_ALIAS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TIMESERIES_DATATYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TIMESERIES_ENCODING, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TIMESERIES_COMPRESSION, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TAGS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_ATTRIBUTES, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesWithSgColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_DEVICES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT),
          new ColumnHeader(COLUMN_IS_ALIGNED, TSDataType.TEXT));

  public static final List<ColumnHeader> showDevicesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_DEVICES, TSDataType.TEXT),
          new ColumnHeader(COLUMN_IS_ALIGNED, TSDataType.TEXT));

  public static final List<ColumnHeader> showTTLColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TTL, TSDataType.INT64));

  public static final List<ColumnHeader> showStorageGroupColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT),
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
          new ColumnHeader(COLUMN_TIMESERIES_DATATYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TIMESERIES_ENCODING, TSDataType.TEXT),
          new ColumnHeader(COLUMN_TIMESERIES_COMPRESSION, TSDataType.TEXT));

  public static final List<ColumnHeader> showChildNodesColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_CHILDNODES, TSDataType.TEXT));

  public static final List<ColumnHeader> showVersionColumnHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_VERSION, TSDataType.TEXT));

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
      ImmutableList.of(new ColumnHeader(COLUMN_COUNT_STORAGE_GROUP, TSDataType.INT32));

  public static final List<ColumnHeader> showRegionColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_REGION_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT),
          new ColumnHeader(COLUMN_SERIES_SLOTS, TSDataType.INT64),
          new ColumnHeader(COLUMN_TIME_SLOTS, TSDataType.INT64),
          new ColumnHeader(COLUMN_DATANODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_HOST, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PORT, TSDataType.INT32),
          new ColumnHeader(COLUMN_ROLE, TSDataType.TEXT));

  public static final List<ColumnHeader> showDataNodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_HOST, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PORT, TSDataType.INT32),
          new ColumnHeader(COLUMN_DATA_REGION_NUM, TSDataType.INT32),
          new ColumnHeader(COLUMN_SCHEMA_REGION_NUM, TSDataType.INT32));

  public static final List<ColumnHeader> showConfigNodesColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_HOST, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PORT, TSDataType.INT32));

  public static final List<ColumnHeader> showClusterColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_NODE_ID, TSDataType.INT32),
          new ColumnHeader(COLUMN_NODE_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_STATUS, TSDataType.TEXT),
          new ColumnHeader(COLUMN_HOST, TSDataType.TEXT),
          new ColumnHeader(COLUMN_PORT, TSDataType.INT32));

  public static final List<ColumnHeader> showFunctionsColumnHeaders =
      ImmutableList.of(
          new ColumnHeader(COLUMN_FUNCTION_NAME, TSDataType.TEXT),
          new ColumnHeader(COLUMN_FUNCTION_TYPE, TSDataType.TEXT),
          new ColumnHeader(COLUMN_FUNCTION_CLASS, TSDataType.TEXT));

  public static final List<ColumnHeader> showSchemaTemplateHeaders =
      ImmutableList.of(new ColumnHeader(COLUMN_TEMPLATE_NAME, TSDataType.TEXT));
}
