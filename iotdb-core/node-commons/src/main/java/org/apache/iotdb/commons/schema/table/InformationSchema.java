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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;

import org.apache.tsfile.enums.TSDataType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class InformationSchema {
  public static final String INFORMATION_DATABASE = "information_schema";
  private static final Map<String, TsTable> schemaTables = new HashMap<>();

  public static final String QUERIES = "queries";
  public static final String DATABASES = "databases";
  public static final String TABLES = "tables";
  public static final String COLUMNS = "columns";
  public static final String REGIONS = "regions";
  public static final String PIPES = "pipes";
  public static final String PIPE_PLUGINS = "pipe_plugins";
  public static final String TOPICS = "topics";
  public static final String SUBSCRIPTIONS = "subscriptions";
  public static final String VIEWS = "views";
  public static final String FUNCTIONS = "functions";
  public static final String CONFIGURATIONS = "configurations";
  public static final String KEYWORDS = "keywords";
  public static final String NODES = "nodes";
  public static final String CONFIG_NODES = "config_nodes";
  public static final String DATA_NODES = "data_nodes";
  public static final String CONNECTIONS = "connections";
  public static final String CURRENT_QUERIES = "current_queries";
  public static final String QUERIES_COSTS_HISTOGRAM = "queries_costs_histogram";
  public static final String SERVICES = "services";

  static {
    final TsTable queriesTable = new TsTable(QUERIES);
    queriesTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.QUERY_ID_TABLE_MODEL, TSDataType.STRING));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.START_TIME_TABLE_MODEL, TSDataType.TIMESTAMP));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.DATA_NODE_ID_TABLE_MODEL, TSDataType.INT32));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.ELAPSED_TIME_TABLE_MODEL, TSDataType.FLOAT));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.STATEMENT_TABLE_MODEL, TSDataType.STRING));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.USER_TABLE_MODEL, TSDataType.STRING));
    queriesTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(QUERIES, queriesTable);

    final TsTable databaseTable = new TsTable(DATABASES);
    databaseTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COLUMN_TTL.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SCHEMA_REPLICATION_FACTOR_TABLE_MODEL, TSDataType.INT32));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATA_REPLICATION_FACTOR_TABLE_MODEL, TSDataType.INT32));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.TIME_PARTITION_INTERVAL_TABLE_MODEL, TSDataType.INT64));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SCHEMA_REGION_GROUP_NUM_TABLE_MODEL, TSDataType.INT32));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATA_REGION_GROUP_NUM_TABLE_MODEL, TSDataType.INT32));
    databaseTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(DATABASES, databaseTable);

    final TsTable tableTable = new TsTable(TABLES);
    tableTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.TABLE_NAME_TABLE_MODEL, TSDataType.STRING));
    tableTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COLUMN_TTL.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATUS.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COMMENT.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.TABLE_TYPE_TABLE_MODEL, TSDataType.STRING));
    tableTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(TABLES, tableTable);

    final TsTable columnTable = new TsTable(COLUMNS);
    columnTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.TABLE_NAME_TABLE_MODEL, TSDataType.STRING));
    columnTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.COLUMN_NAME_TABLE_MODEL, TSDataType.STRING));
    columnTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATATYPE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COLUMN_CATEGORY.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATUS.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COMMENT.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(COLUMNS, columnTable);

    final TsTable regionTable = new TsTable(REGIONS);
    regionTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.REGION_ID_TABLE_MODEL, TSDataType.INT32));
    regionTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.DATANODE_ID_TABLE_MODEL, TSDataType.INT32));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.TYPE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATUS.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SERIES_SLOT_NUM_TABLE_MODEL, TSDataType.INT32));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.TIME_SLOT_NUM_TABLE_MODEL, TSDataType.INT64));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.RPC_ADDRESS_TABLE_MODEL, TSDataType.STRING));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.RPC_PORT_TABLE_MODEL, TSDataType.INT32));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.INTERNAL_ADDRESS_TABLE_MODEL, TSDataType.STRING));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.ROLE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.CREATE_TIME_TABLE_MODEL, TSDataType.TIMESTAMP));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.TS_FILE_SIZE_BYTES_TABLE_MODEL, TSDataType.INT64));
    regionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COMPRESSION_RATIO_TABLE_MODEL, TSDataType.DOUBLE));
    regionTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(REGIONS, regionTable);

    final TsTable pipeTable = new TsTable(PIPES);
    pipeTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.ID.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.CREATION_TIME_TABLE_MODEL, TSDataType.TIMESTAMP));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.PIPE_SOURCE_TABLE_MODEL, TSDataType.STRING));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.PIPE_PROCESSOR_TABLE_MODEL, TSDataType.STRING));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.PIPE_SINK_TABLE_MODEL, TSDataType.STRING));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.EXCEPTION_MESSAGE_TABLE_MODEL, TSDataType.STRING));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.REMAINING_EVENT_COUNT_TABLE_MODEL, TSDataType.INT64));
    pipeTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.ESTIMATED_REMAINING_SECONDS_TABLE_MODEL, TSDataType.DOUBLE));
    pipeTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(PIPES, pipeTable);

    final TsTable pipePluginTable = new TsTable(PIPE_PLUGINS);
    pipePluginTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.PLUGIN_NAME_TABLE_MODEL, TSDataType.STRING));
    pipePluginTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.PLUGIN_TYPE_TABLE_MODEL, TSDataType.STRING));
    pipePluginTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.CLASS_NAME_TABLE_MODEL, TSDataType.STRING));
    pipePluginTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.PLUGIN_JAR_TABLE_MODEL, TSDataType.STRING));
    pipePluginTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(PIPE_PLUGINS, pipePluginTable);

    final TsTable topicTable = new TsTable(TOPICS);
    topicTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.TOPIC_NAME_TABLE_MODEL, TSDataType.STRING));
    topicTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.TOPIC_CONFIGS_TABLE_MODEL, TSDataType.STRING));
    topicTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(TOPICS, topicTable);

    final TsTable subscriptionTable = new TsTable(SUBSCRIPTIONS);
    subscriptionTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.TOPIC_NAME_TABLE_MODEL, TSDataType.STRING));
    subscriptionTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.CONSUMER_GROUP_NAME_TABLE_MODEL, TSDataType.STRING));
    subscriptionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SUBSCRIBED_CONSUMERS_TABLE_MODEL, TSDataType.STRING));
    subscriptionTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(SUBSCRIPTIONS, subscriptionTable);

    final TsTable viewTable = new TsTable(VIEWS);
    viewTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    viewTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.TABLE_NAME_TABLE_MODEL, TSDataType.STRING));
    viewTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.VIEW_DEFINITION_TABLE_MODEL, TSDataType.STRING));
    viewTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(VIEWS, viewTable);

    final TsTable functionTable = new TsTable(FUNCTIONS);
    functionTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.FUNCTION_NAME_TABLE_MODEL, TSDataType.STRING));
    functionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.FUNCTION_TYPE_TABLE_MODEL, TSDataType.STRING));
    functionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.CLASS_NAME_UDF_TABLE_MODEL, TSDataType.STRING));
    functionTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    functionTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(FUNCTIONS, functionTable);

    final TsTable configurationsTable = new TsTable(CONFIGURATIONS);
    configurationsTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.VARIABLE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    configurationsTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.VALUE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    configurationsTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(CONFIGURATIONS, configurationsTable);

    final TsTable keywordsTable = new TsTable(KEYWORDS);
    keywordsTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.WORD, TSDataType.STRING));
    keywordsTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.RESERVED, TSDataType.INT32));
    keywordsTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(KEYWORDS, keywordsTable);

    final TsTable nodesTable = new TsTable(NODES);
    nodesTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.NODE_ID_TABLE_MODEL, TSDataType.INT32));
    nodesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.NODE_TYPE_TABLE_MODEL, TSDataType.STRING));
    nodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATUS.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    nodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.INTERNAL_ADDRESS_TABLE_MODEL, TSDataType.STRING));
    nodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.INTERNAL_PORT_TABLE_MODEL, TSDataType.INT32));
    nodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.VERSION.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    nodesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.BUILD_INFO_TABLE_MODEL, TSDataType.STRING));
    nodesTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(NODES, nodesTable);

    final TsTable configNodesTable = new TsTable(CONFIG_NODES);
    configNodesTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.NODE_ID_TABLE_MODEL, TSDataType.INT32));
    configNodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.CONFIG_CONSENSUS_PORT_TABLE_MODEL, TSDataType.INT32));
    configNodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.ROLE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    configNodesTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(CONFIG_NODES, configNodesTable);

    final TsTable dataNodesTable = new TsTable(DATA_NODES);
    dataNodesTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.NODE_ID_TABLE_MODEL, TSDataType.INT32));
    dataNodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATA_REGION_NUM_TABLE_MODEL, TSDataType.INT32));
    dataNodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SCHEMA_REGION_NUM_TABLE_MODEL, TSDataType.INT32));
    dataNodesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.RPC_ADDRESS_TABLE_MODEL, TSDataType.STRING));
    dataNodesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.RPC_PORT_TABLE_MODEL, TSDataType.INT32));
    dataNodesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.MPP_PORT_TABLE_MODEL, TSDataType.INT32));
    dataNodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATA_CONSENSUS_PORT_TABLE_MODEL, TSDataType.INT32));
    dataNodesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SCHEMA_CONSENSUS_PORT_TABLE_MODEL, TSDataType.INT32));
    dataNodesTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(DATA_NODES, dataNodesTable);

    final TsTable connectionsTable = new TsTable(CONNECTIONS);
    connectionsTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.DATANODE_ID, TSDataType.STRING));
    connectionsTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.USERID, TSDataType.STRING));
    connectionsTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.SESSION_ID, TSDataType.STRING));
    connectionsTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.USER_NAME, TSDataType.STRING));
    connectionsTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.LAST_ACTIVE_TIME, TSDataType.TIMESTAMP));
    connectionsTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.CLIENT_IP, TSDataType.STRING));
    connectionsTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(CONNECTIONS, connectionsTable);

    final TsTable currentQueriesTable = new TsTable(CURRENT_QUERIES);
    currentQueriesTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.QUERY_ID_TABLE_MODEL, TSDataType.STRING));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.STATE_TABLE_MODEL, TSDataType.STRING));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.START_TIME_TABLE_MODEL, TSDataType.TIMESTAMP));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.END_TIME_TABLE_MODEL, TSDataType.TIMESTAMP));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.DATA_NODE_ID_TABLE_MODEL, TSDataType.INT32));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.COST_TIME, TSDataType.FLOAT));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.STATEMENT_TABLE_MODEL, TSDataType.STRING));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.USER_TABLE_MODEL, TSDataType.STRING));
    currentQueriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.CLIENT_IP, TSDataType.STRING));
    currentQueriesTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(CURRENT_QUERIES, currentQueriesTable);

    final TsTable queriesCostsHistogramTable = new TsTable(QUERIES_COSTS_HISTOGRAM);
    queriesCostsHistogramTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.BIN, TSDataType.STRING));
    queriesCostsHistogramTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.NUMS, TSDataType.INT32));
    queriesCostsHistogramTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.DATANODE_ID, TSDataType.INT32));
    queriesCostsHistogramTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(QUERIES_COSTS_HISTOGRAM, queriesCostsHistogramTable);

    final TsTable servicesTable = new TsTable(SERVICES);
    servicesTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.SERVICE_NAME_TABLE_MODEL, TSDataType.STRING));
    servicesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.DATA_NODE_ID_TABLE_MODEL, TSDataType.INT32));
    servicesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.STATE_TABLE_MODEL, TSDataType.STRING));
    servicesTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(SERVICES, servicesTable);
  }

  public static Map<String, TsTable> getSchemaTables() {
    return schemaTables;
  }

  private InformationSchema() {
    // Utils
  }
}
