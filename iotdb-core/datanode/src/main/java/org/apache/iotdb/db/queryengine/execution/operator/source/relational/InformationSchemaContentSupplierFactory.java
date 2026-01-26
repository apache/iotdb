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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceEntry;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceListResp;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.confignode.rpc.thrift.TClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo4InformationSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo4InformationSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDescTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetUdfTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.ConnectionInfo;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowCreateViewTask;
import org.apache.iotdb.db.queryengine.plan.relational.function.TableBuiltinTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.ReservedIdentifiers;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlKeywords;
import org.apache.iotdb.db.schemaengine.table.InformationSchemaUtils;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_STATE_AVAILABLE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_AGG_FUNC;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_SCALAR_FUNC;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_TABLE_FUNC;
import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_RESULT_NODES;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NODE_TYPE_AI_NODE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NODE_TYPE_CONFIG_NODE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NODE_TYPE_DATA_NODE;
import static org.apache.iotdb.commons.schema.table.TsTable.TTL_PROPERTY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.canShowDB;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.canShowTable;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask.BINARY_MAP;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask.getFunctionState;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask.getFunctionType;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowPipePluginsTask.PIPE_PLUGIN_TYPE_BUILTIN;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowPipePluginsTask.PIPE_PLUGIN_TYPE_EXTERNAL;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.externalservice.ShowExternalServiceTask.appendServiceEntry;

public class InformationSchemaContentSupplierFactory {

  private static final SessionManager sessionManager = SessionManager.getInstance();

  private InformationSchemaContentSupplierFactory() {}

  public static Iterator<TsBlock> getSupplier(
      final String tableName,
      final List<TSDataType> dataTypes,
      Expression predicate,
      final UserEntity userEntity) {
    try {
      switch (tableName) {
        case InformationSchema.QUERIES:
          return new QueriesSupplier(dataTypes, userEntity);
        case InformationSchema.DATABASES:
          return new DatabaseSupplier(dataTypes, userEntity);
        case InformationSchema.TABLES:
          return new TableSupplier(dataTypes, userEntity);
        case InformationSchema.COLUMNS:
          return new ColumnSupplier(dataTypes, userEntity);
        case InformationSchema.REGIONS:
          return new RegionSupplier(dataTypes, userEntity);
        case InformationSchema.PIPES:
          return new PipeSupplier(dataTypes, userEntity.getUsername());
        case InformationSchema.PIPE_PLUGINS:
          return new PipePluginSupplier(dataTypes, userEntity);
        case InformationSchema.TOPICS:
          return new TopicSupplier(dataTypes, userEntity);
        case InformationSchema.SUBSCRIPTIONS:
          return new SubscriptionSupplier(dataTypes, userEntity);
        case InformationSchema.VIEWS:
          return new ViewsSupplier(dataTypes, userEntity);
        case InformationSchema.FUNCTIONS:
          return new FunctionsSupplier(dataTypes);
        case InformationSchema.CONFIGURATIONS:
          return new ConfigurationsSupplier(dataTypes, userEntity);
        case InformationSchema.KEYWORDS:
          return new KeywordsSupplier(dataTypes);
        case InformationSchema.NODES:
          return new NodesSupplier(dataTypes, userEntity);
        case InformationSchema.CONFIG_NODES:
          return new ConfigNodesSupplier(dataTypes, userEntity);
        case InformationSchema.DATA_NODES:
          return new DataNodesSupplier(dataTypes, userEntity);
        case InformationSchema.CONNECTIONS:
          return new ConnectionsSupplier(dataTypes, userEntity);
        case InformationSchema.CURRENT_QUERIES:
          return new CurrentQueriesSupplier(dataTypes, predicate, userEntity);
        case InformationSchema.QUERIES_COSTS_HISTOGRAM:
          return new QueriesCostsHistogramSupplier(dataTypes, userEntity);
        case InformationSchema.SERVICES:
          return new ServicesSupplier(dataTypes, userEntity);
        default:
          throw new UnsupportedOperationException("Unknown table: " + tableName);
      }
    } catch (final Exception e) {
      throw new IoTDBRuntimeException(e, TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private static class QueriesSupplier extends TsBlockSupplier {
    private final long currTime = System.currentTimeMillis();
    // We initialize it later for the convenience of data preparation
    protected int totalSize;
    protected int nextConsumedIndex;
    private List<IQueryExecution> queryExecutions;

    private QueriesSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity) {
      super(dataTypes);
      queryExecutions = Coordinator.getInstance().getAllQueryExecutions();
      try {
        accessControl.checkUserGlobalSysPrivilege(userEntity);
      } catch (final AccessDeniedException e) {
        queryExecutions =
            queryExecutions.stream()
                .filter(
                    iQueryExecution -> userEntity.getUsername().equals(iQueryExecution.getUser()))
                .collect(Collectors.toList());
      }
      this.totalSize = queryExecutions.size();
    }

    @Override
    protected void constructLine() {
      final IQueryExecution queryExecution = queryExecutions.get(nextConsumedIndex);

      if (queryExecution.getSQLDialect().equals(IClientSession.SqlDialect.TABLE)) {
        columnBuilders[0].writeBinary(BytesUtils.valueOf(queryExecution.getQueryId()));
        columnBuilders[1].writeLong(
            TimestampPrecisionUtils.convertToCurrPrecision(
                queryExecution.getStartExecutionTime(), TimeUnit.MILLISECONDS));
        columnBuilders[2].writeInt(QueryId.getDataNodeId());
        columnBuilders[3].writeFloat(
            (float) (currTime - queryExecution.getStartExecutionTime()) / 1000);
        columnBuilders[4].writeBinary(
            BytesUtils.valueOf(queryExecution.getExecuteSQL().orElse("UNKNOWN")));
        columnBuilders[5].writeBinary(BytesUtils.valueOf(queryExecution.getUser()));
        resultBuilder.declarePosition();
      }
      nextConsumedIndex++;
    }

    @Override
    public boolean hasNext() {
      return nextConsumedIndex < queryExecutions.size();
    }
  }

  private static class DatabaseSupplier extends TsBlockSupplier {
    private final Iterator<Map.Entry<String, TDatabaseInfo>> iterator;
    private TDatabaseInfo currentDatabase;
    private boolean hasShownInformationSchema;
    private final UserEntity userEntity;

    private DatabaseSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      this.userEntity = userEntity;
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showDatabase(
                    new TGetDatabaseReq(
                            Arrays.asList(ALL_RESULT_NODES), ALL_MATCH_SCOPE.serialize())
                        .setIsTableModel(true))
                .getDatabaseInfoMap()
                .entrySet()
                .iterator();
      }
    }

    @Override
    protected void constructLine() {
      if (!hasShownInformationSchema) {
        InformationSchemaUtils.buildDatabaseTsBlock(resultBuilder, true, false);
        hasShownInformationSchema = true;
        return;
      }
      columnBuilders[0].writeBinary(
          new Binary(currentDatabase.getName(), TSFileConfig.STRING_CHARSET));

      if (Long.MAX_VALUE == currentDatabase.getTTL()) {
        columnBuilders[1].writeBinary(
            new Binary(IoTDBConstant.TTL_INFINITE, TSFileConfig.STRING_CHARSET));
      } else {
        columnBuilders[1].writeBinary(
            new Binary(String.valueOf(currentDatabase.getTTL()), TSFileConfig.STRING_CHARSET));
      }
      columnBuilders[2].writeInt(currentDatabase.getSchemaReplicationFactor());
      columnBuilders[3].writeInt(currentDatabase.getDataReplicationFactor());
      columnBuilders[4].writeLong(currentDatabase.getTimePartitionInterval());
      columnBuilders[5].writeInt(currentDatabase.getSchemaRegionNum());
      columnBuilders[6].writeInt(currentDatabase.getDataRegionNum());
      resultBuilder.declarePosition();
      currentDatabase = null;
    }

    @Override
    public boolean hasNext() {
      if (!hasShownInformationSchema) {
        if (!canShowDB(
            accessControl,
            userEntity.getUsername(),
            InformationSchema.INFORMATION_DATABASE,
            userEntity)) {
          hasShownInformationSchema = true;
        } else {
          return true;
        }
      }
      while (iterator.hasNext()) {
        final Map.Entry<String, TDatabaseInfo> result = iterator.next();
        if (!canShowDB(accessControl, userEntity.getUsername(), result.getKey(), userEntity)) {
          continue;
        }
        currentDatabase = result.getValue();
        break;
      }
      return Objects.nonNull(currentDatabase);
    }
  }

  private static class TableSupplier extends TsBlockSupplier {
    private final Iterator<Map.Entry<String, List<TTableInfo>>> dbIterator;
    private Iterator<TTableInfo> tableInfoIterator = null;
    private TTableInfo currentTable;
    private String dbName;
    private final UserEntity userEntity;

    private TableSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      this.userEntity = userEntity;
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final Map<String, List<TTableInfo>> databaseTableInfoMap =
            client.showTables4InformationSchema().getDatabaseTableInfoMap();
        databaseTableInfoMap.put(
            InformationSchema.INFORMATION_DATABASE,
            InformationSchema.getSchemaTables().values().stream()
                .map(
                    table -> {
                      final TTableInfo info =
                          new TTableInfo(
                              table.getTableName(),
                              table.getPropValue(TTL_PROPERTY).orElse(TTL_INFINITE));
                      info.setState(TableNodeStatus.USING.ordinal());
                      return info;
                    })
                .collect(Collectors.toList()));
        dbIterator = databaseTableInfoMap.entrySet().iterator();
      }
    }

    @Override
    protected void constructLine() {
      columnBuilders[0].writeBinary(new Binary(dbName, TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(currentTable.getTableName(), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(new Binary(currentTable.getTTL(), TSFileConfig.STRING_CHARSET));
      columnBuilders[3].writeBinary(
          new Binary(
              TableNodeStatus.values()[currentTable.getState()].toString(),
              TSFileConfig.STRING_CHARSET));
      if (currentTable.isSetComment()) {
        columnBuilders[4].writeBinary(
            new Binary(currentTable.getComment(), TSFileConfig.STRING_CHARSET));
      } else {
        columnBuilders[4].appendNull();
      }
      if (dbName.equals(InformationSchema.INFORMATION_DATABASE)) {
        columnBuilders[5].writeBinary(
            new Binary(TableType.SYSTEM_VIEW.getName(), TSFileConfig.STRING_CHARSET));
      } else if (currentTable.isSetType()) {
        columnBuilders[5].writeBinary(
            new Binary(
                TableType.values()[currentTable.getType()].getName(), TSFileConfig.STRING_CHARSET));
      } else {
        columnBuilders[5].writeBinary(
            new Binary(TableType.BASE_TABLE.getName(), TSFileConfig.STRING_CHARSET));
      }
      resultBuilder.declarePosition();
      currentTable = null;
    }

    @Override
    public boolean hasNext() {
      // Get next table info iterator
      while (Objects.isNull(currentTable)) {
        while (Objects.nonNull(tableInfoIterator) && tableInfoIterator.hasNext()) {
          final TTableInfo info = tableInfoIterator.next();
          if (canShowTable(
              accessControl, userEntity.getUsername(), dbName, info.getTableName(), userEntity)) {
            currentTable = info;
            return true;
          }
        }
        if (!dbIterator.hasNext()) {
          return false;
        }
        final Map.Entry<String, List<TTableInfo>> entry = dbIterator.next();
        dbName = entry.getKey();
        if (!canShowDB(accessControl, userEntity.getUsername(), dbName, userEntity)) {
          continue;
        }
        tableInfoIterator = entry.getValue().iterator();
      }
      return true;
    }
  }

  private static class ColumnSupplier extends TsBlockSupplier {
    private final Iterator<Map.Entry<String, Map<String, Pair<TsTable, Set<String>>>>> dbIterator;
    private Iterator<Map.Entry<String, Pair<TsTable, Set<String>>>> tableInfoIterator;
    private Iterator<TsTableColumnSchema> columnSchemaIterator;
    private String dbName;
    private String tableName;
    private Set<String> preDeletedColumns;
    private final UserEntity userEntity;

    private ColumnSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      this.userEntity = userEntity;
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TDescTable4InformationSchemaResp resp = client.descTables4InformationSchema();
        final Map<String, Map<String, Pair<TsTable, Set<String>>>> resultMap =
            resp.getTableColumnInfoMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry ->
                            entry.getValue().entrySet().stream()
                                .collect(
                                    Collectors.toMap(
                                        Map.Entry::getKey,
                                        tableEntry ->
                                            new Pair<>(
                                                TsTableInternalRPCUtil.deserializeSingleTsTable(
                                                    tableEntry.getValue().getTableInfo()),
                                                tableEntry.getValue().getPreDeletedColumns())))));
        resultMap.put(
            InformationSchema.INFORMATION_DATABASE,
            InformationSchema.getSchemaTables().values().stream()
                .collect(
                    Collectors.toMap(
                        TsTable::getTableName,
                        table -> new Pair<>(table, Collections.emptySet()))));
        dbIterator = resultMap.entrySet().iterator();
      }
    }

    @Override
    protected void constructLine() {
      final TsTableColumnSchema schema = columnSchemaIterator.next();
      columnBuilders[0].writeBinary(new Binary(dbName, TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(new Binary(tableName, TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(
          new Binary(schema.getColumnName(), TSFileConfig.STRING_CHARSET));
      columnBuilders[3].writeBinary(
          new Binary(schema.getDataType().name(), TSFileConfig.STRING_CHARSET));
      columnBuilders[4].writeBinary(
          new Binary(schema.getColumnCategory().name(), TSFileConfig.STRING_CHARSET));
      columnBuilders[5].writeBinary(
          new Binary(
              preDeletedColumns.contains(schema.getColumnName()) ? "PRE_DELETE" : "USING",
              TSFileConfig.STRING_CHARSET));

      if (schema.getProps().containsKey(TsTable.COMMENT_KEY)) {
        columnBuilders[6].writeBinary(
            new Binary(schema.getProps().get(TsTable.COMMENT_KEY), TSFileConfig.STRING_CHARSET));
      } else {
        columnBuilders[6].appendNull();
      }
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      while (Objects.isNull(columnSchemaIterator) || !columnSchemaIterator.hasNext()) {
        while (Objects.isNull(tableInfoIterator) || !tableInfoIterator.hasNext()) {
          if (!dbIterator.hasNext()) {
            return false;
          }
          final Map.Entry<String, Map<String, Pair<TsTable, Set<String>>>> entry =
              dbIterator.next();
          dbName = entry.getKey();
          if (!canShowDB(accessControl, userEntity.getUsername(), dbName, userEntity)) {
            continue;
          }
          tableInfoIterator = entry.getValue().entrySet().iterator();
        }

        Map.Entry<String, Pair<TsTable, Set<String>>> tableEntry;
        while (tableInfoIterator.hasNext()) {
          tableEntry = tableInfoIterator.next();
          if (canShowTable(
              accessControl, userEntity.getUsername(), dbName, tableEntry.getKey(), userEntity)) {
            tableName = tableEntry.getKey();
            preDeletedColumns = tableEntry.getValue().getRight();
            columnSchemaIterator = tableEntry.getValue().getLeft().getColumnList().iterator();
            break;
          }
        }
      }
      return true;
    }
  }

  private static class RegionSupplier extends TsBlockSupplier {
    private final Iterator<TRegionInfo> iterator;

    private RegionSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showRegion(new TShowRegionReq().setIsTableModel(true).setDatabases(null))
                .getRegionInfoListIterator();
      }
    }

    @Override
    protected void constructLine() {
      final TRegionInfo regionInfo = iterator.next();
      columnBuilders[0].writeInt(regionInfo.getConsensusGroupId().getId());
      columnBuilders[1].writeInt(regionInfo.getDataNodeId());
      if (regionInfo.getConsensusGroupId().getType().ordinal()
          == TConsensusGroupType.SchemaRegion.ordinal()) {
        columnBuilders[2].writeBinary(
            BytesUtils.valueOf(String.valueOf(TConsensusGroupType.SchemaRegion)));
      } else if (regionInfo.getConsensusGroupId().getType().ordinal()
          == TConsensusGroupType.DataRegion.ordinal()) {
        columnBuilders[2].writeBinary(
            BytesUtils.valueOf(String.valueOf(TConsensusGroupType.DataRegion)));
      }
      columnBuilders[3].writeBinary(
          BytesUtils.valueOf(regionInfo.getStatus() == null ? "" : regionInfo.getStatus()));
      columnBuilders[4].writeBinary(BytesUtils.valueOf(regionInfo.getDatabase()));
      columnBuilders[5].writeInt(regionInfo.getSeriesSlots());
      columnBuilders[6].writeLong(regionInfo.getTimeSlots());
      columnBuilders[7].writeBinary(BytesUtils.valueOf(regionInfo.getClientRpcIp()));
      columnBuilders[8].writeInt(regionInfo.getClientRpcPort());
      columnBuilders[9].writeBinary(BytesUtils.valueOf(regionInfo.getInternalAddress()));
      columnBuilders[10].writeBinary(BytesUtils.valueOf(regionInfo.getRoleType()));
      columnBuilders[11].writeLong(regionInfo.getCreateTime());
      if (regionInfo.getConsensusGroupId().getType().ordinal()
          == TConsensusGroupType.DataRegion.ordinal()) {
        columnBuilders[12].writeLong(regionInfo.getTsFileSize());
        columnBuilders[13].writeDouble(
            MathUtils.roundWithGivenPrecision(
                (double) regionInfo.getRawDataSize() / regionInfo.getTsFileSize(), 2));
      } else {
        columnBuilders[12].appendNull();
        columnBuilders[13].appendNull();
      }
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static class PipeSupplier extends TsBlockSupplier {
    private final Iterator<TShowPipeInfo> iterator;

    private PipeSupplier(final List<TSDataType> dataTypes, final String userName) throws Exception {
      super(dataTypes);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showPipe(new TShowPipeReq().setIsTableModel(true).setUserName(userName))
                .getPipeInfoListIterator();
      }
    }

    @Override
    protected void constructLine() {
      final TShowPipeInfo tPipeInfo = iterator.next();
      columnBuilders[0].writeBinary(new Binary(tPipeInfo.getId(), TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeLong(
          TimestampPrecisionUtils.convertToCurrPrecision(
              tPipeInfo.getCreationTime(), TimeUnit.MILLISECONDS));
      columnBuilders[2].writeBinary(new Binary(tPipeInfo.getState(), TSFileConfig.STRING_CHARSET));
      columnBuilders[3].writeBinary(
          new Binary(tPipeInfo.getPipeExtractor(), TSFileConfig.STRING_CHARSET));
      columnBuilders[4].writeBinary(
          new Binary(tPipeInfo.getPipeProcessor(), TSFileConfig.STRING_CHARSET));
      columnBuilders[5].writeBinary(
          new Binary(tPipeInfo.getPipeConnector(), TSFileConfig.STRING_CHARSET));
      columnBuilders[6].writeBinary(
          new Binary(tPipeInfo.getExceptionMessage(), TSFileConfig.STRING_CHARSET));

      // Optional, default 0/0.0
      long remainingEventCount = tPipeInfo.getRemainingEventCount();
      double remainingTime = tPipeInfo.getEstimatedRemainingTime();

      if (remainingEventCount == -1 && remainingTime == -1) {
        final Pair<Long, Double> remainingEventAndTime =
            PipeDataNodeSinglePipeMetrics.getInstance()
                .getRemainingEventAndTime(tPipeInfo.getId(), tPipeInfo.getCreationTime());
        remainingEventCount = remainingEventAndTime.getLeft();
        remainingTime = remainingEventAndTime.getRight();
      }

      columnBuilders[7].writeLong(tPipeInfo.isSetRemainingEventCount() ? remainingEventCount : -1);
      columnBuilders[8].writeDouble(tPipeInfo.isSetEstimatedRemainingTime() ? remainingTime : -1);

      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static class PipePluginSupplier extends TsBlockSupplier {
    private final Iterator<PipePluginMeta> iterator;

    private PipePluginSupplier(final List<TSDataType> dataTypes, final UserEntity entity)
        throws ClientManagerException, TException {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(entity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client.getPipePluginTable().getAllPipePluginMeta().stream()
                .map(PipePluginMeta::deserialize)
                .filter(
                    pipePluginMeta ->
                        !BuiltinPipePlugin.SHOW_PIPE_PLUGINS_BLACKLIST.contains(
                            pipePluginMeta.getPluginName()))
                .iterator();
      }
    }

    @Override
    protected void constructLine() {
      final PipePluginMeta pipePluginMeta = iterator.next();
      columnBuilders[0].writeBinary(BytesUtils.valueOf(pipePluginMeta.getPluginName()));
      columnBuilders[1].writeBinary(
          pipePluginMeta.isBuiltin() ? PIPE_PLUGIN_TYPE_BUILTIN : PIPE_PLUGIN_TYPE_EXTERNAL);
      columnBuilders[2].writeBinary(BytesUtils.valueOf(pipePluginMeta.getClassName()));
      if (Objects.nonNull(pipePluginMeta.getJarName())) {
        columnBuilders[3].writeBinary(BytesUtils.valueOf(pipePluginMeta.getJarName()));
      } else {
        columnBuilders[3].appendNull();
      }
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static class TopicSupplier extends TsBlockSupplier {
    private final Iterator<TShowTopicInfo> iterator;

    private TopicSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showTopic(new TShowTopicReq().setIsTableModel(true))
                .getTopicInfoList()
                .iterator();
      }
    }

    @Override
    protected void constructLine() {
      final TShowTopicInfo topicInfo = iterator.next();
      columnBuilders[0].writeBinary(
          new Binary(topicInfo.getTopicName(), TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(topicInfo.getTopicAttributes(), TSFileConfig.STRING_CHARSET));
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static class SubscriptionSupplier extends TsBlockSupplier {
    private final Iterator<TShowSubscriptionInfo> iterator;

    private SubscriptionSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showSubscription(new TShowSubscriptionReq().setIsTableModel(true))
                .getSubscriptionInfoList()
                .iterator();
      }
    }

    @Override
    protected void constructLine() {
      final TShowSubscriptionInfo tSubscriptionInfo = iterator.next();
      columnBuilders[0].writeBinary(
          new Binary(tSubscriptionInfo.getTopicName(), TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(tSubscriptionInfo.getConsumerGroupId(), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(
          new Binary(tSubscriptionInfo.getConsumerIds().toString(), TSFileConfig.STRING_CHARSET));
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static class ViewsSupplier extends TsBlockSupplier {
    private final Iterator<Map.Entry<String, Map<String, Pair<TsTable, Set<String>>>>> dbIterator;
    private Iterator<Map.Entry<String, Pair<TsTable, Set<String>>>> tableInfoIterator;
    private String dbName;
    private TsTable currentTable;
    private final UserEntity userEntity;

    private ViewsSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      this.userEntity = userEntity;
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TDescTable4InformationSchemaResp resp = client.descTables4InformationSchema();
        final Map<String, Map<String, Pair<TsTable, Set<String>>>> resultMap =
            resp.getTableColumnInfoMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry ->
                            entry.getValue().entrySet().stream()
                                .collect(
                                    Collectors.toMap(
                                        Map.Entry::getKey,
                                        tableEntry ->
                                            new Pair<>(
                                                TsTableInternalRPCUtil.deserializeSingleTsTable(
                                                    tableEntry.getValue().getTableInfo()),
                                                tableEntry.getValue().getPreDeletedColumns())))));
        dbIterator = resultMap.entrySet().iterator();
      }
    }

    @Override
    protected void constructLine() {
      columnBuilders[0].writeBinary(new Binary(dbName, TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(currentTable.getTableName(), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(
          new Binary(
              ShowCreateViewTask.getShowCreateViewSQL(currentTable), TSFileConfig.STRING_CHARSET));
      resultBuilder.declarePosition();
      currentTable = null;
    }

    @Override
    public boolean hasNext() {
      while (Objects.isNull(currentTable)) {
        while (Objects.isNull(tableInfoIterator) || !tableInfoIterator.hasNext()) {
          if (!dbIterator.hasNext()) {
            return false;
          }
          final Map.Entry<String, Map<String, Pair<TsTable, Set<String>>>> entry =
              dbIterator.next();
          dbName = entry.getKey();
          if (!canShowDB(accessControl, userEntity.getUsername(), dbName, userEntity)) {
            continue;
          }
          tableInfoIterator = entry.getValue().entrySet().iterator();
        }

        while (tableInfoIterator.hasNext()) {
          final Map.Entry<String, Pair<TsTable, Set<String>>> tableEntry = tableInfoIterator.next();
          if (!TreeViewSchema.isTreeViewTable(tableEntry.getValue().getLeft())
              || !canShowTable(
                  accessControl,
                  userEntity.getUsername(),
                  dbName,
                  tableEntry.getKey(),
                  userEntity)) {
            continue;
          }
          currentTable = tableEntry.getValue().getLeft();
          return true;
        }
      }
      return true;
    }
  }

  private static class FunctionsSupplier extends TsBlockSupplier {

    private final Iterator<UDFInformation> udfIterator;
    private Iterator<String> nameIterator;
    private Binary functionType;
    private final Binary functionState;

    private FunctionsSupplier(final List<TSDataType> dataTypes) throws Exception {
      super(dataTypes);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        udfIterator =
            client.getUDFTable(new TGetUdfTableReq(Model.TABLE)).getAllUDFInformation().stream()
                .map(UDFInformation::deserialize)
                .sorted(Comparator.comparing(UDFInformation::getFunctionName))
                .iterator();
        nameIterator = TableBuiltinScalarFunction.getBuiltInScalarFunctionName().iterator();
        functionType = BINARY_MAP.get(FUNCTION_TYPE_BUILTIN_SCALAR_FUNC);
        functionState = BINARY_MAP.get(FUNCTION_STATE_AVAILABLE);
      }
    }

    @Override
    protected void constructLine() {
      if (udfIterator.hasNext()) {
        final UDFInformation udfInformation = udfIterator.next();
        columnBuilders[0].writeBinary(BytesUtils.valueOf(udfInformation.getFunctionName()));
        columnBuilders[1].writeBinary(getFunctionType(udfInformation));
        columnBuilders[2].writeBinary(BytesUtils.valueOf(udfInformation.getClassName()));
        columnBuilders[3].writeBinary(getFunctionState(udfInformation));
      } else if (nameIterator.hasNext()) {
        final String name = nameIterator.next();
        columnBuilders[0].writeBinary(BytesUtils.valueOf(name.toUpperCase()));
        columnBuilders[1].writeBinary(functionType);
        columnBuilders[2].appendNull();
        columnBuilders[3].writeBinary(functionState);
      }
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      if (udfIterator.hasNext()) {
        return true;
      }
      while (!nameIterator.hasNext()) {
        if (functionType.equals(BINARY_MAP.get(FUNCTION_TYPE_BUILTIN_SCALAR_FUNC))) {
          functionType = BINARY_MAP.get(FUNCTION_TYPE_BUILTIN_AGG_FUNC);
          nameIterator =
              TableBuiltinAggregationFunction.getBuiltInAggregateFunctionName().iterator();
        } else if (functionType.equals(BINARY_MAP.get(FUNCTION_TYPE_BUILTIN_AGG_FUNC))) {
          functionType = BINARY_MAP.get(FUNCTION_TYPE_BUILTIN_TABLE_FUNC);
          nameIterator = TableBuiltinTableFunction.getBuiltInTableFunctionName().iterator();
        } else {
          return false;
        }
      }
      return true;
    }
  }

  private static class ServicesSupplier extends TsBlockSupplier {

    private final Iterator<TExternalServiceEntry> serviceEntryIterator;

    private ServicesSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // -1 means get all services
        TExternalServiceListResp resp = client.showExternalService(-1);
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new IoTDBRuntimeException(resp.getStatus());
        }

        serviceEntryIterator = resp.getExternalServiceInfosIterator();
      }
    }

    @Override
    protected void constructLine() {
      appendServiceEntry(serviceEntryIterator.next(), columnBuilders);
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return serviceEntryIterator.hasNext();
    }
  }

  private static class ConfigurationsSupplier extends TsBlockSupplier {
    private final Iterator<Pair<Binary, Binary>> resultIterator;

    private ConfigurationsSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TClusterParameters parameters = client.showVariables().getClusterParameters();
        resultIterator =
            Arrays.asList(
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.COLUMN_CLUSTER_NAME),
                        BytesUtils.valueOf(parameters.getClusterName())),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.DATA_REPLICATION_FACTOR),
                        BytesUtils.valueOf(String.valueOf(parameters.getDataReplicationFactor()))),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.SCHEMA_REPLICATION_FACTOR),
                        BytesUtils.valueOf(
                            String.valueOf(parameters.getSchemaReplicationFactor()))),
                    new Pair<>(
                        BytesUtils.valueOf(
                            ColumnHeaderConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS),
                        BytesUtils.valueOf(parameters.getDataRegionConsensusProtocolClass())),
                    new Pair<>(
                        BytesUtils.valueOf(
                            ColumnHeaderConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS),
                        BytesUtils.valueOf(parameters.getSchemaRegionConsensusProtocolClass())),
                    new Pair<>(
                        BytesUtils.valueOf(
                            ColumnHeaderConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS),
                        BytesUtils.valueOf(parameters.getConfigNodeConsensusProtocolClass())),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.TIME_PARTITION_ORIGIN),
                        BytesUtils.valueOf(String.valueOf(parameters.getTimePartitionOrigin()))),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.TIME_PARTITION_INTERVAL),
                        BytesUtils.valueOf(String.valueOf(parameters.getTimePartitionInterval()))),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.READ_CONSISTENCY_LEVEL),
                        BytesUtils.valueOf(parameters.getReadConsistencyLevel())),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.SCHEMA_REGION_PER_DATA_NODE),
                        BytesUtils.valueOf(
                            String.valueOf(parameters.getSchemaRegionPerDataNode()))),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.DATA_REGION_PER_DATA_NODE),
                        BytesUtils.valueOf(String.valueOf(parameters.getDataRegionPerDataNode()))),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.SERIES_SLOT_NUM),
                        BytesUtils.valueOf(String.valueOf(parameters.getSeriesPartitionSlotNum()))),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.SERIES_SLOT_EXECUTOR_CLASS),
                        BytesUtils.valueOf(parameters.getSeriesPartitionExecutorClass())),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.DISK_SPACE_WARNING_THRESHOLD),
                        BytesUtils.valueOf(
                            String.valueOf(parameters.getDiskSpaceWarningThreshold()))),
                    new Pair<>(
                        BytesUtils.valueOf(ColumnHeaderConstant.TIMESTAMP_PRECISION),
                        BytesUtils.valueOf(parameters.getTimestampPrecision())))
                .iterator();
      }
    }

    @Override
    protected void constructLine() {
      final Pair<Binary, Binary> currentVariable = resultIterator.next();
      columnBuilders[0].writeBinary(currentVariable.getLeft());
      columnBuilders[1].writeBinary(currentVariable.getRight());
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return resultIterator.hasNext();
    }
  }

  private static class KeywordsSupplier extends TsBlockSupplier {
    private final Iterator<String> keywordIterator;
    private final Set<String> reserved = ReservedIdentifiers.reservedIdentifiers();

    private KeywordsSupplier(final List<TSDataType> dataTypes) {
      super(dataTypes);
      keywordIterator = RelationalSqlKeywords.sqlKeywords().iterator();
    }

    @Override
    protected void constructLine() {
      final String keyword = keywordIterator.next();
      columnBuilders[0].writeBinary(BytesUtils.valueOf(keyword));
      columnBuilders[1].writeInt(reserved.contains(keyword) ? 1 : 0);
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return keywordIterator.hasNext();
    }
  }

  private static class NodesSupplier extends TsBlockSupplier {
    private final TShowClusterResp showClusterResp;
    private final Iterator<TConfigNodeLocation> configNodeIterator;
    private final Iterator<TDataNodeLocation> dataNodeIterator;
    private final Iterator<TAINodeLocation> aiNodeIterator;

    private NodesSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        showClusterResp = client.showCluster();
        configNodeIterator = showClusterResp.getConfigNodeListIterator();
        dataNodeIterator = showClusterResp.getDataNodeListIterator();
        aiNodeIterator = showClusterResp.getAiNodeListIterator();
      }
    }

    @Override
    protected void constructLine() {
      if (configNodeIterator.hasNext()) {
        final TConfigNodeLocation location = configNodeIterator.next();
        buildNodeTsBlock(
            location.getConfigNodeId(),
            NODE_TYPE_CONFIG_NODE,
            showClusterResp.getNodeStatus().get(location.getConfigNodeId()),
            location.getInternalEndPoint().getIp(),
            location.getInternalEndPoint().getPort(),
            showClusterResp.getNodeVersionInfo().get(location.getConfigNodeId()));
        return;
      }
      if (dataNodeIterator.hasNext()) {
        final TDataNodeLocation location = dataNodeIterator.next();
        buildNodeTsBlock(
            location.getDataNodeId(),
            NODE_TYPE_DATA_NODE,
            showClusterResp.getNodeStatus().get(location.getDataNodeId()),
            location.getInternalEndPoint().getIp(),
            location.getInternalEndPoint().getPort(),
            showClusterResp.getNodeVersionInfo().get(location.getDataNodeId()));
        return;
      }
      if (aiNodeIterator.hasNext()) {
        final TAINodeLocation location = aiNodeIterator.next();
        buildNodeTsBlock(
            location.getAiNodeId(),
            NODE_TYPE_AI_NODE,
            showClusterResp.getNodeStatus().get(location.getAiNodeId()),
            location.getInternalEndPoint().getIp(),
            location.getInternalEndPoint().getPort(),
            showClusterResp.getNodeVersionInfo().get(location.getAiNodeId()));
      }
    }

    private void buildNodeTsBlock(
        int nodeId,
        String nodeType,
        String nodeStatus,
        String internalAddress,
        int internalPort,
        TNodeVersionInfo versionInfo) {
      columnBuilders[0].writeInt(nodeId);
      columnBuilders[1].writeBinary(new Binary(nodeType, TSFileConfig.STRING_CHARSET));
      if (nodeStatus == null) {
        columnBuilders[2].appendNull();
      } else {
        columnBuilders[2].writeBinary(new Binary(nodeStatus, TSFileConfig.STRING_CHARSET));
      }

      if (internalAddress == null) {
        columnBuilders[3].appendNull();
      } else {
        columnBuilders[3].writeBinary(new Binary(internalAddress, TSFileConfig.STRING_CHARSET));
      }
      columnBuilders[4].writeInt(internalPort);
      if (versionInfo == null || versionInfo.getVersion() == null) {
        columnBuilders[5].appendNull();
      } else {
        columnBuilders[5].writeBinary(
            new Binary(versionInfo.getVersion(), TSFileConfig.STRING_CHARSET));
      }
      if (versionInfo == null || versionInfo.getBuildInfo() == null) {
        columnBuilders[6].appendNull();
      } else {
        columnBuilders[6].writeBinary(
            new Binary(versionInfo.getBuildInfo(), TSFileConfig.STRING_CHARSET));
      }
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return configNodeIterator.hasNext() || dataNodeIterator.hasNext() || aiNodeIterator.hasNext();
    }
  }

  private static class ConfigNodesSupplier extends TsBlockSupplier {
    private final Iterator<TConfigNodeInfo4InformationSchema> configNodeIterator;

    private ConfigNodesSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        configNodeIterator =
            client.showConfigNodes4InformationSchema().getConfigNodesInfoListIterator();
      }
    }

    @Override
    protected void constructLine() {
      final TConfigNodeInfo4InformationSchema configNodeInfo4InformationSchema =
          configNodeIterator.next();
      columnBuilders[0].writeInt(configNodeInfo4InformationSchema.getConfigNodeId());
      columnBuilders[1].writeInt(configNodeInfo4InformationSchema.getConsensusPort());
      columnBuilders[2].writeBinary(
          new Binary(configNodeInfo4InformationSchema.getRoleType(), TSFileConfig.STRING_CHARSET));
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return configNodeIterator.hasNext();
    }
  }

  private static class DataNodesSupplier extends TsBlockSupplier {
    private final Iterator<TDataNodeInfo4InformationSchema> dataNodeIterator;

    private DataNodesSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity)
        throws Exception {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeIterator = client.showDataNodes4InformationSchema().getDataNodesInfoListIterator();
      }
    }

    @Override
    protected void constructLine() {
      final TDataNodeInfo4InformationSchema dataNodeInfo4InformationSchema =
          dataNodeIterator.next();
      columnBuilders[0].writeInt(dataNodeInfo4InformationSchema.getDataNodeId());
      columnBuilders[1].writeInt(dataNodeInfo4InformationSchema.getDataRegionNum());
      columnBuilders[2].writeInt(dataNodeInfo4InformationSchema.getSchemaRegionNum());
      columnBuilders[3].writeBinary(
          new Binary(dataNodeInfo4InformationSchema.getRpcAddress(), TSFileConfig.STRING_CHARSET));
      columnBuilders[4].writeInt(dataNodeInfo4InformationSchema.getRpcPort());
      columnBuilders[5].writeInt(dataNodeInfo4InformationSchema.getMppPort());
      columnBuilders[6].writeInt(dataNodeInfo4InformationSchema.getDataConsensusPort());
      columnBuilders[7].writeInt(dataNodeInfo4InformationSchema.getSchemaConsensusPort());
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return dataNodeIterator.hasNext();
    }
  }

  private abstract static class TsBlockSupplier implements Iterator<TsBlock> {

    protected final TsBlockBuilder resultBuilder;
    protected final ColumnBuilder[] columnBuilders;
    protected final AccessControl accessControl = AuthorityChecker.getAccessControl();

    private TsBlockSupplier(final List<TSDataType> dataTypes) {
      this.resultBuilder = new TsBlockBuilder(dataTypes);
      this.columnBuilders = resultBuilder.getValueColumnBuilders();
    }

    @Override
    public TsBlock next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      while (hasNext() && !resultBuilder.isFull()) {
        constructLine();
      }
      final TsBlock result =
          resultBuilder.build(
              new RunLengthEncodedColumn(
                  AbstractTableScanOperator.TIME_COLUMN_TEMPLATE,
                  resultBuilder.getPositionCount()));
      resultBuilder.reset();
      return result;
    }

    protected abstract void constructLine();
  }

  private static class ConnectionsSupplier extends TsBlockSupplier {
    private Iterator<ConnectionInfo> sessionConnectionIterator;

    private ConnectionsSupplier(final List<TSDataType> dataTypes, final UserEntity userEntity) {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      sessionConnectionIterator = sessionManager.getAllSessionConnectionInfo().iterator();
    }

    @Override
    protected void constructLine() {
      ConnectionInfo connectionInfo = sessionConnectionIterator.next();
      columnBuilders[0].writeBinary(
          new Binary(String.valueOf(connectionInfo.getDataNodeId()), TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(String.valueOf(connectionInfo.getUserId()), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(
          new Binary(String.valueOf(connectionInfo.getSessionId()), TSFileConfig.STRING_CHARSET));
      columnBuilders[3].writeBinary(
          new Binary(connectionInfo.getUserName(), TSFileConfig.STRING_CHARSET));
      columnBuilders[4].writeLong(connectionInfo.getLastActiveTime());
      columnBuilders[5].writeBinary(
          new Binary(connectionInfo.getClientAddress(), TSFileConfig.STRING_CHARSET));
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return sessionConnectionIterator.hasNext();
    }
  }

  private static class CurrentQueriesSupplier extends TsBlockSupplier {
    private int nextConsumedIndex;
    private List<Coordinator.StatedQueriesInfo> queriesInfo;

    private CurrentQueriesSupplier(
        final List<TSDataType> dataTypes, Expression predicate, final UserEntity userEntity) {
      super(dataTypes);

      if (predicate == null) {
        queriesInfo = Coordinator.getInstance().getCurrentQueriesInfo();
      } else if (QueryState.RUNNING
          .toString()
          .equals(((StringLiteral) ((ComparisonExpression) predicate).getRight()).getValue())) {
        queriesInfo = Coordinator.getInstance().getRunningQueriesInfos();
      } else if (QueryState.FINISHED
          .toString()
          .equals(((StringLiteral) ((ComparisonExpression) predicate).getRight()).getValue())) {
        queriesInfo = Coordinator.getInstance().getFinishedQueriesInfos();
      } else {
        queriesInfo = Collections.emptyList();
      }

      try {
        accessControl.checkUserGlobalSysPrivilege(userEntity);
      } catch (final AccessDeniedException e) {
        queriesInfo =
            queriesInfo.stream()
                .filter(iQueryInfo -> userEntity.getUsername().equals(iQueryInfo.getUser()))
                .collect(Collectors.toList());
      }
    }

    @Override
    protected void constructLine() {
      final Coordinator.StatedQueriesInfo queryInfo = queriesInfo.get(nextConsumedIndex);
      columnBuilders[0].writeBinary(BytesUtils.valueOf(queryInfo.getQueryId()));
      columnBuilders[1].writeBinary(BytesUtils.valueOf(queryInfo.getQueryState()));
      columnBuilders[2].writeLong(
          TimestampPrecisionUtils.convertToCurrPrecision(
              queryInfo.getStartTime(), TimeUnit.MILLISECONDS));
      if (queryInfo.getEndTime() == Coordinator.QueryInfo.DEFAULT_END_TIME) {
        columnBuilders[3].appendNull();
      } else {
        columnBuilders[3].writeLong(
            TimestampPrecisionUtils.convertToCurrPrecision(
                queryInfo.getEndTime(), TimeUnit.MILLISECONDS));
      }
      columnBuilders[4].writeInt(QueryId.getDataNodeId());
      columnBuilders[5].writeFloat(queryInfo.getCostTime());
      columnBuilders[6].writeBinary(BytesUtils.valueOf(queryInfo.getStatement()));
      columnBuilders[7].writeBinary(BytesUtils.valueOf(queryInfo.getUser()));
      columnBuilders[8].writeBinary(BytesUtils.valueOf(queryInfo.getClientHost()));
      resultBuilder.declarePosition();
      nextConsumedIndex++;
    }

    @Override
    public boolean hasNext() {
      return nextConsumedIndex < queriesInfo.size();
    }
  }

  private static class QueriesCostsHistogramSupplier extends TsBlockSupplier {
    private int nextConsumedIndex;
    private static final Binary[] BUCKETS =
        new Binary[] {
          BytesUtils.valueOf("[0,1)"),
          BytesUtils.valueOf("[1,2)"),
          BytesUtils.valueOf("[2,3)"),
          BytesUtils.valueOf("[3,4)"),
          BytesUtils.valueOf("[4,5)"),
          BytesUtils.valueOf("[5,6)"),
          BytesUtils.valueOf("[6,7)"),
          BytesUtils.valueOf("[7,8)"),
          BytesUtils.valueOf("[8,9)"),
          BytesUtils.valueOf("[9,10)"),
          BytesUtils.valueOf("[10,11)"),
          BytesUtils.valueOf("[11,12)"),
          BytesUtils.valueOf("[12,13)"),
          BytesUtils.valueOf("[13,14)"),
          BytesUtils.valueOf("[14,15)"),
          BytesUtils.valueOf("[15,16)"),
          BytesUtils.valueOf("[16,17)"),
          BytesUtils.valueOf("[17,18)"),
          BytesUtils.valueOf("[18,19)"),
          BytesUtils.valueOf("[19,20)"),
          BytesUtils.valueOf("[20,21)"),
          BytesUtils.valueOf("[21,22)"),
          BytesUtils.valueOf("[22,23)"),
          BytesUtils.valueOf("[23,24)"),
          BytesUtils.valueOf("[24,25)"),
          BytesUtils.valueOf("[25,26)"),
          BytesUtils.valueOf("[26,27)"),
          BytesUtils.valueOf("[27,28)"),
          BytesUtils.valueOf("[28,29)"),
          BytesUtils.valueOf("[29,30)"),
          BytesUtils.valueOf("[30,31)"),
          BytesUtils.valueOf("[31,32)"),
          BytesUtils.valueOf("[32,33)"),
          BytesUtils.valueOf("[33,34)"),
          BytesUtils.valueOf("[34,35)"),
          BytesUtils.valueOf("[35,36)"),
          BytesUtils.valueOf("[36,37)"),
          BytesUtils.valueOf("[37,38)"),
          BytesUtils.valueOf("[38,39)"),
          BytesUtils.valueOf("[39,40)"),
          BytesUtils.valueOf("[40,41)"),
          BytesUtils.valueOf("[41,42)"),
          BytesUtils.valueOf("[42,43)"),
          BytesUtils.valueOf("[43,44)"),
          BytesUtils.valueOf("[44,45)"),
          BytesUtils.valueOf("[45,46)"),
          BytesUtils.valueOf("[46,47)"),
          BytesUtils.valueOf("[47,48)"),
          BytesUtils.valueOf("[48,49)"),
          BytesUtils.valueOf("[49,50)"),
          BytesUtils.valueOf("[50,51)"),
          BytesUtils.valueOf("[51,52)"),
          BytesUtils.valueOf("[52,53)"),
          BytesUtils.valueOf("[53,54)"),
          BytesUtils.valueOf("[54,55)"),
          BytesUtils.valueOf("[55,56)"),
          BytesUtils.valueOf("[56,57)"),
          BytesUtils.valueOf("[57,58)"),
          BytesUtils.valueOf("[58,59)"),
          BytesUtils.valueOf("[59,60)"),
          BytesUtils.valueOf("60+")
        };
    private final int[] currentQueriesCostHistogram;

    private QueriesCostsHistogramSupplier(
        final List<TSDataType> dataTypes, final UserEntity userEntity) {
      super(dataTypes);
      accessControl.checkUserGlobalSysPrivilege(userEntity);
      currentQueriesCostHistogram = Coordinator.getInstance().getCurrentQueriesCostHistogram();
    }

    @Override
    protected void constructLine() {
      columnBuilders[0].writeBinary(BUCKETS[nextConsumedIndex]);
      columnBuilders[1].writeInt(currentQueriesCostHistogram[nextConsumedIndex]);
      columnBuilders[2].writeInt(QueryId.getDataNodeId());
      resultBuilder.declarePosition();
      nextConsumedIndex++;
    }

    @Override
    public boolean hasNext() {
      return nextConsumedIndex < 61;
    }
  }
}
