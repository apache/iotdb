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
import org.apache.iotdb.commons.conf.IoTDBConstant;
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
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeRemainingEventAndTimeMetrics;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowCreateViewTask;
import org.apache.iotdb.db.queryengine.plan.relational.function.TableBuiltinTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.ReservedIdentifiers;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlKeywords;
import org.apache.iotdb.db.schemaengine.table.InformationSchemaUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

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

public class InformationSchemaContentSupplierFactory {
  private InformationSchemaContentSupplierFactory() {}

  private static final AccessControl accessControl = Coordinator.getInstance().getAccessControl();

  public static Iterator<TsBlock> getSupplier(
      final String tableName, final List<TSDataType> dataTypes, final String userName) {
    switch (tableName) {
      case InformationSchema.QUERIES:
        return new QueriesSupplier(dataTypes, userName);
      case InformationSchema.DATABASES:
        return new DatabaseSupplier(dataTypes, userName);
      case InformationSchema.TABLES:
        return new TableSupplier(dataTypes, userName);
      case InformationSchema.COLUMNS:
        return new ColumnSupplier(dataTypes, userName);
      case InformationSchema.REGIONS:
        return new RegionSupplier(dataTypes, userName);
      case InformationSchema.PIPES:
        return new PipeSupplier(dataTypes, userName);
      case InformationSchema.PIPE_PLUGINS:
        return new PipePluginSupplier(dataTypes);
      case InformationSchema.TOPICS:
        return new TopicSupplier(dataTypes, userName);
      case InformationSchema.SUBSCRIPTIONS:
        return new SubscriptionSupplier(dataTypes, userName);
      case InformationSchema.VIEWS:
        return new ViewsSupplier(dataTypes, userName);
      case InformationSchema.MODELS:
        return new ModelsSupplier(dataTypes);
      case InformationSchema.FUNCTIONS:
        return new FunctionsSupplier(dataTypes);
      case InformationSchema.CONFIGURATIONS:
        return new ConfigurationsSupplier(dataTypes, userName);
      case InformationSchema.KEYWORDS:
        return new KeywordsSupplier(dataTypes);
      case InformationSchema.NODES:
        return new NodesSupplier(dataTypes, userName);
      case InformationSchema.CONFIG_NODES:
        return new ConfigNodesSupplier(dataTypes, userName);
      case InformationSchema.DATA_NODES:
        return new DataNodesSupplier(dataTypes, userName);
      default:
        throw new UnsupportedOperationException("Unknown table: " + tableName);
    }
  }

  private static class QueriesSupplier extends TsBlockSupplier {
    private final long currTime = System.currentTimeMillis();
    // We initialize it later for the convenience of data preparation
    protected int totalSize;
    protected int nextConsumedIndex;
    private List<IQueryExecution> queryExecutions;

    private QueriesSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      queryExecutions = Coordinator.getInstance().getAllQueryExecutions();
      try {
        accessControl.checkUserIsAdmin(userName);
      } catch (final AccessDeniedException e) {
        queryExecutions =
            queryExecutions.stream()
                .filter(iQueryExecution -> userName.equals(iQueryExecution.getUser()))
                .collect(Collectors.toList());
      }
      this.totalSize = queryExecutions.size();
    }

    @Override
    protected void constructLine() {
      final IQueryExecution queryExecution = queryExecutions.get(nextConsumedIndex);

      if (queryExecution.getSQLDialect().equals(IClientSession.SqlDialect.TABLE)) {
        final String[] splits = queryExecution.getQueryId().split("_");
        final int dataNodeId = Integer.parseInt(splits[splits.length - 1]);

        columnBuilders[0].writeBinary(BytesUtils.valueOf(queryExecution.getQueryId()));
        columnBuilders[1].writeLong(
            TimestampPrecisionUtils.convertToCurrPrecision(
                queryExecution.getStartExecutionTime(), TimeUnit.MILLISECONDS));
        columnBuilders[2].writeInt(dataNodeId);
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
    private Iterator<Map.Entry<String, TDatabaseInfo>> iterator;
    private TDatabaseInfo currentDatabase;
    private boolean hasShownInformationSchema;
    private final String userName;

    private DatabaseSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      this.userName = userName;
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
      } catch (final Exception e) {
        lastException = e;
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
        if (!canShowDB(accessControl, userName, InformationSchema.INFORMATION_DATABASE)) {
          hasShownInformationSchema = true;
        } else {
          return true;
        }
      }
      while (iterator.hasNext()) {
        final Map.Entry<String, TDatabaseInfo> result = iterator.next();
        if (!canShowDB(accessControl, userName, result.getKey())) {
          continue;
        }
        currentDatabase = result.getValue();
        break;
      }
      return Objects.nonNull(currentDatabase);
    }
  }

  private static class TableSupplier extends TsBlockSupplier {
    private Iterator<Map.Entry<String, List<TTableInfo>>> dbIterator;
    private Iterator<TTableInfo> tableInfoIterator = null;
    private TTableInfo currentTable;
    private String dbName;
    private final String userName;

    private TableSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      this.userName = userName;
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
      } catch (final Exception e) {
        lastException = e;
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
          if (canShowTable(accessControl, userName, dbName, info.getTableName())) {
            currentTable = info;
            return true;
          }
        }
        if (!dbIterator.hasNext()) {
          return false;
        }
        final Map.Entry<String, List<TTableInfo>> entry = dbIterator.next();
        dbName = entry.getKey();
        if (!canShowDB(accessControl, userName, dbName)) {
          continue;
        }
        tableInfoIterator = entry.getValue().iterator();
      }
      return true;
    }
  }

  private static class ColumnSupplier extends TsBlockSupplier {
    private Iterator<Map.Entry<String, Map<String, Pair<TsTable, Set<String>>>>> dbIterator;
    private Iterator<Map.Entry<String, Pair<TsTable, Set<String>>>> tableInfoIterator;
    private Iterator<TsTableColumnSchema> columnSchemaIterator;
    private String dbName;
    private String tableName;
    private Set<String> preDeletedColumns;
    private final String userName;

    private ColumnSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      this.userName = userName;
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
      } catch (final Exception e) {
        lastException = e;
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
          if (!canShowDB(accessControl, userName, dbName)) {
            continue;
          }
          tableInfoIterator = entry.getValue().entrySet().iterator();
        }

        Map.Entry<String, Pair<TsTable, Set<String>>> tableEntry;
        while (tableInfoIterator.hasNext()) {
          tableEntry = tableInfoIterator.next();
          if (canShowTable(accessControl, userName, dbName, tableEntry.getKey())) {
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
    private Iterator<TRegionInfo> iterator;

    private RegionSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showRegion(new TShowRegionReq().setIsTableModel(true).setDatabases(null))
                .getRegionInfoListIterator();
      } catch (final Exception e) {
        lastException = e;
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
      } else {
        columnBuilders[12].appendNull();
      }
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static class PipeSupplier extends TsBlockSupplier {
    private Iterator<TShowPipeInfo> iterator;

    private PipeSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client.showPipe(new TShowPipeReq().setIsTableModel(true)).getPipeInfoListIterator();
      } catch (final Exception e) {
        lastException = e;
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
            PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
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
    private Iterator<PipePluginMeta> iterator;

    private PipePluginSupplier(final List<TSDataType> dataTypes) {
      super(dataTypes);
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
      } catch (final Exception e) {
        lastException = e;
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
    private Iterator<TShowTopicInfo> iterator;

    private TopicSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showTopic(new TShowTopicReq().setIsTableModel(true))
                .getTopicInfoList()
                .iterator();
      } catch (final Exception e) {
        lastException = e;
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
    private Iterator<TShowSubscriptionInfo> iterator;

    private SubscriptionSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator =
            client
                .showSubscription(new TShowSubscriptionReq().setIsTableModel(true))
                .getSubscriptionInfoList()
                .iterator();
      } catch (final Exception e) {
        lastException = e;
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
    private Iterator<Map.Entry<String, Map<String, Pair<TsTable, Set<String>>>>> dbIterator;
    private Iterator<Map.Entry<String, Pair<TsTable, Set<String>>>> tableInfoIterator;
    private String dbName;
    private TsTable currentTable;
    private final String userName;

    private ViewsSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      this.userName = userName;
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
      } catch (final Exception e) {
        lastException = e;
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
          if (!canShowDB(accessControl, userName, dbName)) {
            continue;
          }
          tableInfoIterator = entry.getValue().entrySet().iterator();
        }

        while (tableInfoIterator.hasNext()) {
          final Map.Entry<String, Pair<TsTable, Set<String>>> tableEntry = tableInfoIterator.next();
          if (!TreeViewSchema.isTreeViewTable(tableEntry.getValue().getLeft())
              || !canShowTable(accessControl, userName, dbName, tableEntry.getKey())) {
            continue;
          }
          currentTable = tableEntry.getValue().getLeft();
          return true;
        }
      }
      return true;
    }
  }

  private static class ModelsSupplier extends TsBlockSupplier {
    private ModelIterator iterator;

    private ModelsSupplier(final List<TSDataType> dataTypes) {
      super(dataTypes);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        iterator = new ModelIterator(client.showModel(new TShowModelReq()));
      } catch (final Exception e) {
        lastException = e;
      }
    }

    private static class ModelIterator implements Iterator<ModelInfoInString> {

      private int index = 0;
      private final TShowModelResp resp;

      private ModelIterator(TShowModelResp resp) {
        this.resp = resp;
      }

      @Override
      public boolean hasNext() {
        return index < resp.getModelIdListSize();
      }

      @Override
      public ModelInfoInString next() {
        String modelId = resp.getModelIdList().get(index++);
        return new ModelInfoInString(
            modelId,
            resp.getModelTypeMap().get(modelId),
            resp.getCategoryMap().get(modelId),
            resp.getStateMap().get(modelId));
      }
    }

    private static class ModelInfoInString {

      private final String modelId;
      private final String modelType;
      private final String category;
      private final String state;

      public ModelInfoInString(String modelId, String modelType, String category, String state) {
        this.modelId = modelId;
        this.modelType = modelType;
        this.category = category;
        this.state = state;
      }

      public String getModelId() {
        return modelId;
      }

      public String getModelType() {
        return modelType;
      }

      public String getCategory() {
        return category;
      }

      public String getState() {
        return state;
      }
    }

    @Override
    protected void constructLine() {
      final ModelInfoInString modelInfo = iterator.next();
      columnBuilders[0].writeBinary(
          new Binary(modelInfo.getModelId(), TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(modelInfo.getModelType(), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(
          new Binary(modelInfo.getCategory(), TSFileConfig.STRING_CHARSET));
      columnBuilders[3].writeBinary(new Binary(modelInfo.getState(), TSFileConfig.STRING_CHARSET));
      //      if (Objects.equals(modelType, ModelType.USER_DEFINED.toString())) {
      //        columnBuilders[3].writeBinary(
      //            new Binary(
      //                INPUT_SHAPE
      //                    + ReadWriteIOUtils.readString(modelInfo)
      //                    + OUTPUT_SHAPE
      //                    + ReadWriteIOUtils.readString(modelInfo)
      //                    + INPUT_DATA_TYPE
      //                    + ReadWriteIOUtils.readString(modelInfo)
      //                    + OUTPUT_DATA_TYPE
      //                    + ReadWriteIOUtils.readString(modelInfo),
      //                TSFileConfig.STRING_CHARSET));
      //        columnBuilders[4].writeBinary(
      //            new Binary(ReadWriteIOUtils.readString(modelInfo),
      // TSFileConfig.STRING_CHARSET));
      //      } else {
      //        columnBuilders[3].appendNull();
      //        columnBuilders[4].writeBinary(
      //            new Binary("Built-in model in IoTDB", TSFileConfig.STRING_CHARSET));
      //      }
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static class FunctionsSupplier extends TsBlockSupplier {

    private Iterator<UDFInformation> udfIterator;
    private Iterator<String> nameIterator;
    private Binary functionType;
    private Binary functionState;

    private FunctionsSupplier(final List<TSDataType> dataTypes) {
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
      } catch (final Exception e) {
        lastException = e;
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

  private static class ConfigurationsSupplier extends TsBlockSupplier {
    private Iterator<Pair<Binary, Binary>> resultIterator;

    private ConfigurationsSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
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
      } catch (final Exception e) {
        lastException = e;
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
    private TShowClusterResp showClusterResp;
    private Iterator<TConfigNodeLocation> configNodeIterator;
    private Iterator<TDataNodeLocation> dataNodeIterator;
    private Iterator<TAINodeLocation> aiNodeIterator;

    private NodesSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        showClusterResp = client.showCluster();
        configNodeIterator = showClusterResp.getConfigNodeListIterator();
        dataNodeIterator = showClusterResp.getDataNodeListIterator();
        aiNodeIterator = showClusterResp.getAiNodeListIterator();
      } catch (final Exception e) {
        lastException = e;
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
    private Iterator<TConfigNodeInfo4InformationSchema> configNodeIterator;

    private ConfigNodesSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        configNodeIterator =
            client.showConfigNodes4InformationSchema().getConfigNodesInfoListIterator();
      } catch (final Exception e) {
        lastException = e;
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
    private Iterator<TDataNodeInfo4InformationSchema> dataNodeIterator;

    private DataNodesSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      accessControl.checkUserIsAdmin(userName);
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeIterator = client.showDataNodes4InformationSchema().getDataNodesInfoListIterator();
      } catch (final Exception e) {
        lastException = e;
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
    protected Exception lastException;

    private TsBlockSupplier(final List<TSDataType> dataTypes) {
      this.resultBuilder = new TsBlockBuilder(dataTypes);
      this.columnBuilders = resultBuilder.getValueColumnBuilders();
    }

    @Override
    public TsBlock next() {
      if (Objects.nonNull(lastException)) {
        throw new NoSuchElementException(lastException.getMessage());
      }
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      while (hasNext() && !resultBuilder.isFull()) {
        constructLine();
      }
      final TsBlock result =
          resultBuilder.build(
              new RunLengthEncodedColumn(
                  TableScanOperator.TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
      resultBuilder.reset();
      return result;
    }

    protected abstract void constructLine();
  }
}
