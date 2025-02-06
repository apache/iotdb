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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDescTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.schemaengine.table.InformationSchemaUtils;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_RESULT_NODES;
import static org.apache.iotdb.commons.schema.table.TsTable.TTL_PROPERTY;

public class InformationSchemaContentSupplierFactory {
  private InformationSchemaContentSupplierFactory() {}

  public static Iterator<TsBlock> getSupplier(
      final String tableName, final List<TSDataType> dataTypes, final String userName) {
    switch (tableName) {
      case InformationSchema.QUERIES:
        return new QueriesSupplier(dataTypes);
      case InformationSchema.DATABASES:
        return new DatabaseSupplier(dataTypes, userName);
      case InformationSchema.TABLES:
        return new TableSupplier(dataTypes, userName);
      case InformationSchema.COLUMNS:
        return new ColumnSupplier(dataTypes, userName);
      default:
        throw new UnsupportedOperationException("Unknown table: " + tableName);
    }
  }

  private static class QueriesSupplier extends TsBlockSupplier {
    private final long currTime = System.currentTimeMillis();
    // We initialize it later for the convenience of data preparation
    protected int totalSize;
    protected int nextConsumedIndex;
    private final List<IQueryExecution> queryExecutions =
        Coordinator.getInstance().getAllQueryExecutions();

    private QueriesSupplier(final List<TSDataType> dataTypes) {
      super(dataTypes);
      this.totalSize = queryExecutions.size();
    }

    @Override
    protected void constructLine() {
      IQueryExecution queryExecution = queryExecutions.get(nextConsumedIndex);

      if (queryExecution.getSQLDialect().equals(IClientSession.SqlDialect.TABLE)) {
        String[] splits = queryExecution.getQueryId().split("_");
        int dataNodeId = Integer.parseInt(splits[splits.length - 1]);

        columnBuilders[0].writeBinary(BytesUtils.valueOf(queryExecution.getQueryId()));
        columnBuilders[1].writeLong(queryExecution.getStartExecutionTime());
        columnBuilders[2].writeInt(dataNodeId);
        columnBuilders[3].writeFloat(
            (float) (currTime - queryExecution.getStartExecutionTime()) / 1000);
        columnBuilders[4].writeBinary(
            BytesUtils.valueOf(queryExecution.getExecuteSQL().orElse("UNKNOWN")));
        resultBuilder.declarePosition();
      }
      nextConsumedIndex++;
    }

    @Override
    public boolean hasNext() {
      return nextConsumedIndex < totalSize;
    }
  }

  private static class DatabaseSupplier extends TsBlockSupplier {
    private Iterator<Map.Entry<String, TDatabaseInfo>> iterator;
    private TDatabaseInfo currentDatabase;
    private final String userName;
    private boolean hasShownInformationSchema;

    private DatabaseSupplier(final List<TSDataType> dataTypes, final String userName) {
      super(dataTypes);
      this.userName = userName;
      try (final ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TShowDatabaseResp resp =
            client.showDatabase(
                new TGetDatabaseReq(Arrays.asList(ALL_RESULT_NODES), ALL_MATCH_SCOPE.serialize())
                    .setIsTableModel(true));
        iterator = resp.getDatabaseInfoMap().entrySet().iterator();
      } catch (final Exception e) {
        lastException = e;
      }
    }

    @Override
    protected void constructLine() {
      if (!hasShownInformationSchema) {
        InformationSchemaUtils.buildDatabaseTsBlock(s -> true, resultBuilder, true, false);
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
        if (!canShowDB(userName, InformationSchema.INFORMATION_DATABASE)) {
          hasShownInformationSchema = true;
        } else {
          return true;
        }
      }
      while (iterator.hasNext()) {
        final Map.Entry<String, TDatabaseInfo> result = iterator.next();
        if (!canShowDB(userName, result.getKey())) {
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
      final TTableInfo info = tableInfoIterator.next();
      columnBuilders[0].writeBinary(new Binary(dbName, TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(new Binary(info.getTableName(), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(new Binary(info.getTTL(), TSFileConfig.STRING_CHARSET));
      columnBuilders[3].writeBinary(
          new Binary(
              TableNodeStatus.values()[info.getState()].toString(), TSFileConfig.STRING_CHARSET));
      resultBuilder.declarePosition();
    }

    @Override
    public boolean hasNext() {
      // Get next table info iterator
      while (Objects.isNull(tableInfoIterator) || !tableInfoIterator.hasNext()) {
        if (!dbIterator.hasNext()) {
          return false;
        }
        final Map.Entry<String, List<TTableInfo>> entry = dbIterator.next();
        dbName = entry.getKey();
        if (!canShowDB(userName, dbName)) {
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
          if (!canShowDB(userName, dbName)) {
            continue;
          }
          tableInfoIterator = entry.getValue().entrySet().iterator();
        }

        final Map.Entry<String, Pair<TsTable, Set<String>>> tableEntry = tableInfoIterator.next();
        tableName = tableEntry.getKey();
        preDeletedColumns = tableEntry.getValue().getRight();
        columnSchemaIterator = tableEntry.getValue().getLeft().getColumnList().iterator();
      }
      return true;
    }
  }

  private static boolean canShowDB(final String userName, final String dbName) {
    try {
      Coordinator.getInstance().getAccessControl().checkCanShowOrUseDatabase(userName, dbName);
    } catch (final RuntimeException e) {
      if (e.getCause() instanceof IoTDBException) {
        return false;
      }
      throw e;
    }
    return true;
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
