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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.impl.DeviceAttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceIdFilter;
import org.apache.iotdb.commons.schema.filter.impl.OrFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.internal.CreateTableDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTableDevicesStatement;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.Literal;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.StringLiteral;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class TableModelSchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static class TableModelSchemaFetcherHolder {
    private static final TableModelSchemaFetcher INSTANCE = new TableModelSchemaFetcher();
  }

  public static TableModelSchemaFetcher getInstance() {
    return TableModelSchemaFetcherHolder.INSTANCE;
  }

  private TableModelSchemaFetcher() {
    // do nothing
  }

  public void validateDeviceSchema(
      ITableDeviceSchemaValidation schemaValidation, MPPQueryContext context) {
    CreateTableDeviceStatement statement =
        new CreateTableDeviceStatement(
            schemaValidation.getDatabase(),
            schemaValidation.getTableName(),
            schemaValidation.getDeviceIdList(),
            schemaValidation.getAttributeColumnNameList(),
            schemaValidation.getAttributeValue());
    ExecutionResult executionResult =
        Coordinator.getInstance()
            .executeForTreeModel(
                statement,
                SessionManager.getInstance().requestQueryId(),
                context == null ? null : context.getSession(),
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance(),
                context == null || context.getQueryType().equals(QueryType.WRITE)
                    ? config.getQueryTimeoutThreshold()
                    : context.getTimeOut());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }
  }

  public List<DeviceEntry> fetchDeviceSchema(
      String database,
      String table,
      List<Expression> expressionList,
      List<String> attributeColumns) {
    List<DeviceEntry> deviceEntryList = new ArrayList<>();

    Coordinator coordinator = Coordinator.getInstance();
    long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);
    Pair<List<SchemaFilter>, List<SchemaFilter>> filters =
        transformExpression(expressionList, tableInstance);
    List<SchemaFilter> idFilters = filters.getLeft();
    List<SchemaFilter> attributeFilters = filters.getRight();
    ShowTableDevicesStatement statement =
        new ShowTableDevicesStatement(database, table, idFilters, attributeFilters);
    ExecutionResult executionResult =
        Coordinator.getInstance()
            .executeForTreeModel(
                statement,
                queryId,
                SessionManager.getInstance()
                    .getSessionInfo(SessionManager.getInstance().getCurrSession()),
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance(),
                config.getQueryTimeoutThreshold());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }

    List<ColumnHeader> columnHeaderList =
        coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();
    int idLength = DataNodeTableCache.getInstance().getTable(database, table).getIdNums();
    Map<String, String> attributeMap;

    try {
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (IoTDBException e) {
          t = e;
          throw new RuntimeException("Fetch Table Device Schema failed. ", e);
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          break;
        }
        Column[] columns = tsBlock.get().getValueColumns();
        for (int i = 0; i < tsBlock.get().getPositionCount(); i++) {
          String[] nodes = new String[idLength + 1];
          nodes[0] = database + PATH_SEPARATOR + table;
          int idIndex = 0;
          attributeMap = new HashMap<>();
          for (int j = 0; j < columnHeaderList.size(); j++) {
            TsTableColumnSchema columnSchema =
                tableInstance.getColumnSchema(columnHeaderList.get(j).getColumnName());
            if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
              nodes[idIndex + 1] = columns[j].getBinary(i).toString();
              idIndex++;
            } else {
              attributeMap.put(columnSchema.getColumnName(), columns[j].getBinary(i).toString());
            }
          }
          IDeviceID deviceID = new StringArrayDeviceID(nodes);
          deviceEntryList.add(
              new DeviceEntry(
                  deviceID,
                  attributeColumns.stream().map(attributeMap::get).collect(Collectors.toList())));
        }
      }
    } catch (Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    System.out.println(deviceEntryList);
    return deviceEntryList;
  }

  private Pair<List<SchemaFilter>, List<SchemaFilter>> transformExpression(
      List<Expression> expressionList, TsTable table) {
    List<SchemaFilter> idDeterminedFilters = new ArrayList<>();
    List<SchemaFilter> idFuzzyFilters = new ArrayList<>();
    Map<String, Integer> indexMap = getIdColumnIndex(table);
    for (Expression expression : expressionList) {
      if (expression == null) {
        continue;
      }
      if (expression instanceof LogicalExpression) {
        LogicalExpression logicalExpression = (LogicalExpression) expression;
        SchemaFilter schemaFilter = transformToSchemaFilter(logicalExpression, table, indexMap);
        if (hasAttribute(schemaFilter)) {
          idFuzzyFilters.add(schemaFilter);
        } else {
          idDeterminedFilters.add(schemaFilter);
        }
      } else {
        SchemaFilter schemaFilter =
            transformToSchemaFilter((ComparisonExpression) expression, table, indexMap);
        if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.DEVICE_ATTRIBUTE)) {
          idFuzzyFilters.add(schemaFilter);
        } else {
          idDeterminedFilters.add(schemaFilter);
        }
      }
    }
    return new Pair<>(idDeterminedFilters, idFuzzyFilters);
  }

  private boolean hasAttribute(SchemaFilter schemaFilter) {
    if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.OR)) {
      return hasAttribute(((OrFilter) schemaFilter).getLeft())
          || hasAttribute(((OrFilter) schemaFilter).getRight());
    }

    return schemaFilter.getSchemaFilterType().equals(SchemaFilterType.DEVICE_ATTRIBUTE);
  }

  private SchemaFilter transformToSchemaFilter(
      LogicalExpression logicalExpression, TsTable table, Map<String, Integer> indexMap) {
    SchemaFilter left;
    SchemaFilter right;
    if (logicalExpression.getTerms().get(0) instanceof LogicalExpression) {
      left =
          transformToSchemaFilter(
              (LogicalExpression) (logicalExpression.getChildren().get(0)), table, indexMap);
    } else {
      left =
          transformToSchemaFilter(
              (ComparisonExpression) (logicalExpression.getChildren().get(0)), table, indexMap);
    }
    if (logicalExpression.getTerms().get(1) instanceof LogicalExpression) {
      right =
          transformToSchemaFilter(
              (LogicalExpression) (logicalExpression.getChildren().get(1)), table, indexMap);
    } else {
      right =
          transformToSchemaFilter(
              (ComparisonExpression) (logicalExpression.getChildren().get(1)), table, indexMap);
    }
    return new OrFilter(left, right);
  }

  private SchemaFilter transformToSchemaFilter(
      ComparisonExpression comparisonExpression, TsTable table, Map<String, Integer> indexMap) {
    String columnName;
    String value;
    if (comparisonExpression.getLeft() instanceof Literal) {
      value = ((StringLiteral) (comparisonExpression.getLeft())).getValue();
      if (comparisonExpression.getRight() instanceof Identifier) {
        columnName = ((Identifier) (comparisonExpression.getRight())).getValue();
      } else {
        columnName = ((SymbolReference) (comparisonExpression.getRight())).getName();
      }
    } else {
      value = ((StringLiteral) (comparisonExpression.getRight())).getValue();
      if (comparisonExpression.getLeft() instanceof Identifier) {
        columnName = ((Identifier) (comparisonExpression.getLeft())).getValue();
      } else {
        columnName = ((SymbolReference) (comparisonExpression.getLeft())).getName();
      }
    }
    if (table.getColumnSchema(columnName).getColumnCategory().equals(TsTableColumnCategory.ID)) {
      return new DeviceIdFilter(indexMap.get(columnName), value);
    } else {
      return new DeviceAttributeFilter(columnName, value);
    }
  }

  private Map<String, Integer> getIdColumnIndex(TsTable table) {
    Map<String, Integer> map = new HashMap<>();
    List<TsTableColumnSchema> columnSchemaList = table.getColumnList();
    int idIndex = 0;
    for (TsTableColumnSchema columnSchema : columnSchemaList) {
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        map.put(columnSchema.getColumnName(), idIndex);
        idIndex++;
      }
    }
    return map;
  }
}
