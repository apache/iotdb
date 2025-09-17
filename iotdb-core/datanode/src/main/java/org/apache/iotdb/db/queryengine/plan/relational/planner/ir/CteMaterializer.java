/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.utils.cte.CteDataStore;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CteMaterializer {

  private static final Coordinator coordinator = Coordinator.getInstance();

  private CteMaterializer() {}

  public static void materializeCTE(Analysis analysis, MPPQueryContext context) {
    Set<Query> materializedQueries = new HashSet<>();
    analysis
        .getNamedQueries()
        .forEach(
            (tableRef, query) -> {
              Table table = tableRef.getNode();
              if (query.isMaterialized() && !materializedQueries.contains(query)) {
                CteDataStore dataStore = fetchCteQueryResult(table, query, context);
                if (dataStore == null) {
                  query.setMaterialized(false);
                } else {
                  context.addCteDataStore(table, dataStore);
                  context.reserveMemoryForFrontEnd(dataStore.getCachedBytes());
                  materializedQueries.add(query);
                }
              }
            });
  }

  public static void cleanUpCTE(MPPQueryContext context) {
    Map<NodeRef<Table>, CteDataStore> cteDataStores = context.getCteDataStores();
    cteDataStores
        .values()
        .forEach(
            dataStore -> {
              context.releaseMemoryReservedForFrontEnd(dataStore.getCachedBytes());
              dataStore.clear();
            });
    cteDataStores.clear();
  }

  public static CteDataStore fetchCteQueryResult(
      Table table, Query query, MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;
    try {
      final ExecutionResult executionResult =
          coordinator.executeForTableModel(
              query,
              new SqlParser(),
              SessionManager.getInstance().getCurrSession(),
              queryId,
              SessionManager.getInstance()
                  .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
              "Materialize common table expression",
              LocalExecutionPlanner.getInstance().metadata,
              context.getTimeOut(),
              false);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return null;
      }

      // get table schema
      DatasetHeader datasetHeader = coordinator.getQueryExecution(queryId).getDatasetHeader();
      TableSchema tableSchema = getTableSchema(datasetHeader, table.getName().toString());

      CteDataStore cteDataStore = new CteDataStore(query, tableSchema);
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new IoTDBRuntimeException(
              String.format("Fail to materialize CTE because %s", e.getMessage()),
              e.getErrorCode(),
              e.isUserException());
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        if (!cteDataStore.addTsBlock(tsBlock.get())) {
          return null;
        }
      }
      return cteDataStore;
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return null;
  }

  private static TableSchema getTableSchema(DatasetHeader datasetHeader, String cteName) {
    final List<String> columnNames = datasetHeader.getRespColumns();
    final List<TSDataType> columnDataTypes = datasetHeader.getRespDataTypes();
    if (columnNames.size() != columnDataTypes.size()) {
      throw new IoTDBRuntimeException(
          "Size of column names and column data types do not match",
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    final Map<String, Integer> columnNameIndexMap = datasetHeader.getColumnNameIndexMap();
    final List<ColumnSchema> columnSchemaList = new ArrayList<>();

    // build name -> type map
    Map<String, TSDataType> columnNameDataTypeMap =
        IntStream.range(0, columnNames.size())
            .boxed()
            .collect(Collectors.toMap(columnNames::get, columnDataTypes::get));

    // build column schema list of cte table based on columnNameIndexMap
    columnNameIndexMap.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .forEach(
            entry ->
                columnSchemaList.add(
                    new ColumnSchema(
                        entry.getKey(),
                        TypeFactory.getType(columnNameDataTypeMap.get(entry.getKey())),
                        false,
                        TsTableColumnCategory.FIELD)));
    return new TableSchema(cteName, columnSchemaList);
  }
}
