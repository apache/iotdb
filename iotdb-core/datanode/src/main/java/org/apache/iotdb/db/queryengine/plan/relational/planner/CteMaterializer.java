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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceFetchException;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.statistics.FragmentInstanceStatisticsDrawer;
import org.apache.iotdb.db.queryengine.statistics.QueryStatisticsFetcher;
import org.apache.iotdb.db.queryengine.statistics.StatisticLine;
import org.apache.iotdb.db.utils.cte.CteDataStore;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CteMaterializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CteMaterializer.class);

  private static final Coordinator coordinator = Coordinator.getInstance();

  public void materializeCTE(Analysis analysis, MPPQueryContext context) {
    analysis
        .getNamedQueries()
        .forEach(
            (tableRef, query) -> {
              Table table = tableRef.getNode();
              if (query.isMaterialized() && !query.isDone()) {
                CteDataStore dataStore = fetchCteQueryResult(table, query, context);
                if (dataStore == null) {
                  // CTE query execution failed. Use inline instead of materialization
                  // in the outer query
                  query.setDone(false);
                  return;
                }

                context.addCteDataStore(table, dataStore);
                query.setDone(true);
              }
            });
  }

  public void cleanUpCTE(MPPQueryContext context) {
    Map<NodeRef<Table>, CteDataStore> cteDataStores = context.getCteDataStores();
    cteDataStores.values().forEach(CteDataStore::clear);
    cteDataStores.clear();
  }

  public CteDataStore fetchCteQueryResult(Table table, Query query, MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;
    CteDataStore cteDataStore = null;
    long startTime = System.nanoTime();
    try {
      final ExecutionResult executionResult =
          coordinator.executeForTableModel(
              query,
              new SqlParser(),
              SessionManager.getInstance().getCurrSession(),
              queryId,
              SessionManager.getInstance()
                  .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
              String.format("Materialize query for CTE '%s'", table.getName()),
              LocalExecutionPlanner.getInstance().metadata,
              context.getCteDataStores(),
              context.getExplainType(),
              context.getTimeOut(),
              false);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return null;
      }
      // query execution
      QueryExecution execution = (QueryExecution) coordinator.getQueryExecution(queryId);
      // get table schema
      DatasetHeader datasetHeader = coordinator.getQueryExecution(queryId).getDatasetHeader();
      TableSchema tableSchema = getTableSchema(datasetHeader, table.getName().toString());

      cteDataStore =
          new CteDataStore(
              query, tableSchema, datasetHeader.getColumnIndex2TsBlockColumnIndexList());
      while (execution.hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = execution.getBatchResult();
        } catch (final IoTDBException e) {
          LOGGER.warn("Fail to materialize CTE because {}", e.getMessage());
          return null;
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        context.reserveMemoryForFrontEnd(tsBlock.get().getRetainedSizeInBytes());
        if (!cteDataStore.addTsBlock(tsBlock.get())) {
          LOGGER.warn(
              "Fail to materialize CTE because the data size exceeded memory or the row count threshold");
          if (context.isExplainAnalyze()) {
            handleCteExplainAnalyzeResults(
                context,
                queryId,
                table,
                "!!! Failed to materialize CTE. The main query falls back to INLINE mode !!!");
          }
          context.releaseMemoryReservedForFrontEnd(cteDataStore.getCachedBytes());
          cteDataStore.clear();
          return null;
        }
      }

      if (context.isExplainAnalyze()) {
        handleCteExplainAnalyzeResults(context, queryId, table, null);
      } else if (context.isExplain()) {
        handleCteExplainResults(context, queryId, table);
      }

      return cteDataStore;
    } catch (final Throwable throwable) {
      if (cteDataStore != null) {
        context.releaseMemoryReservedForFrontEnd(cteDataStore.getCachedBytes());
        cteDataStore.clear();
      }
      t = throwable;
    } finally {
      long cost = System.nanoTime() - startTime;
      context.addCteMaterializationCost(table, cost);
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return null;
  }

  private TableSchema getTableSchema(DatasetHeader datasetHeader, String cteName) {
    final List<String> columnNames = datasetHeader.getRespColumns();
    final List<TSDataType> columnDataTypes = datasetHeader.getRespDataTypes();
    if (columnNames.size() != columnDataTypes.size()) {
      throw new IoTDBRuntimeException(
          "Size of column names and column data types do not match",
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    List<Integer> columnIndex2TsBlockColumnIndexList =
        datasetHeader.getColumnIndex2TsBlockColumnIndexList();
    if (columnIndex2TsBlockColumnIndexList == null) {
      columnIndex2TsBlockColumnIndexList =
          IntStream.range(0, columnNames.size()).boxed().collect(Collectors.toList());
    }
    // build column schema list of cte table based on column2BlockColumnIndex
    final List<ColumnSchema> columnSchemaList =
        columnIndex2TsBlockColumnIndexList.stream()
            .map(
                index ->
                    new ColumnSchema(
                        columnNames.get(index),
                        TypeFactory.getType(columnDataTypes.get(index)),
                        false,
                        TsTableColumnCategory.FIELD))
            .collect(Collectors.toList());
    return new TableSchema(cteName, columnSchemaList);
  }

  private List<String> getCteExplainAnalyzeLines(
      FragmentInstanceStatisticsDrawer fragmentInstanceStatisticsDrawer,
      List<FragmentInstance> instances,
      boolean verbose)
      throws FragmentInstanceFetchException {
    if (instances == null || instances.isEmpty()) {
      return ImmutableList.of();
    }

    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
        coordinator.getInternalServiceClientManager();
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics =
        QueryStatisticsFetcher.fetchAllStatistics(instances, clientManager);
    List<StatisticLine> statisticLines =
        fragmentInstanceStatisticsDrawer.renderFragmentInstances(instances, allStatistics, verbose);
    return statisticLines.stream().map(StatisticLine::getValue).collect(Collectors.toList());
  }

  private void handleCteExplainAnalyzeResults(
      MPPQueryContext context, long queryId, Table table, String warnMessage) {
    QueryExecution execution = (QueryExecution) coordinator.getQueryExecution(queryId);
    DistributedQueryPlan distributedQueryPlan = execution.getDistributedPlan();
    if (distributedQueryPlan == null) {
      context.addCteExplainResult(table, new Pair<>(0, ImmutableList.of()));
      return;
    }

    MPPQueryContext cteContext = execution.getContext();
    FragmentInstanceStatisticsDrawer fragmentInstanceStatisticsDrawer =
        new FragmentInstanceStatisticsDrawer();
    fragmentInstanceStatisticsDrawer.renderPlanStatistics(cteContext);
    fragmentInstanceStatisticsDrawer.renderDispatchCost(cteContext);

    try {
      List<String> lines =
          getCteExplainAnalyzeLines(
              fragmentInstanceStatisticsDrawer,
              distributedQueryPlan.getInstances(),
              context.isVerbose());
      int maxLineLength = fragmentInstanceStatisticsDrawer.getMaxLineLength();
      if (warnMessage != null) {
        lines.add(warnMessage);
        maxLineLength = Math.max(maxLineLength, warnMessage.length());
      }
      context.addCteExplainResult(table, new Pair<>(maxLineLength, lines));
    } catch (FragmentInstanceFetchException e) {
      throw new IoTDBRuntimeException(
          "Failed to fetch fragment instance statistics",
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private void handleCteExplainResults(MPPQueryContext context, long queryId, Table table) {
    QueryExecution execution = (QueryExecution) coordinator.getQueryExecution(queryId);
    DistributedQueryPlan distributedQueryPlan = execution.getDistributedPlan();
    if (distributedQueryPlan == null) {
      context.addCteExplainResult(table, new Pair<>(0, ImmutableList.of()));
      return;
    }

    List<String> lines = distributedQueryPlan.getPlanText();
    context.addCteExplainResult(table, new Pair<>(-1, lines));
  }

  private static class CteMaterializerHolder {
    private static CteMaterializer INSTANCE = new CteMaterializer();

    private CteMaterializerHolder() {
      // Empty constructor
    }
  }

  public static CteMaterializer getInstance() {
    return CteMaterializerHolder.INSTANCE;
  }

  public static void setInstance(CteMaterializer instance) {
    CteMaterializerHolder.INSTANCE = instance;
  }
}
