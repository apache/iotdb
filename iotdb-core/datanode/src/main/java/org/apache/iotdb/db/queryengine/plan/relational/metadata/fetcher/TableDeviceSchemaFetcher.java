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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceIdFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.schema.ConvertSchemaPredicateToFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TableDeviceSchemaFetcher {

  private final SqlParser relationSqlParser = new SqlParser();

  private static final Logger LOGGER = LoggerFactory.getLogger(TableDeviceSchemaFetcher.class);

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();

  private final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

  private TableDeviceSchemaFetcher() {
    // do nothing
  }

  private static class TableDeviceSchemaFetcherHolder {
    private static final TableDeviceSchemaFetcher INSTANCE = new TableDeviceSchemaFetcher();
  }

  public static TableDeviceSchemaFetcher getInstance() {
    return TableDeviceSchemaFetcherHolder.INSTANCE;
  }

  public TableDeviceSchemaCache getTableDeviceCache() {
    return cache;
  }

  Map<TableDeviceId, Map<String, String>> fetchMissingDeviceSchemaForDataInsertion(
      FetchDevice statement, MPPQueryContext context) {
    long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    String database = statement.getDatabase();
    String table = statement.getTableName();
    TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);

    ExecutionResult executionResult =
        coordinator.executeForTableModel(
            statement,
            relationSqlParser,
            SessionManager.getInstance().getCurrSession(),
            queryId,
            SessionManager.getInstance()
                .getSessionInfo(SessionManager.getInstance().getCurrSession()),
            "Fetch Device for insert",
            LocalExecutionPlanner.getInstance().metadata,
            config.getQueryTimeoutThreshold());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }

    List<ColumnHeader> columnHeaderList =
        coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();
    int idLength = DataNodeTableCache.getInstance().getTable(database, table).getIdNums();
    Map<TableDeviceId, Map<String, String>> fetchedDeviceSchema = new HashMap<>();

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
          String[] nodes = new String[idLength];
          Map<String, String> attributeMap = new HashMap<>();

          constructNodsArrayAndAttributeMap(
              attributeMap, nodes, 0, columnHeaderList, columns, tableInstance, i);

          fetchedDeviceSchema.put(new TableDeviceId(nodes), attributeMap);
        }
      }
    } catch (Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return fetchedDeviceSchema;
  }

  public List<DeviceEntry> fetchDeviceSchemaForDataQuery(
      String database,
      String table,
      List<Expression> expressionList,
      List<String> attributeColumns,
      MPPQueryContext queryContext) {
    List<DeviceEntry> deviceEntryList = new ArrayList<>();

    TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);
    if (tableInstance == null) {
      throw new SemanticException(String.format("Table '%s.%s' does not exist", database, table));
    }
    Pair<List<Expression>, List<Expression>> separatedExpression =
        SchemaPredicateUtil.separateIdDeterminedPredicate(expressionList, tableInstance);
    List<Expression> idDeterminedPredicateList = separatedExpression.left; // and-concat
    List<Expression> idFuzzyPredicateList = separatedExpression.right; // and-concat

    // here we use binary tree way, because the following SchemaFilter only support binary OrFilter
    // TODO table metadata: add multi way OrFilter for SchemaFilter
    Expression compactedIdFuzzyPredicate =
        SchemaPredicateUtil.compactDeviceIdFuzzyPredicate(idFuzzyPredicateList);

    // each element represents one batch of possible devices
    // expressions inner each element are and-concat representing conditions of different column
    List<List<Expression>> idPredicateList =
        SchemaPredicateUtil.convertDeviceIdPredicateToOrConcatList(idDeterminedPredicateList);
    // if List<Expression> in idPredicateList contains all id columns comparison which can use
    // SchemaCache, we store its index.
    List<Integer> idSingleMatchIndexList =
        SchemaPredicateUtil.extractIdSingleMatchExpressionCases(idPredicateList, tableInstance);
    // store missing cache index in idSingleMatchIndexList
    List<Integer> idSingleMatchPredicateNotInCache = new ArrayList<>();

    if (!idSingleMatchIndexList.isEmpty()) {
      // try get from cache
      ConvertSchemaPredicateToFilterVisitor visitor = new ConvertSchemaPredicateToFilterVisitor();
      ConvertSchemaPredicateToFilterVisitor.Context context =
          new ConvertSchemaPredicateToFilterVisitor.Context(tableInstance);
      DeviceInCacheFilterVisitor filterVisitor = new DeviceInCacheFilterVisitor(attributeColumns);
      SchemaFilter fuzzyFilter =
          compactedIdFuzzyPredicate == null
              ? null
              : compactedIdFuzzyPredicate.accept(visitor, context);
      for (int index : idSingleMatchIndexList) {
        if (!tryGetDeviceInCache(
            deviceEntryList,
            database,
            tableInstance,
            idPredicateList.get(index).stream()
                .map(o -> o.accept(visitor, context))
                .collect(Collectors.toList()),
            o -> fuzzyFilter == null || filterVisitor.process(fuzzyFilter, o),
            attributeColumns)) {
          idSingleMatchPredicateNotInCache.add(index);
        }
      }
    }

    if (idSingleMatchIndexList.size() < idPredicateList.size()
        || !idSingleMatchPredicateNotInCache.isEmpty()) {
      List<List<Expression>> idPredicateForFetch =
          new ArrayList<>(
              idPredicateList.size()
                  - idSingleMatchIndexList.size()
                  + idSingleMatchPredicateNotInCache.size());
      int idx1 = 0;
      int idx2 = 0;
      for (int i = 0; i < idPredicateList.size(); i++) {
        if (idx1 >= idSingleMatchIndexList.size() || i != idSingleMatchIndexList.get(idx1)) {
          idPredicateForFetch.add(idPredicateList.get(i));
        } else {
          idx1++;
          if (idx2 >= idSingleMatchPredicateNotInCache.size()
              || i == idSingleMatchPredicateNotInCache.get(idx2)) {
            idPredicateForFetch.add(idPredicateList.get(i));
            idx2++;
          }
        }
      }
      fetchMissingDeviceSchemaForQuery(
          database,
          tableInstance,
          attributeColumns,
          idPredicateForFetch,
          compactedIdFuzzyPredicate,
          deviceEntryList,
          // only cache those exact device query
          idSingleMatchIndexList.size() == idPredicateList.size(),
          queryContext);
    }

    // TODO table metadata:  implement deduplicate during schemaRegion execution
    // TODO table metadata:  need further process on input predicates and transform them into
    // disjoint sets
    Set<DeviceEntry> set = new LinkedHashSet<>(deviceEntryList);
    return new ArrayList<>(set);
  }

  // return whether all of required info of current device is in cache
  private boolean tryGetDeviceInCache(
      List<DeviceEntry> deviceEntryList,
      String database,
      TsTable tableInstance,
      List<SchemaFilter> idFilters,
      Predicate<DeviceEntry> check,
      List<String> attributeColumns) {
    String[] idValues = new String[tableInstance.getIdNums()];
    for (SchemaFilter schemaFilter : idFilters) {
      DeviceIdFilter idFilter = (DeviceIdFilter) schemaFilter;
      idValues[idFilter.getIndex()] = idFilter.getValue();
    }

    Map<String, String> attributeMap =
        cache.getDeviceAttribute(database, tableInstance.getTableName(), idValues);
    if (attributeMap == null) {
      return false;
    }
    List<String> attributeValues = new ArrayList<>(attributeColumns.size());
    for (String attributeKey : attributeColumns) {
      String value = attributeMap.get(attributeKey);
      // TODO table metadata: what if the value is null?
      attributeValues.add(value);
    }
    String[] deviceIdNodes = new String[idValues.length + 1];
    deviceIdNodes[0] = tableInstance.getTableName();
    System.arraycopy(idValues, 0, deviceIdNodes, 1, idValues.length);
    DeviceEntry deviceEntry =
        new DeviceEntry(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdNodes), attributeValues);
    // TODO table metadata: process cases that selected attr columns different from those used for
    // predicate
    if (check.test(deviceEntry)) {
      deviceEntryList.add(deviceEntry);
    }
    return true;
  }

  private void fetchMissingDeviceSchemaForQuery(
      String database,
      TsTable tableInstance,
      List<String> attributeColumns,
      List<List<Expression>> idDeterminedPredicateList,
      Expression idFuzzyPredicate,
      List<DeviceEntry> deviceEntryList,
      boolean cacheFetchedDevice,
      MPPQueryContext mppQueryContext) {

    String table = tableInstance.getTableName();

    long queryId = SessionManager.getInstance().requestQueryId();
    ShowDevice statement =
        new ShowDevice(database, table, idDeterminedPredicateList, idFuzzyPredicate);
    ExecutionResult executionResult =
        coordinator.executeForTableModel(
            statement,
            relationSqlParser,
            SessionManager.getInstance().getCurrSession(),
            queryId,
            SessionManager.getInstance()
                .getSessionInfo(SessionManager.getInstance().getCurrSession()),
            String.format(
                "fetch device for query %s : %s",
                mppQueryContext.getQueryId(), mppQueryContext.getSql()),
            LocalExecutionPlanner.getInstance().metadata,
            config.getQueryTimeoutThreshold());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }

    List<ColumnHeader> columnHeaderList =
        coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();
    int idLength = DataNodeTableCache.getInstance().getTable(database, table).getIdNums();
    Throwable t = null;
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
          nodes[0] = table;
          Map<String, String> attributeMap = new HashMap<>();
          constructNodsArrayAndAttributeMap(
              attributeMap, nodes, 1, columnHeaderList, columns, tableInstance, i);
          IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(nodes);
          // TODO table metadata: add memory control in query
          deviceEntryList.add(
              new DeviceEntry(
                  deviceID,
                  attributeColumns.stream().map(attributeMap::get).collect(Collectors.toList())));
          if (cacheFetchedDevice) {
            cache.put(database, table, Arrays.copyOfRange(nodes, 1, nodes.length), attributeMap);
          }
        }
      }
    } catch (Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
  }

  private void constructNodsArrayAndAttributeMap(
      Map<String, String> attributeMap,
      String[] nodes,
      int startIndex,
      List<ColumnHeader> columnHeaderList,
      Column[] columns,
      TsTable tableInstance,
      int rowIndex) {
    for (int j = 0; j < columnHeaderList.size(); j++) {
      TsTableColumnSchema columnSchema =
          tableInstance.getColumnSchema(columnHeaderList.get(j).getColumnName());
      // means that TsTable tableInstance which previously fetched is outdated, but it's ok that we
      // ignore that newly added column here
      if (columnSchema == null) {
        continue;
      }
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        if (columns[j].isNull(rowIndex)) {
          nodes[startIndex] = null;
        } else {
          nodes[startIndex] =
              columns[j].getBinary(rowIndex).getStringValue(TSFileConfig.STRING_CHARSET);
        }
        startIndex++;
      } else {
        if (columns[j].isNull(rowIndex)) {
          attributeMap.put(columnSchema.getColumnName(), null);
        } else {
          attributeMap.put(
              columnSchema.getColumnName(),
              columns[j].getBinary(rowIndex).getStringValue(TSFileConfig.STRING_CHARSET));
        }
      }
    }
  }
}
