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
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
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

  TableDeviceSchemaCache getTableDeviceCache() {
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
            null,
            SessionManager.getInstance().getCurrSession(),
            queryId,
            SessionManager.getInstance()
                .getSessionInfo(SessionManager.getInstance().getCurrSession()),
            "",
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
          int idIndex = 0;
          Map<String, String> attributeMap = new HashMap<>();
          for (int j = 0; j < columnHeaderList.size(); j++) {
            TsTableColumnSchema columnSchema =
                tableInstance.getColumnSchema(columnHeaderList.get(j).getColumnName());
            if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
              if (columns[j].isNull(i)) {
                nodes[idIndex] = null;
              } else {
                nodes[idIndex] = columns[j].getBinary(i).toString();
              }
              idIndex++;
            } else {
              if (columns[j].isNull(i)) {
                attributeMap.put(columnSchema.getColumnName(), null);
              } else {
                attributeMap.put(columnSchema.getColumnName(), columns[j].getBinary(i).toString());
              }
            }
          }
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
      List<String> attributeColumns) {
    List<DeviceEntry> deviceEntryList = new ArrayList<>();

    TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);
    Pair<List<Expression>, List<Expression>> separatedExpression =
        SchemaPredicateUtil.separateIdDeterminedPredicate(expressionList, tableInstance);
    List<Expression> idDeterminedPredicateList = separatedExpression.left; // and-concat
    List<Expression> idFuzzyPredicateList = separatedExpression.right; // and-concat
    Expression compactedIdFuzzyPredicate =
        SchemaPredicateUtil.compactDeviceIdFuzzyPredicate(idFuzzyPredicateList);

    // each element represents one batch of possible devices
    // expressions inner each element are and-concat representing conditions of different column
    List<List<Expression>> idPredicateList =
        SchemaPredicateUtil.convertDeviceIdPredicateToOrConcatList(idDeterminedPredicateList);
    // List<Expression> in idPredicateList contains all id columns comparison which can use
    // SchemaCache
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
      SchemaFilter attributeFilter =
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
            o -> attributeFilter == null || filterVisitor.process(attributeFilter, o),
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
        //
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
          idSingleMatchIndexList.size() == idPredicateList.size());
    }

    // todo implement deduplicate during schemaRegion execution
    // todo need further process on input predicates and transform them into disjoint sets
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
      if (value == null) {
        return false;
      } else {
        attributeValues.add(value);
      }
    }
    String[] deviceIdNodes = new String[idValues.length + 1];
    deviceIdNodes[0] = tableInstance.getTableName();
    System.arraycopy(idValues, 0, deviceIdNodes, 1, idValues.length);
    DeviceEntry deviceEntry =
        new DeviceEntry(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdNodes), attributeValues);
    // todo process cases that selected attr columns different from those used for predicate
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
      boolean cacheFetchedDevice) {

    String table = tableInstance.getTableName();

    long queryId = SessionManager.getInstance().requestQueryId();
    ShowDevice statement =
        new ShowDevice(database, table, idDeterminedPredicateList, idFuzzyPredicate);
    ExecutionResult executionResult =
        coordinator.executeForTableModel(
            statement,
            null,
            SessionManager.getInstance().getCurrSession(),
            queryId,
            SessionManager.getInstance()
                .getSessionInfo(SessionManager.getInstance().getCurrSession()),
            "",
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
    Map<String, String> attributeMap;

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
          int idIndex = 0;
          attributeMap = new HashMap<>();
          for (int j = 0; j < columnHeaderList.size(); j++) {
            TsTableColumnSchema columnSchema =
                tableInstance.getColumnSchema(columnHeaderList.get(j).getColumnName());
            if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
              if (columns[j].isNull(i)) {
                nodes[idIndex + 1] = null;
              } else {
                nodes[idIndex + 1] = columns[j].getBinary(i).toString();
              }
              idIndex++;
            } else {
              if (columns[j].isNull(i)) {
                attributeMap.put(columnSchema.getColumnName(), null);
              } else {
                attributeMap.put(columnSchema.getColumnName(), columns[j].getBinary(i).toString());
              }
            }
          }
          IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(nodes);
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
}
