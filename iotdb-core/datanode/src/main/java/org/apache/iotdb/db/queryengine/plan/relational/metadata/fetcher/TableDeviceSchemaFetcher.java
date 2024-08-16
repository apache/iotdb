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
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractTraverseDevice;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory.truncateTailingNull;

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
      final FetchDevice statement, final MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    final String database = statement.getDatabase();
    final String table = statement.getTableName();
    final TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);

    final ExecutionResult executionResult =
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

    final List<ColumnHeader> columnHeaderList =
        coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();
    final int idLength = DataNodeTableCache.getInstance().getTable(database, table).getIdNums();
    final Map<TableDeviceId, Map<String, String>> fetchedDeviceSchema = new HashMap<>();

    try {
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new RuntimeException("Fetch Table Device Schema failed. ", e);
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          break;
        }
        final Column[] columns = tsBlock.get().getValueColumns();
        for (int i = 0; i < tsBlock.get().getPositionCount(); i++) {
          final String[] nodes = new String[idLength];
          final Map<String, String> attributeMap = new HashMap<>();

          constructNodsArrayAndAttributeMap(
              attributeMap, nodes, 0, columnHeaderList, columns, tableInstance, i);

          fetchedDeviceSchema.put(
              new TableDeviceId((String[]) truncateTailingNull(nodes)), attributeMap);
        }
      }
    } catch (final Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return fetchedDeviceSchema;
  }

  public List<DeviceEntry> fetchDeviceSchemaForDataQuery(
      final String database,
      final String table,
      final List<Expression> expressionList,
      final List<String> attributeColumns,
      final MPPQueryContext queryContext) {
    final List<DeviceEntry> deviceEntryList = new ArrayList<>();
    final ShowDevice statement = new ShowDevice(database, table);
    final TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);
    if (tableInstance == null) {
      throw new SemanticException(String.format("Table '%s.%s' does not exist", database, table));
    }

    if (parseFilter4TraverseDevice(
        database,
        tableInstance,
        expressionList,
        statement,
        deviceEntryList,
        attributeColumns,
        queryContext,
        false)) {
      fetchMissingDeviceSchemaForQuery(
          database, tableInstance, attributeColumns, statement, deviceEntryList, queryContext);
    }

    // TODO table metadata:  implement deduplicate during schemaRegion execution
    // TODO table metadata:  need further process on input predicates and transform them into
    // disjoint sets
    final Set<DeviceEntry> set = new LinkedHashSet<>(deviceEntryList);
    return new ArrayList<>(set);
  }

  // Used by show/count device and update device.
  // Update device will not access cache
  public boolean parseFilter4TraverseDevice(
      final String database,
      final TsTable tableInstance,
      final List<Expression> expressionList,
      final AbstractTraverseDevice statement,
      final List<DeviceEntry> deviceEntryList,
      final List<String> attributeColumns,
      final MPPQueryContext queryContext,
      final boolean isDirectDeviceQuery) {
    final Pair<List<Expression>, List<Expression>> separatedExpression =
        SchemaPredicateUtil.separateIdDeterminedPredicate(
            expressionList, tableInstance, queryContext, isDirectDeviceQuery);
    final List<Expression> idDeterminedPredicateList = separatedExpression.left; // and-concat
    final List<Expression> idFuzzyPredicateList = separatedExpression.right; // and-concat

    final Expression compactedIdFuzzyPredicate =
        SchemaPredicateUtil.compactDeviceIdFuzzyPredicate(idFuzzyPredicateList);

    // Each element represents one batch of possible devices
    // expressions inner each element are and-concat representing conditions of different column
    final List<Map<Integer, List<SchemaFilter>>> index2FilterMapList =
        SchemaPredicateUtil.convertDeviceIdPredicateToOrConcatList(
            idDeterminedPredicateList, tableInstance);
    // If List<Expression> in idPredicateList contains all id columns comparison which can use
    // SchemaCache, we store its index.
    final List<Integer> idSingleMatchIndexList =
        SchemaPredicateUtil.extractIdSingleMatchExpressionCases(index2FilterMapList, tableInstance);
    // Store missing cache index in idSingleMatchIndexList
    final List<Integer> idSingleMatchPredicateNotInCache = new ArrayList<>();

    final boolean isExactDeviceQuery = idSingleMatchIndexList.size() == index2FilterMapList.size();

    // If the query is exact, then we can specify the fetch paths to determine the related schema
    // regions
    final List<IDeviceID> fetchPaths = isExactDeviceQuery ? new ArrayList<>() : null;

    if (!idSingleMatchIndexList.isEmpty()) {
      // Try get from cache
      final ConvertSchemaPredicateToFilterVisitor visitor =
          new ConvertSchemaPredicateToFilterVisitor();
      final ConvertSchemaPredicateToFilterVisitor.Context context =
          new ConvertSchemaPredicateToFilterVisitor.Context(tableInstance);
      final DeviceInCacheFilterVisitor filterVisitor =
          new DeviceInCacheFilterVisitor(attributeColumns);
      final SchemaFilter fuzzyFilter =
          compactedIdFuzzyPredicate == null
              ? null
              : compactedIdFuzzyPredicate.accept(visitor, context);
      for (final int index : idSingleMatchIndexList) {
        if (!tryGetDeviceInCache(
            deviceEntryList,
            database,
            tableInstance,
            index2FilterMapList.get(index),
            o -> fuzzyFilter == null || filterVisitor.process(fuzzyFilter, o),
            attributeColumns,
            fetchPaths,
            isDirectDeviceQuery)) {
          idSingleMatchPredicateNotInCache.add(index);
        }
      }
    }

    if (idSingleMatchIndexList.size() < index2FilterMapList.size()
        || !idSingleMatchPredicateNotInCache.isEmpty()) {
      final List<List<SchemaFilter>> idPredicateForFetch =
          new ArrayList<>(
              index2FilterMapList.size()
                  - idSingleMatchIndexList.size()
                  + idSingleMatchPredicateNotInCache.size());
      int idx1 = 0;
      int idx2 = 0;
      for (int i = 0; i < index2FilterMapList.size(); i++) {
        if (idx1 >= idSingleMatchIndexList.size() || i != idSingleMatchIndexList.get(idx1)) {
          idPredicateForFetch.add(
              index2FilterMapList.get(i).values().stream()
                  .flatMap(Collection::stream)
                  .collect(Collectors.toList()));
        } else {
          idx1++;
          if (idx2 >= idSingleMatchPredicateNotInCache.size()
              || i == idSingleMatchPredicateNotInCache.get(idx2)) {
            idPredicateForFetch.add(
                index2FilterMapList.get(i).values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()));
            idx2++;
          }
        }
      }
      statement.setIdDeterminedFilterList(idPredicateForFetch);
      statement.setIdFuzzyPredicate(compactedIdFuzzyPredicate);
      statement.setPartitionKeyList(fetchPaths);
      return true;
    }
    return false;
  }

  // Return whether all of required info of current device is in cache
  private boolean tryGetDeviceInCache(
      final List<DeviceEntry> deviceEntryList,
      final String database,
      final TsTable tableInstance,
      final Map<Integer, List<SchemaFilter>> idFilters,
      final Predicate<DeviceEntry> check,
      final List<String> attributeColumns,
      final List<IDeviceID> fetchPaths,
      final boolean isDirectDeviceQuery) {
    String[] idValues = new String[tableInstance.getIdNums()];
    for (final List<SchemaFilter> schemaFilters : idFilters.values()) {
      final IdFilter idFilter = (IdFilter) schemaFilters.get(0);
      final SchemaFilter childFilter = idFilter.getChild();
      idValues[idFilter.getIndex()] = ((PreciseFilter) childFilter).getValue();
    }

    idValues = (String[]) truncateTailingNull(idValues);
    final Map<String, String> attributeMap =
        cache.getDeviceAttribute(database, tableInstance.getTableName(), idValues);

    final IDeviceID deviceID = convertIdValuesToDeviceID(idValues, tableInstance);
    if (attributeMap == null) {
      if (Objects.nonNull(fetchPaths)) {
        fetchPaths.add(deviceID);
      }
      return false;
    }
    final List<String> attributeValues = new ArrayList<>(attributeColumns.size());
    for (final String attributeKey : attributeColumns) {
      if (!attributeMap.containsKey(attributeKey)) {
        // The attributes may be updated and the cache entry is stale
        if (Objects.nonNull(fetchPaths)) {
          fetchPaths.add(deviceID);
        }
        return false;
      }
      attributeValues.add(attributeMap.get(attributeKey));
    }

    final DeviceEntry deviceEntry = new DeviceEntry(deviceID, attributeValues);
    // TODO table metadata: process cases that selected attr columns different from those used for
    // predicate
    if (check.test(deviceEntry)) {
      deviceEntryList.add(deviceEntry);
      // If we partially hit cache in direct device query, we must fetch for all the predicates
      // because now we do not support combining memory source and other sources
      if (isDirectDeviceQuery) {
        fetchPaths.add(deviceID);
      }
    }
    return true;
  }

  private IDeviceID convertIdValuesToDeviceID(
      final String[] idValues, final TsTable tableInstance) {
    // Convert to IDeviceID
    final String[] deviceIdNodes = new String[idValues.length + 1];
    deviceIdNodes[0] = tableInstance.getTableName();
    System.arraycopy(idValues, 0, deviceIdNodes, 1, idValues.length);
    return IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdNodes);
  }

  private void fetchMissingDeviceSchemaForQuery(
      final String database,
      final TsTable tableInstance,
      final List<String> attributeColumns,
      final ShowDevice statement,
      final List<DeviceEntry> deviceEntryList,
      final MPPQueryContext mppQueryContext) {

    final String table = tableInstance.getTableName();

    final long queryId = SessionManager.getInstance().requestQueryId();
    final ExecutionResult executionResult =
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

    final List<ColumnHeader> columnHeaderList =
        coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();
    final int idLength = DataNodeTableCache.getInstance().getTable(database, table).getIdNums();
    Throwable t = null;
    try {
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new RuntimeException("Fetch Table Device Schema failed. ", e);
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          break;
        }
        final Column[] columns = tsBlock.get().getValueColumns();
        for (int i = 0; i < tsBlock.get().getPositionCount(); i++) {
          String[] nodes = new String[idLength + 1];
          nodes[0] = table;
          final Map<String, String> attributeMap = new HashMap<>();
          constructNodsArrayAndAttributeMap(
              attributeMap, nodes, 1, columnHeaderList, columns, tableInstance, i);
          nodes = (String[]) truncateTailingNull(nodes);
          final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(nodes);
          final DeviceEntry deviceEntry =
              new DeviceEntry(
                  deviceID,
                  attributeColumns.stream().map(attributeMap::get).collect(Collectors.toList()));
          mppQueryContext.reserveMemoryForFrontEnd(deviceEntry.ramBytesUsed());
          deviceEntryList.add(deviceEntry);
          // Only cache those exact device query
          // Fetch paths is null iff there are fuzzy queries related to id columns
          if (Objects.nonNull(statement.getPartitionKeyList())) {
            cache.put(database, table, Arrays.copyOfRange(nodes, 1, nodes.length), attributeMap);
          }
        }
      }
    } catch (final Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
  }

  private void constructNodsArrayAndAttributeMap(
      final Map<String, String> attributeMap,
      final String[] nodes,
      int startIndex,
      final List<ColumnHeader> columnHeaderList,
      final Column[] columns,
      final TsTable tableInstance,
      final int rowIndex) {
    for (int j = 0; j < columnHeaderList.size(); j++) {
      final TsTableColumnSchema columnSchema =
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
