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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.schema.ConvertSchemaPredicateToFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.IDeviceSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceNormalSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractTraverseDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TableDeviceSchemaFetcher {

  private final SqlParser relationSqlParser = new SqlParser();

  private final Coordinator coordinator = Coordinator.getInstance();

  private final TableDeviceSchemaCache cache = TableDeviceSchemaCache.getInstance();

  private final TableDeviceCacheAttributeGuard attributeGuard =
      new TableDeviceCacheAttributeGuard();

  private TableDeviceSchemaFetcher() {
    // do nothing
  }

  private static class TableDeviceSchemaFetcherHolder {
    private static final TableDeviceSchemaFetcher INSTANCE = new TableDeviceSchemaFetcher();
  }

  public static TableDeviceSchemaFetcher getInstance() {
    return TableDeviceSchemaFetcherHolder.INSTANCE;
  }

  public TableDeviceCacheAttributeGuard getAttributeGuard() {
    return attributeGuard;
  }

  Map<IDeviceID, Map<String, Binary>> fetchMissingDeviceSchemaForDataInsertion(
      final FetchDevice statement, final MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    final String database = statement.getDatabase();
    final String table = statement.getTableName();
    final TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);

    // For the correctness of attribute remote update
    final Set<Long> queryIdSet = attributeGuard.addFetchQueryId(queryId);
    try {
      final ExecutionResult executionResult =
          coordinator.executeForTableModel(
              statement,
              relationSqlParser,
              SessionManager.getInstance().getCurrSession(),
              queryId,
              SessionManager.getInstance()
                  .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
              "Fetch Device for insert",
              LocalExecutionPlanner.getInstance().metadata,
              // Never timeout for insert
              Long.MAX_VALUE,
              false);

      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                executionResult.status.getMessage(), executionResult.status.getCode()));
      }

      final List<ColumnHeader> columnHeaderList =
          coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();
      final int idLength = DataNodeTableCache.getInstance().getTable(database, table).getIdNums();
      final Map<IDeviceID, Map<String, Binary>> fetchedDeviceSchema = new HashMap<>();

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
          final String[] nodes = new String[idLength + 1];
          final Map<String, Binary> attributeMap = new HashMap<>();
          constructNodsArrayAndAttributeMap(
              attributeMap, nodes, table, columnHeaderList, columns, tableInstance, i);

          fetchedDeviceSchema.put(IDeviceID.Factory.DEFAULT_FACTORY.create(nodes), attributeMap);
        }
      }

      fetchedDeviceSchema.forEach((key, value) -> cache.putAttributes(database, key, value));

      return fetchedDeviceSchema;
    } catch (final Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      queryIdSet.remove(queryId);
      attributeGuard.tryUpdateCache();
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
  }

  public List<DeviceEntry> fetchDeviceSchemaForDataQuery(
      final String database,
      final String table,
      final List<Expression> expressionList,
      final List<String> attributeColumns,
      final MPPQueryContext queryContext) {
    final List<DeviceEntry> deviceEntryList = new ArrayList<>();
    final TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);
    final AtomicBoolean mayContainDuplicateDevice = new AtomicBoolean(false);
    if (tableInstance == null) {
      TableMetadataImpl.throwTableNotExistsException(database, table);
    }
    final ShowDevice statement = new ShowDevice(database, table);

    if (parseFilter4TraverseDevice(
        tableInstance,
        expressionList,
        statement,
        deviceEntryList,
        attributeColumns,
        queryContext,
        mayContainDuplicateDevice,
        false)) {
      fetchMissingDeviceSchemaForQuery(
          tableInstance, attributeColumns, statement, deviceEntryList, queryContext);
    }

    // TODO table metadata:  implement deduplicate during schemaRegion execution
    // TODO table metadata:  need further process on input predicates and transform them into
    // disjoint sets
    return mayContainDuplicateDevice.get()
        ? new ArrayList<>(new LinkedHashSet<>(deviceEntryList))
        : deviceEntryList;
  }

  // Used by show/count device and update device.
  // Update device will not access cache
  public boolean parseFilter4TraverseDevice(
      final TsTable tableInstance,
      final List<Expression> expressionList,
      final AbstractTraverseDevice statement,
      final List<DeviceEntry> deviceEntryList,
      final List<String> attributeColumns,
      final MPPQueryContext queryContext,
      final AtomicBoolean mayContainDuplicateDevice,
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
            idDeterminedPredicateList, tableInstance, mayContainDuplicateDevice);
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

      final Predicate<AlignedDeviceEntry> check;
      if (Objects.isNull(compactedIdFuzzyPredicate)) {
        check = o -> true;
      } else {
        final SchemaFilter fuzzyFilter = compactedIdFuzzyPredicate.accept(visitor, context);
        // Currently if a fuzzy predicate exists, if it cannot be converted to a schema filter, or
        // this query is about tree device view, we abandon cache and just fetch remote. Later cache
        // will be a memory source and combine filter
        check =
            Objects.nonNull(fuzzyFilter) && !TreeViewSchema.isTreeViewTable(tableInstance)
                ? o -> filterVisitor.process(fuzzyFilter, o)
                : null;
      }

      for (final int index : idSingleMatchIndexList) {
        if (!tryGetDeviceInCache(
            deviceEntryList,
            statement.getDatabase(),
            tableInstance,
            index2FilterMapList.get(index),
            check,
            attributeColumns,
            fetchPaths,
            isDirectDeviceQuery,
            queryContext)) {
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
      // Return only the required attributes for non-schema queries
      // if there is no need to put to cache
      if (!isDirectDeviceQuery && Objects.isNull(fetchPaths)) {
        statement.setAttributeColumns(attributeColumns);
      }
      return true;
    }
    return false;
  }

  // Return whether all of required info of current device is in cache
  @SuppressWarnings("squid:S107")
  private boolean tryGetDeviceInCache(
      final List<DeviceEntry> deviceEntryList,
      final String database,
      final TsTable tableInstance,
      final Map<Integer, List<SchemaFilter>> idFilters,
      final Predicate<AlignedDeviceEntry> check,
      final List<String> attributeColumns,
      final List<IDeviceID> fetchPaths,
      final boolean isDirectDeviceQuery,
      final MPPQueryContext queryContext) {
    final String[] idValues = new String[tableInstance.getIdNums()];
    for (final List<SchemaFilter> schemaFilters : idFilters.values()) {
      final IdFilter idFilter = (IdFilter) schemaFilters.get(0);
      final SchemaFilter childFilter = idFilter.getChild();
      idValues[idFilter.getIndex()] = ((PreciseFilter) childFilter).getValue();
    }

    return !TreeViewSchema.isTreeViewTable(tableInstance)
        ? tryGetTableDeviceInCache(
            deviceEntryList,
            database,
            tableInstance,
            check,
            attributeColumns,
            fetchPaths,
            isDirectDeviceQuery,
            idValues,
            queryContext)
        : tryGetTreeDeviceInCache(deviceEntryList, tableInstance, check, fetchPaths, idValues);
  }

  private boolean tryGetTableDeviceInCache(
      final List<DeviceEntry> deviceEntryList,
      final String database,
      final TsTable tableInstance,
      final Predicate<AlignedDeviceEntry> check,
      final List<String> attributeColumns,
      final List<IDeviceID> fetchPaths,
      final boolean isDirectDeviceQuery,
      final String[] idValues,
      final MPPQueryContext queryContext) {
    final IDeviceID deviceID = convertIdValuesToDeviceID(tableInstance.getTableName(), idValues);
    final Map<String, Binary> attributeMap = cache.getDeviceAttribute(database, deviceID);

    // 1. AttributeMap == null means cache miss
    // 2. DeviceEntryList == null means that this is update statement, shall not get from cache and
    // shall reach the SchemaRegion to update
    if (Objects.isNull(attributeMap) || Objects.isNull(deviceEntryList) || Objects.isNull(check)) {
      if (Objects.nonNull(fetchPaths)) {
        fetchPaths.add(deviceID);
      }
      return false;
    }

    final AlignedDeviceEntry deviceEntry =
        new AlignedDeviceEntry(
            deviceID, attributeColumns.stream().map(attributeMap::get).toArray(Binary[]::new));
    // TODO table metadata: process cases that selected attr columns different from those used for
    // predicate
    if (check.test(deviceEntry)) {
      deviceEntryList.add(deviceEntry);
      // If we partially hit cache in direct device query, we must fetch for all the predicates
      // because now we do not support combining memory source and other sources
      if (isDirectDeviceQuery) {
        fetchPaths.add(deviceID);
      } else {
        queryContext.reserveMemoryForFrontEnd(deviceEntry.ramBytesUsed());
      }
    }
    return true;
  }

  private boolean tryGetTreeDeviceInCache(
      final List<DeviceEntry> deviceEntryList,
      final TsTable tableInstance,
      final Predicate<AlignedDeviceEntry> check,
      final List<IDeviceID> fetchPaths,
      final String[] idValues) {
    final IDeviceID deviceID =
        DataNodeTreeViewSchemaUtils.convertToIDeviceID(tableInstance, idValues);
    final IDeviceSchema schema = TableDeviceSchemaCache.getInstance().getDeviceSchema(deviceID);
    if (!(schema instanceof TreeDeviceNormalSchema) || Objects.isNull(check)) {
      if (Objects.nonNull(fetchPaths)) {
        fetchPaths.add(deviceID);
      }
      return false;
    }
    deviceEntryList.add(
        ((TreeDeviceNormalSchema) schema).isAligned()
            ? new AlignedDeviceEntry(deviceID, new Binary[0])
            : new NonAlignedDeviceEntry(deviceID, new Binary[0]));
    return true;
  }

  public static IDeviceID convertIdValuesToDeviceID(
      final String tableName, final String[] idValues) {
    // Convert to IDeviceID
    final String[] deviceIdNodes = new String[idValues.length + 1];
    deviceIdNodes[0] = tableName;
    System.arraycopy(idValues, 0, deviceIdNodes, 1, idValues.length);
    return IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdNodes);
  }

  private void fetchMissingDeviceSchemaForQuery(
      final TsTable tableInstance,
      final List<String> attributeColumns,
      final ShowDevice statement,
      final List<DeviceEntry> deviceEntryList,
      final MPPQueryContext mppQueryContext) {
    Throwable t = null;

    final long queryId = SessionManager.getInstance().requestQueryId();
    // For the correctness of attribute remote update
    Set<Long> queryIdSet = null;
    if (!TreeViewSchema.isTreeViewTable(tableInstance)
        && Objects.nonNull(statement.getPartitionKeyList())) {
      queryIdSet = attributeGuard.addFetchQueryId(queryId);
    }

    try {
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
              mppQueryContext.getTimeOut()
                  - (System.currentTimeMillis() - mppQueryContext.getStartTime()),
              false);

      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                executionResult.status.getMessage(), executionResult.status.getCode()));
      }

      final List<ColumnHeader> columnHeaderList =
          coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();

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
        if (!TreeViewSchema.isTreeViewTable(tableInstance)) {
          constructTableResults(
              tsBlock.get(),
              columnHeaderList,
              tableInstance,
              statement,
              mppQueryContext,
              attributeColumns,
              deviceEntryList);
        } else {
          constructTreeResults(
              tsBlock.get(), columnHeaderList, tableInstance, mppQueryContext, deviceEntryList);
        }
      }
    } catch (final Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      if (Objects.nonNull(queryIdSet)) {
        queryIdSet.remove(queryId);
        attributeGuard.tryUpdateCache();
      }
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
  }

  private void constructTableResults(
      final TsBlock tsBlock,
      final List<ColumnHeader> columnHeaderList,
      final TsTable tableInstance,
      final ShowDevice statement,
      final MPPQueryContext mppQueryContext,
      final List<String> attributeColumns,
      final List<DeviceEntry> deviceEntryList) {
    final Column[] columns = tsBlock.getValueColumns();
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      final String[] nodes = new String[tableInstance.getIdNums() + 1];
      final Map<String, Binary> attributeMap = new HashMap<>();
      constructNodsArrayAndAttributeMap(
          attributeMap,
          nodes,
          tableInstance.getTableName(),
          columnHeaderList,
          columns,
          tableInstance,
          i);
      final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(nodes);
      final AlignedDeviceEntry deviceEntry =
          new AlignedDeviceEntry(
              deviceID, attributeColumns.stream().map(attributeMap::get).toArray(Binary[]::new));
      mppQueryContext.reserveMemoryForFrontEnd(deviceEntry.ramBytesUsed());
      deviceEntryList.add(deviceEntry);
      // Only cache those exact device query
      // Fetch paths is null iff there are fuzzy queries related to id columns
      if (Objects.nonNull(statement.getPartitionKeyList())) {
        cache.putAttributes(statement.getDatabase(), deviceID, attributeMap);
      }
    }
  }

  private void constructTreeResults(
      final TsBlock tsBlock,
      final List<ColumnHeader> columnHeaderList,
      final TsTable tableInstance,
      final MPPQueryContext mppQueryContext,
      final List<DeviceEntry> deviceEntryList) {
    final Column[] columns = tsBlock.getValueColumns();
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      final String[] nodes = new String[tableInstance.getIdNums() + 1];
      constructNodsArrayAndAttributeMap(
          Collections.emptyMap(),
          nodes,
          tableInstance.getTableName(),
          columnHeaderList,
          columns,
          tableInstance,
          i);
      final IDeviceID deviceID =
          DataNodeTreeViewSchemaUtils.convertToIDeviceID(tableInstance, nodes);
      final DeviceEntry deviceEntry =
          columns[columns.length - 1].getBoolean(i)
              ? new AlignedDeviceEntry(deviceID, new Binary[0])
              : new NonAlignedDeviceEntry(deviceID, new Binary[0]);
      mppQueryContext.reserveMemoryForFrontEnd(deviceEntry.ramBytesUsed());
      deviceEntryList.add(deviceEntry);
    }
  }

  private void constructNodsArrayAndAttributeMap(
      final Map<String, Binary> attributeMap,
      final String[] nodes,
      final String tableName,
      final List<ColumnHeader> columnHeaderList,
      final Column[] columns,
      final TsTable tableInstance,
      final int rowIndex) {
    nodes[0] = tableName;
    int currentIndex = 1;
    for (int j = 0; j < columnHeaderList.size(); j++) {
      final TsTableColumnSchema columnSchema =
          tableInstance.getColumnSchema(columnHeaderList.get(j).getColumnName());
      // means that TsTable tableInstance which previously fetched is outdated, but it's ok that we
      // ignore that newly added column here
      if (columnSchema == null) {
        continue;
      }
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
        if (columns[j].isNull(rowIndex)) {
          nodes[currentIndex] = null;
        } else {
          nodes[currentIndex] =
              columns[j].getBinary(rowIndex).getStringValue(TSFileConfig.STRING_CHARSET);
        }
        currentIndex++;
      } else if (!columns[j].isNull(rowIndex)) {
        attributeMap.put(columnSchema.getColumnName(), columns[j].getBinary(rowIndex));
      }
    }
  }
}
