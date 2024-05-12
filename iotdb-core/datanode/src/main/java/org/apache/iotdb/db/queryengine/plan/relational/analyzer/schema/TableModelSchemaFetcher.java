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
import org.apache.iotdb.commons.schema.filter.impl.AndFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceFilterUtil;
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
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertSchemaPredicateToFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache.TableDeviceId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.table.CreateTableDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.table.FetchTableDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.table.ShowTableDevicesStatement;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class TableModelSchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();

  private final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

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
    ValidateResult validateResult = validateDeviceSchemaInCache(schemaValidation);

    if (!validateResult.missingDeviceIndexList.isEmpty()
        || !validateResult.attributeMissingInCacheDeviceIndexList.isEmpty()) {
      validateResult = fetchAndValidateDeviceSchema(schemaValidation, validateResult, context);
    }

    if (!validateResult.missingDeviceIndexList.isEmpty()
        || !validateResult.attributeUpdateDeviceIndexList.isEmpty()) {
      autoCreateDeviceSchema(schemaValidation, validateResult, context);
    }
  }

  private ValidateResult validateDeviceSchemaInCache(
      ITableDeviceSchemaValidation schemaValidation) {
    ValidateResult result = new ValidateResult();
    String database = schemaValidation.getDatabase();
    String tableName = schemaValidation.getTableName();
    List<String[]> deviceIdList = schemaValidation.getDeviceIdList();
    List<String> attributeKeyList = schemaValidation.getAttributeColumnNameList();
    List<List<String>> attributeValueList = schemaValidation.getAttributeValue();

    for (int i = 0, size = deviceIdList.size(); i < size; i++) {
      Map<String, String> attributeMap =
          cache.getDeviceAttribute(database, tableName, deviceIdList.get(i));
      if (attributeMap == null) {
        result.missingDeviceIndexList.add(i);
        continue;
      }
      List<String> deviceAttributeValueList = attributeValueList.get(i);
      for (int j = 0; j < attributeKeyList.size(); j++) {
        String value = attributeMap.get(attributeKeyList.get(j));
        if (value == null) {
          result.attributeMissingInCacheDeviceIndexList.add(i);
          break;
        } else if (!value.equals(deviceAttributeValueList.get(j))) {
          result.attributeUpdateDeviceIndexList.add(i);
          break;
        }
      }
    }
    return result;
  }

  private ValidateResult fetchAndValidateDeviceSchema(
      ITableDeviceSchemaValidation schemaValidation,
      ValidateResult previousValidateResult,
      MPPQueryContext context) {
    List<String[]> targetDeviceList =
        new ArrayList<>(
            previousValidateResult.missingDeviceIndexList.size()
                + previousValidateResult.attributeMissingInCacheDeviceIndexList.size());
    for (int index : previousValidateResult.missingDeviceIndexList) {
      targetDeviceList.add(schemaValidation.getDeviceIdList().get(index));
    }
    for (int index : previousValidateResult.attributeMissingInCacheDeviceIndexList) {
      targetDeviceList.add(schemaValidation.getDeviceIdList().get(index));
    }

    Map<TableDeviceId, Map<String, String>> fetchedDeviceSchema =
        fetchMissingDeviceSchema(
            new FetchTableDevicesStatement(
                schemaValidation.getDatabase(), schemaValidation.getTableName(), targetDeviceList),
            context);

    for (Map.Entry<TableDeviceId, Map<String, String>> entry : fetchedDeviceSchema.entrySet()) {
      cache.put(
          schemaValidation.getDatabase(),
          schemaValidation.getTableName(),
          entry.getKey().getIdValues(),
          entry.getValue());
    }

    ValidateResult result = new ValidateResult();
    for (int index : previousValidateResult.missingDeviceIndexList) {
      String[] deviceId = schemaValidation.getDeviceIdList().get(index);
      Map<String, String> attributeMap = fetchedDeviceSchema.get(new TableDeviceId(deviceId));
      if (attributeMap == null) {
        result.missingDeviceIndexList.add(index);
      } else {
        for (int j = 0; j < schemaValidation.getAttributeColumnNameList().size(); j++) {
          String key = schemaValidation.getAttributeColumnNameList().get(j);
          String value = attributeMap.get(key);
          if (value == null
              || !value.equals(schemaValidation.getAttributeValue().get(index).get(j))) {
            result.attributeUpdateDeviceIndexList.add(index);
            break;
          }
        }
      }
    }

    for (int index : previousValidateResult.attributeMissingInCacheDeviceIndexList) {
      String[] deviceId = schemaValidation.getDeviceIdList().get(index);
      Map<String, String> attributeMap = fetchedDeviceSchema.get(new TableDeviceId(deviceId));
      if (attributeMap == null) {
        throw new IllegalStateException("Device shall exist but not exist.");
      } else {
        for (int j = 0; j < schemaValidation.getAttributeColumnNameList().size(); j++) {
          String key = schemaValidation.getAttributeColumnNameList().get(j);
          String value = attributeMap.get(key);
          if (value == null
              || !value.equals(schemaValidation.getAttributeValue().get(index).get(j))) {
            result.attributeUpdateDeviceIndexList.add(index);
            break;
          }
        }
      }
    }

    result.attributeUpdateDeviceIndexList.addAll(
        previousValidateResult.attributeUpdateDeviceIndexList);

    return result;
  }

  private Map<TableDeviceId, Map<String, String>> fetchMissingDeviceSchema(
      FetchTableDevicesStatement statement, MPPQueryContext context) {
    long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    String database = statement.getDatabase();
    String table = statement.getTableName();
    TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);

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

  private void autoCreateDeviceSchema(
      ITableDeviceSchemaValidation schemaValidation,
      ValidateResult previousValidateResult,
      MPPQueryContext context) {
    List<String[]> deviceIdList =
        new ArrayList<>(
            previousValidateResult.missingDeviceIndexList.size()
                + previousValidateResult.attributeUpdateDeviceIndexList.size());
    List<List<String>> attributeValueList = new ArrayList<>(deviceIdList.size());
    for (int index : previousValidateResult.missingDeviceIndexList) {
      deviceIdList.add(schemaValidation.getDeviceIdList().get(index));
      attributeValueList.add(schemaValidation.getAttributeValue().get(index));
    }
    for (int index : previousValidateResult.attributeUpdateDeviceIndexList) {
      deviceIdList.add(schemaValidation.getDeviceIdList().get(index));
      attributeValueList.add(schemaValidation.getAttributeValue().get(index));
    }

    CreateTableDeviceStatement statement =
        new CreateTableDeviceStatement(
            schemaValidation.getDatabase(),
            schemaValidation.getTableName(),
            deviceIdList,
            schemaValidation.getAttributeColumnNameList(),
            attributeValueList);
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

  private static class ValidateResult {
    final List<Integer> missingDeviceIndexList = new ArrayList<>();
    final List<Integer> attributeMissingInCacheDeviceIndexList = new ArrayList<>();
    final List<Integer> attributeUpdateDeviceIndexList = new ArrayList<>();
  }

  public List<DeviceEntry> fetchDeviceSchema(
      String database,
      String table,
      List<Expression> expressionList,
      List<String> attributeColumns) {
    List<DeviceEntry> deviceEntryList = new ArrayList<>();

    TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);
    Pair<List<SchemaFilter>, List<SchemaFilter>> filters =
        transformExpression(expressionList, tableInstance);
    List<SchemaFilter> idFilters = filters.getLeft();
    List<SchemaFilter> attributeFilters = filters.getRight();
    DeviceInCacheFilterVisitor filterVisitor = new DeviceInCacheFilterVisitor(attributeColumns);
    SchemaFilter attributeFilter = getAttributeFilter(attributeFilters);

    List<List<SchemaFilter>> idPatternList =
        DeviceFilterUtil.convertSchemaFilterToOrConcatList(idFilters);
    List<List<SchemaFilter>> idFilterListForFetch = new ArrayList<>();
    boolean cacheFetchedDevice = true;
    for (int i = 0; i < idPatternList.size(); i++) {
      SchemaFilterCheckResult checkResult =
          checkIdFilterAndTryGetDeviceInCache(
              deviceEntryList,
              database,
              tableInstance,
              idPatternList.get(i),
              o -> attributeFilter == null || filterVisitor.process(attributeFilter, o),
              attributeColumns);
      if (checkResult.needFetch) {
        idFilterListForFetch.add(idPatternList.get(i));
        if (!checkResult.isIdDetermined) {
          cacheFetchedDevice = false;
        }
      }
    }

    if (!idFilterListForFetch.isEmpty()) {
      fetchMissingDeviceSchemaForQuery(
          database,
          tableInstance,
          attributeColumns,
          idFilterListForFetch,
          attributeFilter,
          deviceEntryList,
          cacheFetchedDevice);
    }

    return deviceEntryList;
  }

  private Pair<List<SchemaFilter>, List<SchemaFilter>> transformExpression(
      List<Expression> expressionList, TsTable table) {
    List<SchemaFilter> idDeterminedFilters = new ArrayList<>();
    List<SchemaFilter> idFuzzyFilters = new ArrayList<>();
    ConvertSchemaPredicateToFilterVisitor visitor = new ConvertSchemaPredicateToFilterVisitor();
    ConvertSchemaPredicateToFilterVisitor.Context context =
        new ConvertSchemaPredicateToFilterVisitor.Context(table);
    for (Expression expression : expressionList) {
      if (expression == null) {
        continue;
      }
      context.reset();
      SchemaFilter schemaFilter = expression.accept(visitor, context);
      if (context.hasAttribute()) {
        idFuzzyFilters.add(schemaFilter);
      } else {
        idDeterminedFilters.add(schemaFilter);
      }
    }
    return new Pair<>(idDeterminedFilters, idFuzzyFilters);
  }

  // return whether this condition shall be used for remote fetch
  private SchemaFilterCheckResult checkIdFilterAndTryGetDeviceInCache(
      List<DeviceEntry> deviceEntryList,
      String database,
      TsTable tableInstance,
      List<SchemaFilter> idFilters,
      Predicate<DeviceEntry> check,
      List<String> attributeColumns) {
    String[] idValues = new String[tableInstance.getIdNums()];
    for (SchemaFilter schemaFilter : idFilters) {
      DeviceIdFilter idFilter = (DeviceIdFilter) schemaFilter;
      if (idValues[idFilter.getIndex()] == null) {
        idValues[idFilter.getIndex()] = idFilter.getValue();
      } else {
        // conflict filter
        return new SchemaFilterCheckResult(false, false);
      }
    }
    if (idFilters.size() < idValues.length) {
      return new SchemaFilterCheckResult(true, false);
    }
    Map<String, String> attributeMap =
        cache.getDeviceAttribute(database, tableInstance.getTableName(), idValues);
    if (attributeMap == null) {
      return new SchemaFilterCheckResult(true, true);
    }
    List<String> attributeValues = new ArrayList<>(attributeColumns.size());
    for (String attributeKey : attributeColumns) {
      String value = attributeMap.get(attributeKey);
      if (value == null) {
        return new SchemaFilterCheckResult(true, true);
      } else {
        attributeValues.add(value);
      }
    }
    String[] deviceIdNodes = new String[idValues.length + 1];
    deviceIdNodes[0] = database + PATH_SEPARATOR + tableInstance.getTableName();
    System.arraycopy(idValues, 0, deviceIdNodes, 1, idValues.length);
    DeviceEntry deviceEntry =
        new DeviceEntry(new StringArrayDeviceID(deviceIdNodes), attributeValues);
    if (check.test(deviceEntry)) {
      deviceEntryList.add(deviceEntry);
    }
    return new SchemaFilterCheckResult(false, true);
  }

  private static class SchemaFilterCheckResult {
    boolean needFetch;
    boolean isIdDetermined;

    SchemaFilterCheckResult(boolean needFetch, boolean isIdDetermined) {
      this.needFetch = needFetch;
      this.isIdDetermined = isIdDetermined;
    }
  }

  private void fetchMissingDeviceSchemaForQuery(
      String database,
      TsTable tableInstance,
      List<String> attributeColumns,
      List<List<SchemaFilter>> idPatternList,
      SchemaFilter attributeFilter,
      List<DeviceEntry> deviceEntryList,
      boolean cacheFetchedDevice) {

    String table = tableInstance.getTableName();

    long queryId = SessionManager.getInstance().requestQueryId();
    ShowTableDevicesStatement statement =
        new ShowTableDevicesStatement(database, table, idPatternList, attributeFilter);
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
          nodes[0] = database + PATH_SEPARATOR + table;
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
          IDeviceID deviceID = new StringArrayDeviceID(nodes);
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

  private SchemaFilter getAttributeFilter(List<SchemaFilter> filterList) {
    if (filterList.isEmpty()) {
      return null;
    }
    AndFilter andFilter;
    SchemaFilter latestFilter = filterList.get(0);
    for (int i = 1; i < filterList.size(); i++) {
      andFilter = new AndFilter(latestFilter, filterList.get(i));
      latestFilter = andFilter;
    }
    return latestFilter;
  }
}
