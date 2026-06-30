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

package org.apache.iotdb.db.pipe.sink.protocol.opcua;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.PathParseException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.parser.PathNodesGenerator;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;

class OpcUaInitialValueFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaInitialValueFetcher.class);

  private static final String LAST_QUERY_TIME_COLUMN = "Time";
  private static final String LAST_QUERY_TIMESERIES_COLUMN = "Timeseries";
  private static final String LAST_QUERY_VALUE_COLUMN = "Value";
  private static final String LAST_QUERY_DATA_TYPE_COLUMN = "DataType";
  private static final String DEFAULT_TREE_PATH = "root.**";

  private static final String TABLE_DATABASE_COLUMN = "Database";
  private static final String TABLE_NAME_COLUMN = "TableName";
  private static final String TABLE_COLUMN_NAME_COLUMN = "ColumnName";
  private static final String TABLE_DATA_TYPE_COLUMN = "DataType";
  private static final String TABLE_CATEGORY_COLUMN = "Category";
  private static final String TABLE_TIME_CATEGORY = "TIME";
  private static final String TABLE_TAG_CATEGORY = "TAG";
  private static final String TABLE_FIELD_CATEGORY = "FIELD";
  private static final String TABLE_VALUE_ALIAS_PREFIX = "__opc_value_";
  private static final String TABLE_TIME_ALIAS_PREFIX = "__opc_time_";
  private static final String TABLE_TAG_ALIAS_PREFIX = "__opc_tag_";

  private static final ExecutorService INITIAL_VALUE_FETCH_EXECUTOR =
      Executors.newSingleThreadExecutor(
          runnable -> {
            final Thread thread = new Thread(runnable, "opc-ua-initial-value-fetcher");
            thread.setDaemon(true);
            return thread;
          });

  private final PipeParameters parameters;
  private final String valueName;
  private final String qualityName;
  private final StatusCode defaultQuality;
  private final String placeHolder4NullTag;
  private final int fetchSize;
  private final int loadBatchSize;
  private final String sourceUser;
  private final String sourcePassword;
  private final boolean useEncryptedPassword;

  OpcUaInitialValueFetcher(
      final PipeParameters parameters,
      final String valueName,
      final String qualityName,
      final StatusCode defaultQuality,
      final String placeHolder4NullTag) {
    this.parameters = parameters;
    this.valueName = valueName;
    this.qualityName = qualityName;
    this.defaultQuality = defaultQuality;
    this.placeHolder4NullTag = placeHolder4NullTag;
    fetchSize =
        getPositiveIntOrDefault(
            parameters,
            Arrays.asList(
                PipeSinkConstant.CONNECTOR_OPC_UA_INITIAL_FETCH_FETCH_SIZE_KEY,
                PipeSinkConstant.SINK_OPC_UA_INITIAL_FETCH_FETCH_SIZE_KEY),
            PipeSinkConstant.CONNECTOR_OPC_UA_INITIAL_FETCH_FETCH_SIZE_DEFAULT_VALUE);
    loadBatchSize =
        getPositiveIntOrDefault(
            parameters,
            Arrays.asList(
                PipeSinkConstant.CONNECTOR_OPC_UA_INITIAL_FETCH_LOAD_BATCH_SIZE_KEY,
                PipeSinkConstant.SINK_OPC_UA_INITIAL_FETCH_LOAD_BATCH_SIZE_KEY),
            PipeSinkConstant.CONNECTOR_OPC_UA_INITIAL_FETCH_LOAD_BATCH_SIZE_DEFAULT_VALUE);
    sourceUser =
        parameters.getStringOrDefault(
            PipeSinkConstant.OPC_UA_INITIAL_FETCH_SOURCE_USER_KEY,
            CONNECTOR_IOTDB_USER_DEFAULT_VALUE);
    sourcePassword =
        parameters.getStringOrDefault(
            PipeSinkConstant.OPC_UA_INITIAL_FETCH_SOURCE_PASSWORD_KEY,
            CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);
    useEncryptedPassword =
        parameters.hasAttribute(PipeSinkConstant.OPC_UA_INITIAL_FETCH_SOURCE_PASSWORD_KEY)
            && parameters.getIntOrDefault(PipeSinkConstant.OPC_UA_INITIAL_FETCH_REGION_ID_KEY, -1)
                >= 0;
  }

  Future<?> fetchInBackground(final OpcUaNameSpace nameSpace) {
    return INITIAL_VALUE_FETCH_EXECUTOR.submit(
        () -> {
          try {
            fetch(nameSpace);
          } catch (final RuntimeException e) {
            LOGGER.warn("Failed to fetch OPC UA initial values in background.", e);
          }
        });
  }

  void fetch(final OpcUaNameSpace nameSpace) {
    if (Objects.isNull(nameSpace)) {
      return;
    }

    final long startTimeMillis = System.currentTimeMillis();
    long loadedValueCount = 0;
    long loadedQualityCount = 0;
    try {
      if (!DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters)
          .getLeft()) {
        LOGGER.info(
            "Skip OPC UA initial value fetch because source inclusion/exclusion does not include data.insert.");
        return;
      }

      final TreePattern treePattern = TreePattern.parsePipePatternFromSourceParameters(parameters);
      if (treePattern.isTreeModelDataAllowedToBeCaptured()) {
        final InitialLoadCounts counts = fetchInitialTreeValues(nameSpace, treePattern);
        loadedValueCount += counts.valueCount;
        loadedQualityCount += counts.qualityCount;
      }

      final TablePattern tablePattern =
          TablePattern.parsePipePatternFromSourceParameters(parameters);
      if (tablePattern.isTableModelDataAllowedToBeCaptured()) {
        final InitialLoadCounts counts = fetchInitialTableValues(nameSpace, tablePattern);
        loadedValueCount += counts.valueCount;
        loadedQualityCount += counts.qualityCount;
      }

      LOGGER.info(
          "Loaded {} OPC UA initial values and {} qualities in {} ms.",
          loadedValueCount,
          loadedQualityCount,
          System.currentTimeMillis() - startTimeMillis);
    } catch (final Exception e) {
      LOGGER.warn("Failed to fetch OPC UA initial values from local IoTDB.", e);
    }
  }

  private InitialLoadCounts fetchInitialTreeValues(
      final OpcUaNameSpace nameSpace, final TreePattern treePattern) throws Exception {
    final List<String> queryPaths = getTreeQueryPaths(treePattern);
    if (queryPaths.isEmpty()) {
      return new InitialLoadCounts();
    }

    try (final Session session = createSession(SystemConstant.SQL_DIALECT_TREE_VALUE)) {
      session.open(false);
      try (final SessionDataSet dataSet = session.executeLastDataQuery(queryPaths)) {
        return collectInitialTreeValues(dataSet, nameSpace, treePattern);
      }
    }
  }

  private List<String> getTreeQueryPaths(final TreePattern treePattern) {
    final List<String> basePaths;
    try {
      basePaths =
          treePattern.getBaseInclusionPaths().stream()
              .map(PartialPath::getFullPath)
              .filter(path -> !path.isEmpty())
              .collect(Collectors.toList());
    } catch (final UnsupportedOperationException e) {
      return Collections.singletonList(DEFAULT_TREE_PATH);
    }
    return basePaths.isEmpty() ? Collections.singletonList(DEFAULT_TREE_PATH) : basePaths;
  }

  private InitialLoadCounts collectInitialTreeValues(
      final SessionDataSet dataSet, final OpcUaNameSpace nameSpace, final TreePattern treePattern)
      throws Exception {
    final InitialLoadCounts counts = new InitialLoadCounts();
    final List<OpcUaNameSpace.InitialValueEntry> valueEntries =
        new ArrayList<>(Math.min(loadBatchSize, 1024));
    final SessionDataSet.DataIterator iterator = dataSet.iterator();
    if (Objects.isNull(valueName)) {
      while (iterator.next()) {
        final InitialQueryRow row = getInitialTreeQueryRow(iterator, treePattern);
        if (Objects.isNull(row)) {
          continue;
        }
        valueEntries.add(toInitialValueEntry(row));
        if (valueEntries.size() >= loadBatchSize) {
          counts.valueCount += loadInitialValues(nameSpace, valueEntries);
        }
      }
      counts.valueCount += loadInitialValues(nameSpace, valueEntries);
      return counts;
    }

    final List<String> valuePaths = new ArrayList<>(Math.min(loadBatchSize, 1024));
    final List<OpcUaNameSpace.InitialQualityEntry> qualityEntries =
        new ArrayList<>(Math.min(loadBatchSize, 1024));
    final Map<String, List<OpcUaNameSpace.InitialQualityEntry>> pendingQualityEntries =
        new HashMap<>();
    while (iterator.next()) {
      final InitialQueryRow row = getInitialTreeQueryRow(iterator, treePattern);
      if (Objects.isNull(row)) {
        continue;
      }

      if (row.isMeasurement(valueName)) {
        valueEntries.add(toInitialValueEntry(row));
        valuePaths.add(row.logicalPath);
        if (valueEntries.size() >= loadBatchSize) {
          counts.valueCount +=
              loadInitialValues(
                  nameSpace, valueEntries, valuePaths, pendingQualityEntries, qualityEntries);
        }
        continue;
      }

      if (row.isMeasurement(qualityName)) {
        final OpcUaNameSpace.InitialQualityEntry qualityEntry = toInitialQualityEntry(row);
        if (Objects.nonNull(qualityEntry)) {
          ++counts.qualityCount;
          if (nameSpace.hasInitialValueNode(row.logicalPath)) {
            qualityEntries.add(qualityEntry);
            loadInitialQualitiesIfFull(nameSpace, qualityEntries);
          } else {
            pendingQualityEntries
                .computeIfAbsent(row.logicalPath, ignored -> new ArrayList<>())
                .add(qualityEntry);
          }
        }
      }
    }

    counts.valueCount +=
        loadInitialValues(
            nameSpace, valueEntries, valuePaths, pendingQualityEntries, qualityEntries);
    loadRemainingPendingQualities(pendingQualityEntries, qualityEntries, nameSpace);
    loadInitialQualities(nameSpace, qualityEntries);
    return counts;
  }

  private InitialQueryRow getInitialTreeQueryRow(
      final SessionDataSet.DataIterator iterator, final TreePattern treePattern) throws Exception {
    final String timeseries = iterator.getString(LAST_QUERY_TIMESERIES_COLUMN);
    if (Objects.isNull(timeseries) || timeseries.isEmpty()) {
      return null;
    }
    final TSDataType dataType =
        parseDataType(iterator.getString(LAST_QUERY_DATA_TYPE_COLUMN), timeseries);
    if (Objects.isNull(dataType)) {
      return null;
    }
    final Object value = getTreeQueryValue(iterator, dataType);
    if (Objects.isNull(value)) {
      return null;
    }

    final String[] pathNodes;
    try {
      pathNodes = PathNodesGenerator.splitPathToNodes(timeseries);
    } catch (final PathParseException e) {
      LOGGER.warn("Skip initial value for malformed timeseries {}.", timeseries, e);
      return null;
    }
    if (pathNodes.length < 2) {
      return null;
    }

    final String measurement = pathNodes[pathNodes.length - 1];
    final IDeviceID device =
        IDeviceID.Factory.DEFAULT_FACTORY.create(Arrays.copyOf(pathNodes, pathNodes.length - 1));
    if (!treePattern.mayOverlapWithDevice(device)
        || !treePattern.matchesMeasurement(device, measurement)) {
      return null;
    }

    final long sourceUtcTime =
        OpcUaNameSpace.timestampToUtc(iterator.getLong(LAST_QUERY_TIME_COLUMN));
    if (Objects.isNull(valueName)) {
      return new InitialQueryRow(timeseries, timeseries, null, sourceUtcTime, dataType, value);
    }
    return new InitialQueryRow(
        timeseries,
        joinNodes(pathNodes, 0, pathNodes.length - 1, '.'),
        measurement,
        sourceUtcTime,
        dataType,
        value);
  }

  private Object getTreeQueryValue(
      final SessionDataSet.DataIterator iterator, final TSDataType dataType) throws Exception {
    if (iterator.isNull(LAST_QUERY_VALUE_COLUMN)) {
      return null;
    }
    final String valueLiteral = iterator.getString(LAST_QUERY_VALUE_COLUMN);
    if (Objects.isNull(valueLiteral)) {
      return null;
    }
    try {
      switch (dataType) {
        case BOOLEAN:
          return Boolean.parseBoolean(valueLiteral);
        case INT32:
          return Integer.parseInt(valueLiteral);
        case DATE:
          return new DateTime(java.sql.Date.valueOf(valueLiteral));
        case INT64:
          return Long.parseLong(valueLiteral);
        case TIMESTAMP:
          return new DateTime(OpcUaNameSpace.timestampToUtc(Long.parseLong(valueLiteral)));
        case FLOAT:
          return Float.parseFloat(valueLiteral);
        case DOUBLE:
          return Double.parseDouble(valueLiteral);
        case TEXT:
        case BLOB:
        case STRING:
          return valueLiteral;
        default:
          LOGGER.warn(
              "Skip initial value because data type {} is unsupported for OPC UA.", dataType);
          return null;
      }
    } catch (final IllegalArgumentException e) {
      LOGGER.warn(
          "Skip initial value because value {} cannot be parsed as {}.", valueLiteral, dataType, e);
      return null;
    }
  }

  private InitialLoadCounts fetchInitialTableValues(
      final OpcUaNameSpace nameSpace, final TablePattern tablePattern) throws Exception {
    final InitialLoadCounts counts = new InitialLoadCounts();
    final Pattern databasePattern = Pattern.compile(tablePattern.getDatabasePattern());
    final Pattern tableNamePattern = Pattern.compile(tablePattern.getTablePattern());
    try (final Session session = createSession(SystemConstant.SQL_DIALECT_TABLE_VALUE)) {
      session.open(false);
      final List<String> databases = listMatchingDatabases(session, databasePattern);
      for (final String database : databases) {
        session.executeNonQueryStatement("USE " + quoteIdentifier(database));
        final List<String> tables = listMatchingTables(session, database, tableNamePattern);
        for (final String table : tables) {
          final TableSchema tableSchema = describeTable(session, database, table);
          if (Objects.isNull(tableSchema.timeColumnName) || tableSchema.fieldColumns.isEmpty()) {
            continue;
          }
          final InitialLoadCounts tableCounts =
              fetchInitialTableValues(session, tableSchema, nameSpace);
          counts.valueCount += tableCounts.valueCount;
          counts.qualityCount += tableCounts.qualityCount;
        }
      }
    }
    return counts;
  }

  private List<String> listMatchingDatabases(final Session session, final Pattern databasePattern)
      throws Exception {
    final List<String> databases = new ArrayList<>();
    try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW DATABASES")) {
      final SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        final String database = iterator.getString(TABLE_DATABASE_COLUMN);
        if (Objects.nonNull(database) && databasePattern.matcher(database).matches()) {
          databases.add(database);
        }
      }
    }
    return databases;
  }

  private List<String> listMatchingTables(
      final Session session, final String database, final Pattern tablePattern) throws Exception {
    final List<String> tables = new ArrayList<>();
    try (final SessionDataSet dataSet =
        session.executeQueryStatement("SHOW TABLES FROM " + quoteIdentifier(database))) {
      final SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        final String table = iterator.getString(TABLE_NAME_COLUMN);
        if (Objects.nonNull(table) && tablePattern.matcher(table).matches()) {
          tables.add(table);
        }
      }
    }
    return tables;
  }

  private TableSchema describeTable(
      final Session session, final String database, final String table) throws Exception {
    final TableSchema tableSchema = new TableSchema(database, table);
    try (final SessionDataSet dataSet =
        session.executeQueryStatement("DESCRIBE " + quoteIdentifier(table))) {
      final SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        final String columnName = iterator.getString(TABLE_COLUMN_NAME_COLUMN);
        final String dataTypeLiteral = iterator.getString(TABLE_DATA_TYPE_COLUMN);
        final String categoryLiteral = iterator.getString(TABLE_CATEGORY_COLUMN);
        if (Objects.isNull(columnName)
            || Objects.isNull(dataTypeLiteral)
            || Objects.isNull(categoryLiteral)) {
          continue;
        }
        final String category = categoryLiteral.toUpperCase(Locale.ROOT);
        if (TABLE_TIME_CATEGORY.equals(category)) {
          tableSchema.timeColumnName = columnName;
        } else if (TABLE_TAG_CATEGORY.equals(category)) {
          tableSchema.tagColumnNames.add(columnName);
        } else if (TABLE_FIELD_CATEGORY.equals(category)) {
          final TSDataType dataType =
              parseDataType(dataTypeLiteral, database + "." + table + "." + columnName);
          if (Objects.nonNull(dataType) && isSupportedOpcDataType(dataType)) {
            tableSchema.fieldColumns.add(new TableColumn(columnName, dataType));
          }
        }
      }
    }
    return tableSchema;
  }

  private InitialLoadCounts fetchInitialTableValues(
      final Session session, final TableSchema tableSchema, final OpcUaNameSpace nameSpace)
      throws Exception {
    final InitialLoadCounts counts = new InitialLoadCounts();
    if (Objects.nonNull(valueName) && findTableFieldColumnIndex(tableSchema, valueName) < 0) {
      LOGGER.warn(
          "Skip table {}.{} because value field {} does not exist.",
          tableSchema.databaseName,
          tableSchema.tableName,
          valueName);
      return counts;
    }
    try (final SessionDataSet dataSet =
        session.executeQueryStatement(buildTableLastValueQuery(tableSchema))) {
      return collectInitialTableValues(dataSet, tableSchema, nameSpace);
    }
  }

  private InitialLoadCounts collectInitialTableValues(
      final SessionDataSet dataSet, final TableSchema tableSchema, final OpcUaNameSpace nameSpace)
      throws Exception {
    if (Objects.nonNull(valueName)) {
      return collectInitialTableValuesAndQualities(dataSet, tableSchema, nameSpace);
    }

    final InitialLoadCounts counts = new InitialLoadCounts();
    final List<OpcUaNameSpace.InitialValueEntry> valueEntries =
        new ArrayList<>(Math.min(loadBatchSize, 1024));
    final SessionDataSet.DataIterator iterator = dataSet.iterator();
    while (iterator.next()) {
      for (int i = 0; i < tableSchema.fieldColumns.size(); ++i) {
        final TableColumn fieldColumn = tableSchema.fieldColumns.get(i);
        if (addInitialTableValueEntry(
            iterator,
            tableSchema,
            fieldColumn,
            fieldColumn.name,
            getTableValueAlias(i),
            getTableTimeAlias(i),
            defaultQualityOrGood(),
            valueEntries)) {
          ++counts.valueCount;
          loadInitialValuesIfFull(nameSpace, valueEntries);
        }
      }
    }
    loadInitialValues(nameSpace, valueEntries);
    return counts;
  }

  private InitialLoadCounts collectInitialTableValuesAndQualities(
      final SessionDataSet dataSet, final TableSchema tableSchema, final OpcUaNameSpace nameSpace)
      throws Exception {
    final InitialLoadCounts counts = new InitialLoadCounts();
    final int valueColumnIndex = findTableFieldColumnIndex(tableSchema, valueName);
    if (valueColumnIndex < 0) {
      return counts;
    }
    final TableColumn valueColumn = tableSchema.fieldColumns.get(valueColumnIndex);
    final int qualityColumnIndex = findTableFieldColumnIndex(tableSchema, qualityName);
    final TableColumn qualityColumn =
        qualityColumnIndex < 0 ? null : tableSchema.fieldColumns.get(qualityColumnIndex);
    final List<OpcUaNameSpace.InitialValueEntry> valueEntries =
        new ArrayList<>(Math.min(loadBatchSize, 1024));
    final SessionDataSet.DataIterator iterator = dataSet.iterator();
    while (iterator.next()) {
      final StatusCode quality =
          getInitialTableQuality(iterator, qualityColumn, qualityColumnIndex);
      if (addInitialTableValueEntry(
          iterator,
          tableSchema,
          valueColumn,
          null,
          getTableValueAlias(valueColumnIndex),
          getTableTimeAlias(valueColumnIndex),
          Objects.nonNull(quality) ? quality : defaultQualityOrGood(),
          valueEntries)) {
        ++counts.valueCount;
        if (Objects.nonNull(quality)) {
          ++counts.qualityCount;
        }
        loadInitialValuesIfFull(nameSpace, valueEntries);
      }
    }
    loadInitialValues(nameSpace, valueEntries);
    return counts;
  }

  private boolean addInitialTableValueEntry(
      final SessionDataSet.DataIterator iterator,
      final TableSchema tableSchema,
      final TableColumn fieldColumn,
      final String fieldName,
      final String valueAlias,
      final String timeAlias,
      final StatusCode quality,
      final List<OpcUaNameSpace.InitialValueEntry> valueEntries)
      throws Exception {
    if (iterator.isNull(valueAlias) || iterator.isNull(timeAlias)) {
      return false;
    }

    final Object value = getTableQueryValue(iterator, valueAlias, fieldColumn.dataType);
    if (Objects.isNull(value)) {
      return false;
    }

    valueEntries.add(
        new OpcUaNameSpace.InitialValueEntry(
            getTableLogicalPathSegments(iterator, tableSchema, fieldName),
            fieldColumn.dataType,
            value,
            OpcUaNameSpace.timestampToUtc(iterator.getLong(timeAlias)),
            quality));
    return true;
  }

  private StatusCode getInitialTableQuality(
      final SessionDataSet.DataIterator iterator,
      final TableColumn qualityColumn,
      final int qualityColumnIndex)
      throws Exception {
    if (Objects.isNull(qualityColumn)
        || !TSDataType.BOOLEAN.equals(qualityColumn.dataType)
        || iterator.isNull(getTableValueAlias(qualityColumnIndex))) {
      return null;
    }
    return iterator.getBoolean(getTableValueAlias(qualityColumnIndex))
        ? StatusCode.GOOD
        : StatusCode.BAD;
  }

  private String buildTableLastValueQuery(final TableSchema tableSchema) {
    final List<String> selectItems = new ArrayList<>();
    for (int i = 0; i < tableSchema.tagColumnNames.size(); ++i) {
      selectItems.add(
          quoteIdentifier(tableSchema.tagColumnNames.get(i)) + " AS " + getTableTagAlias(i));
    }
    for (int i = 0; i < tableSchema.fieldColumns.size(); ++i) {
      final TableColumn fieldColumn = tableSchema.fieldColumns.get(i);
      selectItems.add(
          String.format(
              "last(%s) AS %s", quoteIdentifier(fieldColumn.name), getTableValueAlias(i)));
      selectItems.add(
          String.format(
              "last_by(%s, %s) AS %s",
              quoteIdentifier(tableSchema.timeColumnName),
              quoteIdentifier(fieldColumn.name),
              getTableTimeAlias(i)));
    }

    final StringBuilder sql =
        new StringBuilder("SELECT ")
            .append(String.join(", ", selectItems))
            .append(" FROM ")
            .append(quoteIdentifier(tableSchema.tableName))
            .append(" WHERE ")
            .append(
                tableSchema.fieldColumns.stream()
                    .map(fieldColumn -> quoteIdentifier(fieldColumn.name) + " IS NOT NULL")
                    .collect(Collectors.joining(" OR ")));
    if (!tableSchema.tagColumnNames.isEmpty()) {
      sql.append(" GROUP BY ")
          .append(
              tableSchema.tagColumnNames.stream()
                  .map(OpcUaInitialValueFetcher::quoteIdentifier)
                  .collect(Collectors.joining(", ")));
    }
    return sql.toString();
  }

  private int findTableFieldColumnIndex(final TableSchema tableSchema, final String columnName) {
    if (Objects.isNull(columnName)) {
      return -1;
    }
    for (int i = 0; i < tableSchema.fieldColumns.size(); ++i) {
      if (Objects.equals(tableSchema.fieldColumns.get(i).name, columnName)) {
        return i;
      }
    }
    return -1;
  }

  private String[] getTableLogicalPathSegments(
      final SessionDataSet.DataIterator iterator,
      final TableSchema tableSchema,
      final String fieldName)
      throws Exception {
    final List<String> pathSegments = new ArrayList<>(tableSchema.tagColumnNames.size() + 3);
    pathSegments.add(tableSchema.databaseName);
    pathSegments.add(tableSchema.tableName);
    for (int i = 0; i < tableSchema.tagColumnNames.size(); ++i) {
      final String tagValue = iterator.getString(getTableTagAlias(i));
      pathSegments.add(Objects.nonNull(tagValue) ? tagValue : placeHolder4NullTag);
    }
    if (Objects.nonNull(fieldName)) {
      pathSegments.add(fieldName);
    }
    return pathSegments.toArray(new String[0]);
  }

  private Object getTableQueryValue(
      final SessionDataSet.DataIterator iterator,
      final String columnName,
      final TSDataType dataType)
      throws Exception {
    if (iterator.isNull(columnName)) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        return iterator.getBoolean(columnName);
      case INT32:
        return iterator.getInt(columnName);
      case DATE:
        final LocalDate localDate = iterator.getDate(columnName);
        return Objects.isNull(localDate) ? null : new DateTime(java.sql.Date.valueOf(localDate));
      case INT64:
        return iterator.getLong(columnName);
      case TIMESTAMP:
        return new DateTime(OpcUaNameSpace.timestampToUtc(iterator.getLong(columnName)));
      case FLOAT:
        return iterator.getFloat(columnName);
      case DOUBLE:
        return iterator.getDouble(columnName);
      case TEXT:
      case BLOB:
      case STRING:
        return iterator.getString(columnName);
      default:
        LOGGER.warn(
            "Skip table initial value because data type {} is unsupported for OPC UA.", dataType);
        return null;
    }
  }

  private long loadInitialValues(
      final OpcUaNameSpace nameSpace, final List<OpcUaNameSpace.InitialValueEntry> valueEntries) {
    if (valueEntries.isEmpty()) {
      return 0;
    }

    nameSpace.loadInitialValues(valueEntries);
    final long loadedCount = valueEntries.size();
    valueEntries.clear();
    return loadedCount;
  }

  private long loadInitialValues(
      final OpcUaNameSpace nameSpace,
      final List<OpcUaNameSpace.InitialValueEntry> valueEntries,
      final List<String> valuePaths,
      final Map<String, List<OpcUaNameSpace.InitialQualityEntry>> pendingQualityEntries,
      final List<OpcUaNameSpace.InitialQualityEntry> qualityEntries) {
    if (valueEntries.isEmpty()) {
      return 0;
    }

    nameSpace.loadInitialValues(valueEntries);
    for (final String valuePath : valuePaths) {
      final List<OpcUaNameSpace.InitialQualityEntry> pendingEntries =
          pendingQualityEntries.remove(valuePath);
      if (Objects.isNull(pendingEntries)) {
        continue;
      }
      for (final OpcUaNameSpace.InitialQualityEntry pendingEntry : pendingEntries) {
        qualityEntries.add(pendingEntry);
        loadInitialQualitiesIfFull(nameSpace, qualityEntries);
      }
    }

    final long loadedCount = valueEntries.size();
    valueEntries.clear();
    valuePaths.clear();
    return loadedCount;
  }

  private void loadRemainingPendingQualities(
      final Map<String, List<OpcUaNameSpace.InitialQualityEntry>> pendingQualityEntries,
      final List<OpcUaNameSpace.InitialQualityEntry> qualityEntries,
      final OpcUaNameSpace nameSpace) {
    for (final List<OpcUaNameSpace.InitialQualityEntry> pendingEntries :
        pendingQualityEntries.values()) {
      for (final OpcUaNameSpace.InitialQualityEntry pendingEntry : pendingEntries) {
        qualityEntries.add(pendingEntry);
        loadInitialQualitiesIfFull(nameSpace, qualityEntries);
      }
    }
    pendingQualityEntries.clear();
  }

  private void loadInitialValuesIfFull(
      final OpcUaNameSpace nameSpace, final List<OpcUaNameSpace.InitialValueEntry> valueEntries) {
    if (valueEntries.size() >= loadBatchSize) {
      loadInitialValues(nameSpace, valueEntries);
    }
  }

  private void loadInitialQualitiesIfFull(
      final OpcUaNameSpace nameSpace,
      final List<OpcUaNameSpace.InitialQualityEntry> qualityEntries) {
    if (qualityEntries.size() >= loadBatchSize) {
      loadInitialQualities(nameSpace, qualityEntries);
    }
  }

  private void loadInitialQualities(
      final OpcUaNameSpace nameSpace,
      final List<OpcUaNameSpace.InitialQualityEntry> qualityEntries) {
    if (!qualityEntries.isEmpty()) {
      nameSpace.loadInitialQualities(qualityEntries);
      qualityEntries.clear();
    }
  }

  private OpcUaNameSpace.InitialValueEntry toInitialValueEntry(final InitialQueryRow row) {
    return new OpcUaNameSpace.InitialValueEntry(
        row.logicalPath, row.dataType, row.value, row.sourceUtcTime, defaultQualityOrGood());
  }

  private OpcUaNameSpace.InitialQualityEntry toInitialQualityEntry(final InitialQueryRow row) {
    if (!TSDataType.BOOLEAN.equals(row.dataType)) {
      LOGGER.warn(
          "Skip quality timeseries {} because data type {} is not BOOLEAN.",
          row.timeseries,
          row.dataType);
      return null;
    }

    return new OpcUaNameSpace.InitialQualityEntry(
        row.logicalPath, Boolean.TRUE.equals(row.value) ? StatusCode.GOOD : StatusCode.BAD);
  }

  private TSDataType parseDataType(final String dataTypeLiteral, final String path) {
    if (Objects.isNull(dataTypeLiteral) || dataTypeLiteral.isEmpty()) {
      LOGGER.warn("Skip initial value for {} because DataType is empty.", path);
      return null;
    }
    try {
      return TSDataType.valueOf(dataTypeLiteral.toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException e) {
      LOGGER.warn(
          "Skip initial value for {} because DataType {} is unsupported.", path, dataTypeLiteral);
      return null;
    }
  }

  private boolean isSupportedOpcDataType(final TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
      case INT32:
      case DATE:
      case INT64:
      case TIMESTAMP:
      case FLOAT:
      case DOUBLE:
      case TEXT:
      case BLOB:
      case STRING:
        return true;
      default:
        LOGGER.warn("Skip initial value because data type {} is unsupported for OPC UA.", dataType);
        return false;
    }
  }

  private StatusCode defaultQualityOrGood() {
    return Objects.nonNull(defaultQuality) ? defaultQuality : StatusCode.GOOD;
  }

  private Session createSession(final String sqlDialect) {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final Session.Builder builder =
        new Session.Builder()
            .host(config.getRpcAddress())
            .port(config.getRpcPort())
            .username(sourceUser)
            .password(sourcePassword)
            .useEncryptedPassword(useEncryptedPassword)
            .fetchSize(fetchSize);
    if (SystemConstant.SQL_DIALECT_TABLE_VALUE.equals(sqlDialect)) {
      builder.sqlDialect(SystemConstant.SQL_DIALECT_TABLE_VALUE);
    }
    return builder.build();
  }

  private static int getPositiveIntOrDefault(
      final PipeParameters parameters, final List<String> keys, final int defaultValue) {
    final int value = parameters.getIntOrDefault(keys, defaultValue);
    if (value > 0) {
      return value;
    }

    LOGGER.warn(
        "Ignore non-positive OPC UA initial fetch parameter {}={}, use default {}.",
        keys,
        value,
        defaultValue);
    return defaultValue;
  }

  private static String quoteIdentifier(final String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static String getTableTagAlias(final int index) {
    return TABLE_TAG_ALIAS_PREFIX + index;
  }

  private static String getTableValueAlias(final int index) {
    return TABLE_VALUE_ALIAS_PREFIX + index;
  }

  private static String getTableTimeAlias(final int index) {
    return TABLE_TIME_ALIAS_PREFIX + index;
  }

  private static String joinNodes(
      final String[] nodes,
      final int startInclusive,
      final int endExclusive,
      final char separator) {
    if (startInclusive >= endExclusive) {
      return "";
    }

    final StringBuilder builder = new StringBuilder(nodes[startInclusive]);
    for (int i = startInclusive + 1; i < endExclusive; ++i) {
      builder.append(separator).append(nodes[i]);
    }
    return builder.toString();
  }

  private static final class InitialLoadCounts {
    private long valueCount;
    private long qualityCount;
  }

  private static final class InitialQueryRow {
    private final String timeseries;
    private final String logicalPath;
    private final String measurement;
    private final long sourceUtcTime;
    private final TSDataType dataType;
    private final Object value;

    private InitialQueryRow(
        final String timeseries,
        final String logicalPath,
        final String measurement,
        final long sourceUtcTime,
        final TSDataType dataType,
        final Object value) {
      this.timeseries = timeseries;
      this.logicalPath = logicalPath;
      this.measurement = measurement;
      this.sourceUtcTime = sourceUtcTime;
      this.dataType = dataType;
      this.value = value;
    }

    private boolean isMeasurement(final String expectedMeasurement) {
      return Objects.equals(measurement, expectedMeasurement);
    }
  }

  private static final class TableSchema {
    private final String databaseName;
    private final String tableName;
    private final List<String> tagColumnNames = new ArrayList<>();
    private final List<TableColumn> fieldColumns = new ArrayList<>();
    private String timeColumnName;

    private TableSchema(final String databaseName, final String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }
  }

  private static final class TableColumn {
    private final String name;
    private final TSDataType dataType;

    private TableColumn(final String name, final TSDataType dataType) {
      this.name = name;
      this.dataType = dataType;
    }
  }
}
