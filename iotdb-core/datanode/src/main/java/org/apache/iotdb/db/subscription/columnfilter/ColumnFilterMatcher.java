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

package org.apache.iotdb.db.subscription.columnfilter;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.subscription.columnfilter.ColumnMetadata;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public class ColumnFilterMatcher {

  private static final ColumnFilterMatcher MATCH_ALL = new ColumnFilterMatcher(null, null);

  private final BoundColumnFilter boundColumnFilter;
  private final Set<String> selectedColumnNames;
  private final Expression expression;
  private final TablePattern tablePattern;
  private final BiFunction<String, String, TsTable> tableProvider;
  private final Boolean tableTimeSelected;
  private final ConcurrentMap<BoundColumnFilter.TableKey, CachedTableFilter> tableFilters =
      new ConcurrentHashMap<>();

  private ColumnFilterMatcher(final Set<String> selectedColumnNames, final Expression expression) {
    this(null, selectedColumnNames, expression);
  }

  private ColumnFilterMatcher(
      final BoundColumnFilter boundColumnFilter,
      final Set<String> selectedColumnNames,
      final Expression expression) {
    this(boundColumnFilter, selectedColumnNames, expression, null, null);
  }

  private ColumnFilterMatcher(
      final BoundColumnFilter boundColumnFilter,
      final Set<String> selectedColumnNames,
      final Expression expression,
      final TablePattern tablePattern,
      final BiFunction<String, String, TsTable> tableProvider) {
    this(boundColumnFilter, selectedColumnNames, expression, tablePattern, tableProvider, null);
  }

  private ColumnFilterMatcher(
      final BoundColumnFilter boundColumnFilter,
      final Set<String> selectedColumnNames,
      final Expression expression,
      final TablePattern tablePattern,
      final BiFunction<String, String, TsTable> tableProvider,
      final Boolean tableTimeSelected) {
    this.boundColumnFilter = boundColumnFilter;
    this.selectedColumnNames = selectedColumnNames;
    this.expression = expression;
    this.tablePattern = tablePattern;
    this.tableProvider = tableProvider;
    this.tableTimeSelected = tableTimeSelected;
  }

  /** A resolved table plan needs only column membership checks and its declared TIME selection. */
  static ColumnFilterMatcher forTableSelection(
      final Set<String> selectedColumnNames, final boolean timeSelected) {
    return new ColumnFilterMatcher(
        null, Collections.unmodifiableSet(selectedColumnNames), null, null, null, timeSelected);
  }

  public static ColumnFilterMatcher matchAll() {
    return MATCH_ALL;
  }

  public static ColumnFilterMatcher fromBoundColumnFilter(
      final BoundColumnFilter boundColumnFilter) {
    if (Objects.isNull(boundColumnFilter) || boundColumnFilter.isMatchAll()) {
      return matchAll();
    }
    return new ColumnFilterMatcher(boundColumnFilter, null, null);
  }

  public static ColumnFilterMatcher fromTopicConfig(final TopicConfig topicConfig)
      throws SubscriptionException {
    if (Objects.isNull(topicConfig)
        || !topicConfig.isTableTopic()
        || topicConfig.isColumnFilterTrivial()) {
      return matchAll();
    }
    return new ColumnFilterMatcher(
        null, new ColumnFilterParser().parseAndValidate(topicConfig.getColumnFilter()));
  }

  /**
   * Creates a streaming matcher. The expression is parsed once and table plans are reused until
   * their local schema is replaced, including when a previously unknown table becomes available.
   */
  public static ColumnFilterMatcher fromTopicConfig(
      final TopicConfig topicConfig, final BiFunction<String, String, TsTable> tableProvider)
      throws SubscriptionException {
    if (Objects.isNull(topicConfig)
        || !topicConfig.isTableTopic()
        || topicConfig.isColumnFilterTrivial()) {
      return matchAll();
    }
    return new ColumnFilterMatcher(
        null,
        null,
        new ColumnFilterParser().parseAndValidate(topicConfig.getColumnFilter()),
        new TablePattern(
            true,
            topicConfig.getStringOrDefault(
                TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE),
            topicConfig.getStringOrDefault(
                TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE)),
        tableProvider);
  }

  /** Resolves the schema once per Tablet, keeping expression evaluation out of the column loop. */
  public ColumnFilterMatcher forTable(final String databaseName, final String tableName) {
    if (Objects.isNull(tableProvider)) {
      return this;
    }
    final BoundColumnFilter.TableKey key = BoundColumnFilter.TableKey.of(databaseName, tableName);
    final CachedTableFilter cached = tableFilters.get(key);
    if (Objects.isNull(cached)
        && (!tablePattern.matchesDatabase(key.getDatabaseName())
            || !tablePattern.matchesTable(key.getTableName()))) {
      return ofSelectedColumnNames(Collections.emptySet());
    }
    final TsTable table = tableProvider.apply(key.getDatabaseName(), key.getTableName());
    if (Objects.isNull(table)) {
      tableFilters.remove(key);
      return ofSelectedColumnNames(Collections.emptySet());
    }
    // DataNodeTableCache replaces entire TsTable instances on schema changes. Comparing identity
    // avoids copying schemas or evaluating predicates for every Tablet (and every row).
    if (Objects.nonNull(cached) && cached.table == table) {
      return cached.matcher;
    }
    return tableFilters.compute(
            key,
            (ignored, previous) ->
                Objects.nonNull(previous) && previous.table == table
                    ? previous
                    : new CachedTableFilter(
                        table,
                        ColumnFilterBinder.bindTableFilter(
                            expression, key.getDatabaseName(), table)))
        .matcher;
  }

  public static ColumnFilterMatcher ofSelectedColumnNames(final Set<String> selectedColumnNames) {
    if (Objects.isNull(selectedColumnNames)) {
      return matchAll();
    }

    final Set<String> normalizedColumnNames = new HashSet<>();
    selectedColumnNames.stream()
        .filter(Objects::nonNull)
        .map(ColumnFilterMatcher::normalize)
        .forEach(normalizedColumnNames::add);
    return new ColumnFilterMatcher(Collections.unmodifiableSet(normalizedColumnNames), null);
  }

  public boolean isMatchAll() {
    return Objects.isNull(boundColumnFilter)
        && Objects.isNull(selectedColumnNames)
        && Objects.isNull(expression);
  }

  public boolean isTimeSelected() {
    if (Objects.nonNull(tableTimeSelected)) {
      return tableTimeSelected;
    }
    if (Objects.nonNull(tableProvider)) {
      return tableFilters.values().stream().anyMatch(cached -> cached.matcher.isTimeSelected());
    }
    if (isMatchAll()) {
      return true;
    }
    if (Objects.nonNull(boundColumnFilter)) {
      return boundColumnFilter.isTimeSelected();
    }
    if (Objects.nonNull(selectedColumnNames)) {
      return selectedColumnNames.contains("time");
    }
    return isTimeSelected("", "");
  }

  public boolean isTimeSelected(final String databaseName, final String tableName) {
    if (Objects.nonNull(tableTimeSelected)) {
      return tableTimeSelected;
    }
    if (Objects.nonNull(tableProvider)) {
      return forTable(databaseName, tableName).isTimeSelected(databaseName, tableName);
    }
    if (isMatchAll()) {
      return true;
    }
    if (Objects.nonNull(boundColumnFilter)) {
      return boundColumnFilter.isTimeSelected(databaseName, tableName);
    }
    if (Objects.nonNull(selectedColumnNames)) {
      return selectedColumnNames.contains("time");
    }
    return ColumnFilterEvaluator.evaluate(
        expression,
        new ColumnMetadata(
            normalizeNullable(databaseName),
            normalizeNullable(tableName),
            "time",
            TSDataType.TIMESTAMP.name(),
            ColumnCategory.TIME.name()));
  }

  public Map<String, Map<String, Boolean>> getTimeSelectedByTable(final String databaseName) {
    if (Objects.nonNull(tableProvider)) {
      final Map<String, Map<String, Boolean>> result = new HashMap<>();
      tableFilters
          .keySet()
          .forEach(
              key -> {
                if (Objects.isNull(databaseName)
                    || key.getDatabaseName().equals(normalize(databaseName))) {
                  result
                      .computeIfAbsent(key.getDatabaseName(), ignored -> new HashMap<>())
                      .put(
                          key.getTableName(),
                          isTimeSelected(key.getDatabaseName(), key.getTableName()));
                }
              });
      return result;
    }
    if (Objects.isNull(boundColumnFilter) || boundColumnFilter.getTimeSelectedByTable().isEmpty()) {
      return Collections.emptyMap();
    }

    final String normalizedDatabaseName =
        Objects.nonNull(databaseName) ? normalize(databaseName) : null;
    final Map<String, Map<String, Boolean>> result = new HashMap<>();
    boundColumnFilter
        .getTimeSelectedByTable()
        .forEach(
            (tableKey, timeSelected) -> {
              if (Objects.nonNull(normalizedDatabaseName)
                  && !Objects.equals(normalizedDatabaseName, tableKey.getDatabaseName())) {
                return;
              }
              result
                  .computeIfAbsent(tableKey.getDatabaseName(), ignored -> new HashMap<>())
                  .put(tableKey.getTableName(), timeSelected);
            });
    if (result.isEmpty()) {
      return Collections.emptyMap();
    }

    result.replaceAll((database, tableMap) -> Collections.unmodifiableMap(tableMap));
    return Collections.unmodifiableMap(result);
  }

  public boolean match(final String databaseName, final String tableName, final String columnName) {
    return match(databaseName, tableName, columnName, null, null);
  }

  public boolean match(
      final String databaseName,
      final String tableName,
      final String columnName,
      final TSDataType dataType,
      final ColumnCategory category) {
    if (Objects.nonNull(tableProvider)) {
      return forTable(databaseName, tableName)
          .match(databaseName, tableName, columnName, dataType, category);
    }
    if (isMatchAll()) {
      return true;
    }
    if (Objects.nonNull(boundColumnFilter)) {
      return boundColumnFilter.match(databaseName, tableName, columnName);
    }
    if (Objects.nonNull(selectedColumnNames)) {
      return selectedColumnNames.contains(normalize(columnName));
    }
    return ColumnFilterEvaluator.evaluate(
        expression,
        new ColumnMetadata(
            normalizeNullable(databaseName),
            normalizeNullable(tableName),
            normalizeNullable(columnName),
            Objects.nonNull(dataType) ? dataType.name() : "",
            Objects.nonNull(category) ? category.name() : ""));
  }

  private static String normalize(final String value) {
    return value.trim().toLowerCase(Locale.ROOT);
  }

  private static String normalizeNullable(final String value) {
    return Objects.nonNull(value) ? value.trim() : "";
  }

  private static final class CachedTableFilter {
    private final TsTable table;
    private final ColumnFilterMatcher matcher;

    private CachedTableFilter(final TsTable table, final ColumnFilterMatcher matcher) {
      this.table = table;
      this.matcher = matcher;
    }
  }
}
