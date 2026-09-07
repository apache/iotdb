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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.subscription.columnfilter.ColumnMetadata;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
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

public class ColumnFilterMatcher {

  private static final ColumnFilterMatcher MATCH_ALL = new ColumnFilterMatcher(null, null);

  private final BoundColumnFilter boundColumnFilter;
  private final Set<String> selectedColumnNames;
  private final Expression expression;

  private ColumnFilterMatcher(final Set<String> selectedColumnNames, final Expression expression) {
    this(null, selectedColumnNames, expression);
  }

  private ColumnFilterMatcher(
      final BoundColumnFilter boundColumnFilter,
      final Set<String> selectedColumnNames,
      final Expression expression) {
    this.boundColumnFilter = boundColumnFilter;
    this.selectedColumnNames = selectedColumnNames;
    this.expression = expression;
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

  public boolean shouldAutoRetainTagsAtRuntime() {
    return Objects.isNull(boundColumnFilter);
  }

  public boolean isTimeSelected() {
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
}
