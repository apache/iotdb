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

package org.apache.iotdb.db.subscription.tagfilter;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable parsed tag-filter cached per topic. */
public class TagFilterMatcher {

  private static final TagFilterMatcher MATCH_NONE =
      new TagFilterMatcher(
          null, Collections.emptyList(), Collections.emptyMap(), State.MATCH_NONE, null, false);

  private final Expression expression;
  private final List<Identifier> referencedFields;
  private final Map<Expression, Pattern> compiledPatterns;
  private final State state;
  private final Throwable failure;
  private final Map<TableKey, TableBinding> tableBindings;
  private final boolean bindingEnforced;

  private enum State {
    MATCH_ALL,
    MATCH_NONE,
    ACTIVE,
    FAILURE
  }

  private TagFilterMatcher(
      final Expression expression,
      final List<Identifier> referencedFields,
      final Map<Expression, Pattern> compiledPatterns,
      final State state,
      final Throwable failure,
      final boolean bindingEnforced) {
    this(
        expression,
        referencedFields,
        compiledPatterns,
        state,
        failure,
        Collections.emptyMap(),
        bindingEnforced);
  }

  private TagFilterMatcher(
      final Expression expression,
      final List<Identifier> referencedFields,
      final Map<Expression, Pattern> compiledPatterns,
      final State state,
      final Throwable failure,
      final Map<TableKey, TableBinding> tableBindings,
      final boolean bindingEnforced) {
    this.expression = expression;
    this.referencedFields = referencedFields;
    this.compiledPatterns = compiledPatterns;
    this.state = state;
    this.failure = failure;
    this.tableBindings = tableBindings;
    this.bindingEnforced = bindingEnforced;
  }

  private static final TagFilterMatcher MATCH_ALL =
      new TagFilterMatcher(
          null, Collections.emptyList(), Collections.emptyMap(), State.MATCH_ALL, null, false);

  public static TagFilterMatcher matchAll() {
    return MATCH_ALL;
  }

  public static TagFilterMatcher matchNone() {
    return MATCH_NONE;
  }

  public static TagFilterMatcher failure(final Throwable failure) {
    return new TagFilterMatcher(
        null,
        Collections.emptyList(),
        Collections.emptyMap(),
        State.FAILURE,
        Objects.nonNull(failure) ? failure : new IllegalStateException(),
        false);
  }

  public static TagFilterMatcher fromTopicConfig(final TopicConfig topicConfig)
      throws SubscriptionException {
    if (Objects.isNull(topicConfig)
        || !topicConfig.isTableTopic()
        || topicConfig.isTagFilterTrivial()) {
      return matchAll();
    }

    final Expression expression;
    final TagFilterValidator.ValidationResult validation;
    try {
      expression = new TagFilterParser().parse(topicConfig.getTagFilter());
      validation = TagFilterValidator.validateAndCompile(expression);
    } catch (final IllegalArgumentException e) {
      throw new SubscriptionException(
          String.format(
              DataNodeMiscMessages.EXCEPTION_INVALID_TAG_FILTER_ARG_E4B1C1C6, e.getMessage()),
          e);
    }
    return new TagFilterMatcher(
        expression,
        validation.getReferencedFields(),
        validation.getCompiledPatterns(),
        State.ACTIVE,
        null,
        false);
  }

  /**
   * Parses a filter and validates every currently visible table selected by the topic pattern. An
   * empty table map is deliberately allowed for a wildcard topic because the table may be created
   * after the topic; a null map means that a required concrete schema is unavailable.
   */
  public static TagFilterMatcher fromTopicConfig(
      final TopicConfig topicConfig, final Map<String, Map<String, TsTable>> tables)
      throws SubscriptionException {
    final TagFilterMatcher matcher = fromTopicConfig(topicConfig);
    if (matcher.isMatchAll()) {
      return matcher;
    }
    if (tables == null) {
      throw new SubscriptionException(
          DataNodeMiscMessages.EXCEPTION_TABLE_SCHEMA_IS_NOT_AVAILABLE_FOR_TAG_FILTER_993AB728);
    }

    final TablePattern tablePattern =
        new TablePattern(
            true,
            topicConfig.getStringOrDefault(
                TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE),
            topicConfig.getStringOrDefault(
                TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE));
    final Map<TableKey, TableBinding> bindings = new HashMap<>();
    for (final Map.Entry<String, Map<String, TsTable>> databaseEntry : tables.entrySet()) {
      if (!tablePattern.matchesDatabase(databaseEntry.getKey())) {
        continue;
      }
      for (final TsTable table : databaseEntry.getValue().values()) {
        if (table == null || !tablePattern.matchesTable(table.getTableName())) {
          continue;
        }
        final TableBinding binding = bindTable(matcher.referencedFields, table);
        if (binding.isFailed()) {
          throw new SubscriptionException(binding.getFailureReason());
        }
        bindings.put(TableKey.of(databaseEntry.getKey(), table.getTableName()), binding);
      }
    }
    final boolean bindingEnforced =
        isLiteralTopicPattern(
                topicConfig.getStringOrDefault(
                    TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE))
            && isLiteralTopicPattern(
                topicConfig.getStringOrDefault(
                    TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE));
    return matcher.withBindings(bindings, bindingEnforced);
  }

  private static boolean isLiteralTopicPattern(final String pattern) {
    final String regexMetaCharacters = ".*+?[](){}\\|^$";
    return Objects.nonNull(pattern)
        && pattern.chars().noneMatch(c -> regexMetaCharacters.indexOf((char) c) >= 0);
  }

  private static TableBinding bindTable(
      final List<Identifier> referencedFields, final TsTable table) {
    final java.util.Set<String> tagNames = new java.util.HashSet<>();
    final java.util.Set<String> allNames = new java.util.HashSet<>();
    for (final TsTableColumnSchema schema : table.getColumnList()) {
      if (schema == null || schema.getColumnName() == null) {
        continue;
      }
      allNames.add(schema.getColumnName().toLowerCase(java.util.Locale.ROOT));
      if (schema.getColumnCategory() == TsTableColumnCategory.TAG) {
        tagNames.add(schema.getColumnName().toLowerCase(java.util.Locale.ROOT));
      }
    }
    for (final Identifier identifier : referencedFields) {
      final String identifierName = identifier.getValue();
      TsTableColumnSchema matched = null;
      for (final TsTableColumnSchema schema : table.getColumnList()) {
        if (schema == null || schema.getColumnName() == null) {
          continue;
        }
        if (identifier.isDelimited()
            ? schema.getColumnName().equals(identifierName)
            : schema.getColumnName().equalsIgnoreCase(identifierName)) {
          matched = schema;
          break;
        }
      }
      if (matched == null) {
        return new TableBinding(
            true,
            String.format(
                DataNodeMiscMessages.EXCEPTION_REFERENCED_TAG_COLUMN_IS_MISSING_ARG_F5300BEA,
                identifierName),
            tagNames,
            allNames);
      }
      if (matched.getColumnCategory() != TsTableColumnCategory.TAG) {
        return new TableBinding(
            true,
            String.format(
                DataNodeMiscMessages.EXCEPTION_REFERENCED_COLUMN_IS_NOT_A_TAG_COLUMN_ARG_34D60881,
                identifierName),
            tagNames,
            allNames);
      }
    }
    return new TableBinding(false, null, tagNames, allNames);
  }

  TagFilterMatcher withBindings(final Map<TableKey, TableBinding> bindings) {
    return withBindings(bindings, true);
  }

  TagFilterMatcher withBindings(
      final Map<TableKey, TableBinding> bindings, final boolean bindingEnforced) {
    return new TagFilterMatcher(
        expression,
        referencedFields,
        compiledPatterns,
        state,
        failure,
        Objects.nonNull(bindings)
            ? Collections.unmodifiableMap(new HashMap<>(bindings))
            : Collections.emptyMap(),
        bindingEnforced);
  }

  public boolean isMatchAll() {
    return state == State.MATCH_ALL;
  }

  public boolean isMatchNone() {
    return state == State.MATCH_NONE;
  }

  public boolean isFailure() {
    return state == State.FAILURE;
  }

  public void throwIfFailure() {
    if (isFailure()) {
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_MATCHER_IS_UNAVAILABLE_1A659D47, failure);
    }
  }

  List<Identifier> getReferencedFields() {
    return referencedFields;
  }

  Map<TableKey, TableBinding> getTableBindings() {
    return tableBindings;
  }

  boolean isBindingEnforced() {
    return bindingEnforced;
  }

  boolean matches(final TagFilterEvaluator.ValueProvider valueProvider) {
    throwIfFailure();
    return !isMatchNone()
        && (Objects.isNull(expression)
            || TagFilterEvaluator.evaluate(expression, valueProvider, compiledPatterns));
  }

  static final class TableKey {

    private final String database;
    private final String table;

    private TableKey(final String database, final String table) {
      this.database = normalize(database);
      this.table = normalize(table);
    }

    static TableKey of(final String database, final String table) {
      return new TableKey(database, table);
    }

    String getDatabase() {
      return database;
    }

    String getTable() {
      return table;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof TableKey)) {
        return false;
      }
      final TableKey that = (TableKey) object;
      return Objects.equals(database, that.database) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
      return Objects.hash(database, table);
    }

    private static String normalize(final String value) {
      return Objects.nonNull(value) ? value.trim().toLowerCase(java.util.Locale.ROOT) : "";
    }
  }

  static final class TableBinding {

    private final boolean failed;
    private final String failureReason;
    private final java.util.Set<String> tagNames;
    private final java.util.Set<String> allNames;

    TableBinding(
        final boolean failed,
        final String failureReason,
        final java.util.Set<String> tagNames,
        final java.util.Set<String> allNames) {
      this.failed = failed;
      this.failureReason = failureReason;
      this.tagNames = tagNames;
      this.allNames = allNames;
    }

    boolean isFailed() {
      return failed;
    }

    String getFailureReason() {
      return failureReason;
    }

    java.util.Set<String> getTagNames() {
      return tagNames;
    }

    java.util.Set<String> getAllNames() {
      return allNames;
    }
  }
}
