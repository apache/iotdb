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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import com.timecho.iotdb.db.queryengine.plan.relational.metadata.WritableViewUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Locale.ENGLISH;

/**
 * Lightweight metadata carrier for rewriting writes on writable views to their source tables.
 *
 * <p>This avoids constructing full relational {@code TableSchema} objects in the hot insert path
 * when only source-table routing and column-name resolution are required.
 */
public final class WritableViewInsertRewriteSupport {

  private final QualifiedObjectName writableViewName;
  private final QualifiedObjectName sourceTableName;
  private final Map<String, String> viewColumnToSourceColumnMap;
  private final Set<String> viewColumnNames;
  private final boolean sourceTableKnownToExist;
  private final Predicate<String> sourceColumnExists;
  private final boolean requiresSourceColumnRewrite;

  public WritableViewInsertRewriteSupport(
      final QualifiedObjectName writableViewName,
      final QualifiedObjectName sourceTableName,
      final Map<String, String> viewColumnToSourceColumnMap,
      final boolean sourceTableKnownToExist,
      final Predicate<String> sourceColumnExists) {
    this(
        writableViewName,
        sourceTableName,
        viewColumnToSourceColumnMap,
        Objects.nonNull(viewColumnToSourceColumnMap)
            ? viewColumnToSourceColumnMap.keySet()
            : Collections.emptySet(),
        sourceTableKnownToExist,
        sourceColumnExists);
  }

  public WritableViewInsertRewriteSupport(
      final QualifiedObjectName writableViewName,
      final QualifiedObjectName sourceTableName,
      final Map<String, String> viewColumnToSourceColumnMap,
      final Collection<String> viewColumnNames,
      final boolean sourceTableKnownToExist,
      final Predicate<String> sourceColumnExists) {
    this.writableViewName = Objects.requireNonNull(writableViewName, "writableViewName is null");
    this.sourceTableName = Objects.requireNonNull(sourceTableName, "sourceTableName is null");
    this.viewColumnToSourceColumnMap =
        Objects.nonNull(viewColumnToSourceColumnMap)
            ? viewColumnToSourceColumnMap
            : Collections.emptyMap();
    this.viewColumnNames = normalizeColumnNames(viewColumnNames);
    this.sourceTableKnownToExist = sourceTableKnownToExist;
    this.sourceColumnExists =
        Objects.requireNonNull(sourceColumnExists, "sourceColumnExists is null");
    this.requiresSourceColumnRewrite =
        WritableViewUtils.requiresSourceColumnRewrite(this.viewColumnToSourceColumnMap);
  }

  public QualifiedObjectName getSourceTableName() {
    return sourceTableName;
  }

  public boolean requiresSourceColumnRewrite() {
    return requiresSourceColumnRewrite;
  }

  private static Set<String> normalizeColumnNames(final Collection<String> columnNames) {
    final Set<String> normalizedColumnNames = new HashSet<>();
    if (Objects.nonNull(columnNames)) {
      for (final String columnName : columnNames) {
        if (Objects.nonNull(columnName)) {
          normalizedColumnNames.add(columnName);
          normalizedColumnNames.add(columnName.toLowerCase(ENGLISH));
        }
      }
    }
    return Collections.unmodifiableSet(normalizedColumnNames);
  }

  public void ensureSourceTableExists() {
    if (!sourceTableKnownToExist) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.SOURCE_TABLE_OF_WRITABLE_VIEW_DOES_NOT_EXIST,
              sourceTableName.getDatabaseName(),
              sourceTableName.getObjectName(),
              writableViewName.getDatabaseName(),
              writableViewName.getObjectName()));
    }
  }

  public String validateExposedViewColumnName(final String viewColumnName) {
    if (Objects.isNull(viewColumnName)
        || (!viewColumnNames.contains(viewColumnName)
            && !viewColumnNames.contains(viewColumnName.toLowerCase(ENGLISH)))) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.INSERT_COLUMN_NOT_EXPOSED_BY_WRITABLE_VIEW,
              viewColumnName,
              writableViewName.getDatabaseName(),
              writableViewName.getObjectName()));
    }
    return viewColumnName;
  }

  public String resolveExistingSourceColumnName(final String viewColumnName) {
    ensureSourceTableExists();
    validateExposedViewColumnName(viewColumnName);

    final String sourceColumnName =
        WritableViewUtils.getSourceColumnName(viewColumnName, viewColumnToSourceColumnMap);
    if (Objects.nonNull(sourceColumnName)) {
      if (!sourceColumnExists.test(sourceColumnName)) {
        WritableViewUtils.throwColumnNotExistsException(
            writableViewName, sourceTableName, viewColumnName, sourceColumnName);
      }
      return sourceColumnName;
    }

    if (sourceColumnExists.test(viewColumnName)) {
      return viewColumnName;
    }
    final String normalizedViewColumnName = viewColumnName.toLowerCase(ENGLISH);
    if (sourceColumnExists.test(normalizedViewColumnName)) {
      return normalizedViewColumnName;
    }

    WritableViewUtils.throwColumnNotExistsException(
        writableViewName, sourceTableName, viewColumnName, (String) null);
    return viewColumnName;
  }
}
