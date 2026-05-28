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

import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import com.timecho.iotdb.db.queryengine.plan.relational.metadata.WritableViewUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Relational schema wrapper for writable views.
 *
 * <p>Writable views share the column-level source-name carrier with tree views, but their source
 * table is still tracked explicitly and is currently expected to be a base table.
 */
public class WritableViewSchema extends SourceColumnMappedViewSchema {
  private final QualifiedObjectName sourceTableName;
  private final TableSchema sourceTableSchema;
  private final boolean sourceTableKnownToExist;
  private final boolean sourceColumnsIdenticalToView;
  private final boolean canUseIdentitySourceFastPath;
  private final Map<String, Integer> sourceTagColumnIndexMap;

  public WritableViewSchema(
      String tableName,
      List<ColumnSchema> columns,
      QualifiedObjectName sourceTableName,
      java.util.Map<String, String> viewColumnToSourceColumnMap) {
    this(tableName, columns, sourceTableName, viewColumnToSourceColumnMap, null);
  }

  public WritableViewSchema(
      String tableName,
      List<ColumnSchema> columns,
      QualifiedObjectName sourceTableName,
      java.util.Map<String, String> viewColumnToSourceColumnMap,
      TableSchema sourceTableSchema) {
    this(
        tableName,
        columns,
        sourceTableName,
        viewColumnToSourceColumnMap,
        sourceTableSchema,
        sourceTableSchema != null,
        false,
        java.util.Collections.emptyMap());
  }

  public WritableViewSchema(
      String tableName,
      List<ColumnSchema> columns,
      QualifiedObjectName sourceTableName,
      java.util.Map<String, String> viewColumnToSourceColumnMap,
      TableSchema sourceTableSchema,
      boolean sourceTableKnownToExist,
      boolean sourceColumnsIdenticalToView) {
    this(
        tableName,
        columns,
        sourceTableName,
        viewColumnToSourceColumnMap,
        sourceTableSchema,
        sourceTableKnownToExist,
        sourceColumnsIdenticalToView,
        java.util.Collections.emptyMap());
  }

  public WritableViewSchema(
      String tableName,
      List<ColumnSchema> columns,
      QualifiedObjectName sourceTableName,
      java.util.Map<String, String> viewColumnToSourceColumnMap,
      TableSchema sourceTableSchema,
      boolean sourceTableKnownToExist,
      boolean sourceColumnsIdenticalToView,
      Map<String, Integer> sourceTagColumnIndexMap) {
    super(tableName, columns, viewColumnToSourceColumnMap);
    this.sourceTableName = Objects.requireNonNull(sourceTableName, "sourceTableName is null");
    this.sourceTableSchema = sourceTableSchema;
    this.sourceTableKnownToExist = sourceTableKnownToExist;
    this.sourceColumnsIdenticalToView = sourceColumnsIdenticalToView;
    this.canUseIdentitySourceFastPath =
        sourceTableKnownToExist
            && sourceColumnsIdenticalToView
            && !WritableViewUtils.requiresSourceColumnRewrite(getViewColumnToSourceColumnMap());
    final Map<String, Integer> resolvedSourceTagColumnIndexMap =
        Objects.requireNonNull(sourceTagColumnIndexMap, "sourceTagColumnIndexMap is null");
    this.sourceTagColumnIndexMap =
        java.util.Collections.unmodifiableMap(
            new java.util.HashMap<>(
                canUseIdentitySourceFastPath && resolvedSourceTagColumnIndexMap.isEmpty()
                    ? buildTagColumnIndexMap(columns)
                    : resolvedSourceTagColumnIndexMap));
  }

  public QualifiedObjectName getSourceTableName() {
    return sourceTableName;
  }

  public Optional<TableSchema> getSourceTableSchema() {
    return Optional.ofNullable(sourceTableSchema);
  }

  public Map<String, Integer> getSourceTagColumnIndexMap() {
    return sourceTagColumnIndexMap;
  }

  public boolean canUseIdentitySourceFastPath() {
    return canUseIdentitySourceFastPath;
  }

  private static Map<String, Integer> buildTagColumnIndexMap(final List<ColumnSchema> columns) {
    final Map<String, Integer> tagColumnIndexMap = new java.util.HashMap<>();
    int tagIndex = 0;
    for (final ColumnSchema column : columns) {
      if (column.getColumnCategory() == TsTableColumnCategory.TAG) {
        tagColumnIndexMap.put(column.getName(), tagIndex++);
      }
    }
    return tagColumnIndexMap;
  }
}
