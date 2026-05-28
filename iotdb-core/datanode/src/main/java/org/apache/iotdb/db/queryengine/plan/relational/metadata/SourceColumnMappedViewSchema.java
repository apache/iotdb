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
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;

import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shared relational schema contract for view-like tables that expose a column alias mapping.
 *
 * <p>This only models the per-column "view name -> underlying source name" relationship. It does
 * not mean the source object itself is another view type. Writable views still require a base-table
 * source even though they share this lightweight column metadata carrier with tree views.
 */
abstract class SourceColumnMappedViewSchema extends TableSchema {

  private final Map<String, String> viewColumnToSourceColumnMap;

  protected SourceColumnMappedViewSchema(final String tableName, final List<ColumnSchema> columns) {
    this(tableName, columns, extractViewColumnToSourceColumnMap(columns));
  }

  protected SourceColumnMappedViewSchema(
      final String tableName,
      final List<ColumnSchema> columns,
      final Map<String, String> viewColumnToSourceColumnMap) {
    super(tableName, columns);
    this.viewColumnToSourceColumnMap =
        buildViewColumnToSourceColumnMap(columns, viewColumnToSourceColumnMap);
  }

  public Map<String, String> getViewColumnToSourceColumnMap() {
    return viewColumnToSourceColumnMap;
  }

  private static Map<String, String> extractViewColumnToSourceColumnMap(
      final List<ColumnSchema> columns) {
    return columns.stream()
        .filter(
            columnSchema ->
                Objects.nonNull(columnSchema.getProps())
                    && columnSchema.getProps().containsKey(ViewColumnSchemaUtils.SOURCE_NAME))
        .collect(
            Collectors.toMap(
                ColumnSchema::getName,
                columnSchema -> columnSchema.getProps().get(ViewColumnSchemaUtils.SOURCE_NAME)));
  }

  static Map<String, String> buildViewColumnToSourceColumnMap(
      final List<ColumnSchema> columns, final Map<String, String> explicitMap) {
    final Map<String, String> sourceNameMap = extractViewColumnToSourceColumnMap(columns);
    if (sourceNameMap.isEmpty()) {
      return Objects.nonNull(explicitMap) ? ImmutableMap.copyOf(explicitMap) : ImmutableMap.of();
    }
    if (Objects.isNull(explicitMap) || explicitMap.isEmpty()) {
      return ImmutableMap.copyOf(sourceNameMap);
    }
    final Map<String, String> mergedMap = new LinkedHashMap<>(sourceNameMap);
    mergedMap.putAll(explicitMap);
    return ImmutableMap.copyOf(mergedMap);
  }
}
