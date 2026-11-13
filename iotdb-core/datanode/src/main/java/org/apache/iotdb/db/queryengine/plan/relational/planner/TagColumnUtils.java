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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TAG;

/**
 * Shared checks for whether a set of columns covers every TAG column of a table.
 *
 * <p>A scan produces one device per distinct TAG combination, so "every TAG column is covered"
 * implies that each group of the covering columns belongs to at most one device. Distribution,
 * grouping parallelization and stream sort all rely on this property, so they must agree on its
 * definition.
 */
public final class TagColumnUtils {

  private TagColumnUtils() {}

  /** Returns the names of all TAG columns in the given table schema. */
  public static Set<String> tagColumnNames(Map<Symbol, ColumnSchema> tableSchema) {
    return tagColumnNames(tableSchema.keySet(), tableSchema);
  }

  /** Returns the names of the TAG columns among the given symbols. */
  public static Set<String> tagColumnNames(
      Collection<Symbol> symbols, Map<Symbol, ColumnSchema> columnSchema) {
    Set<String> tagColumnNames = new HashSet<>();
    for (Symbol symbol : symbols) {
      ColumnSchema schema = columnSchema.get(symbol);
      if (schema != null && schema.getColumnCategory() == TAG) {
        tagColumnNames.add(schema.getName());
      }
    }
    return tagColumnNames;
  }

  /** Whether the given column names cover every TAG column of the table. */
  public static boolean coversAllTagColumns(
      Map<Symbol, ColumnSchema> tableSchema, Collection<String> coveredColumnNames) {
    return coveredColumnNames.containsAll(tagColumnNames(tableSchema));
  }
}
