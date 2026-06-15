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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Map;
import java.util.Objects;

/** Metadata helpers for SQL-based logical views (CREATE VIEW ... AS SELECT ...). */
public class SqlViewSchema {

  public static final String VIEW_QUERY_SQL = "__view_query_sql";
  public static final String VIEW_OWNER = "__view_owner";

  public static boolean isSqlViewTable(final TsTable table) {
    return table.getPropValue(VIEW_QUERY_SQL).isPresent();
  }

  public static boolean isSqlViewTable(final TableSchema tableSchema) {
    final Map<String, String> props = tableSchema.getProps();
    return props != null && props.containsKey(VIEW_QUERY_SQL);
  }

  public static String getQuerySql(final TsTable table) {
    return table
        .getPropValue(VIEW_QUERY_SQL)
        .orElseThrow(
            () ->
                new IoTDBRuntimeException(
                    String.format(
                        "Failed to get view query SQL for table %s", table.getTableName()),
                    TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
  }

  public static String getQuerySql(final TableSchema tableSchema) {
    final Map<String, String> props = tableSchema.getProps();
    if (props == null || !props.containsKey(VIEW_QUERY_SQL)) {
      throw new IoTDBRuntimeException(
          String.format("Failed to get view query SQL for table %s", tableSchema.getTableName()),
          TSStatusCode.SEMANTIC_ERROR.getStatusCode());
    }
    return props.get(VIEW_QUERY_SQL);
  }

  public static void setQuerySql(final TsTable table, final String querySql) {
    table.addProp(VIEW_QUERY_SQL, Objects.requireNonNull(querySql, "querySql is null"));
  }

  public static void setOwner(final TsTable table, final String owner) {
    if (Objects.nonNull(owner)) {
      table.addProp(VIEW_OWNER, owner);
    }
  }

  private SqlViewSchema() {
    // Private constructor
  }
}
