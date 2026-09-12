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

package org.apache.iotdb.cli.fs.provider;

import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.CliMessages;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class TableFilesystemCopyPlanner {

  private TableFilesystemCopyPlanner() {}

  static String create(String database, String table, List<SqlRow> schema, SqlRow metadata)
      throws SQLException {
    if (schema.isEmpty()) {
      throw invalidOperation();
    }
    List<String> definitions = new ArrayList<>();
    for (SqlRow column : schema) {
      String type = required(column, "DataType").toUpperCase(Locale.ROOT);
      String category = required(column, "Category").toUpperCase(Locale.ROOT);
      if (!type.matches("BOOLEAN|INT32|INT64|FLOAT|DOUBLE|TEXT|STRING|BLOB|TIMESTAMP|DATE")
          || !category.matches("TIME|TAG|ATTRIBUTE|FIELD")) {
        throw invalidOperation();
      }
      String definition =
          TableFilesystemSql.identifier(required(column, "ColumnName"))
              + " "
              + type
              + " "
              + category;
      String comment = column.get("Comment");
      if (comment != null && !comment.isEmpty()) {
        definition += " COMMENT " + literal(comment);
      }
      definitions.add(definition);
    }
    String sql =
        "CREATE TABLE "
            + TableFilesystemSql.tablePath(database, table)
            + " ("
            + String.join(", ", definitions)
            + ")";
    if (metadata == null) {
      return sql;
    }
    String comment = metadata.get("Comment");
    if (comment != null && !comment.isEmpty()) {
      sql += " COMMENT " + literal(comment);
    }
    List<String> properties = new ArrayList<>();
    String ttl = metadata.get("TTL(ms)");
    if (ttl != null) {
      if ("INF".equalsIgnoreCase(ttl)) {
        properties.add("ttl='INF'");
      } else if (ttl.matches("[0-9]+")) {
        properties.add("ttl=" + ttl);
      } else {
        throw invalidOperation();
      }
    }
    String cache = metadata.get("NeedLastCache");
    if (cache != null) {
      if (!"true".equalsIgnoreCase(cache) && !"false".equalsIgnoreCase(cache)) {
        throw invalidOperation();
      }
      properties.add("need_last_cache=" + cache.toLowerCase(Locale.ROOT));
    }
    if (!properties.isEmpty()) {
      sql += " WITH (" + String.join(", ", properties) + ")";
    }
    return sql;
  }

  static String columns(List<SqlRow> schema) throws SQLException {
    List<String> names = new ArrayList<>();
    for (SqlRow column : schema) {
      names.add(TableFilesystemSql.identifier(required(column, "ColumnName")));
    }
    return String.join(", ", names);
  }

  private static String required(SqlRow row, String key) throws SQLException {
    String value = row.get(key);
    if (value == null || value.isEmpty()) {
      throw invalidOperation();
    }
    return value;
  }

  private static String literal(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static SQLException invalidOperation() {
    return new SQLException(CliMessages.FS_INVALID_WRITE_OPERATION);
  }
}
