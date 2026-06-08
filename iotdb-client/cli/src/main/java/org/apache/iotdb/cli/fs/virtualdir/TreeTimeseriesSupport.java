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

package org.apache.iotdb.cli.fs.virtualdir;

import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.TreeFilesystemSql;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class TreeTimeseriesSupport {

  private static final String TIMESERIES_COLUMN = "Timeseries";

  private TreeTimeseriesSupport() {}

  static List<SqlRow> query(SqlExecutor executor, String sql) throws SQLException {
    List<SqlRow> rows = executor.query(sql);
    return rows == null ? new ArrayList<>() : rows;
  }

  static String timeseriesPath(SqlRow row) {
    return row.get(TIMESERIES_COLUMN);
  }

  static String measurementName(String timeseries) {
    return TreeFilesystemSql.measurementName(timeseries);
  }

  static FsPath canonicalTimeseriesPath(String timeseries) {
    return TreeFilesystemSql.fromTreePath(timeseries);
  }

  static FsNode timeseriesNode(SqlRow row) {
    String timeseries = timeseriesPath(row);
    return new FsNode(
        timeseries, canonicalTimeseriesPath(timeseries), FsNodeType.TREE_TIMESERIES, row.asMap());
  }

  static Map<String, String> parseMetadataMap(SqlRow row, String column) {
    Map<String, String> values = new LinkedHashMap<>();
    String text = row.get(column);
    if (text == null) {
      return values;
    }
    String trimmed = text.trim();
    if (trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed)) {
      return values;
    }
    if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
      trimmed = trimmed.substring(1, trimmed.length() - 1);
    }
    if (trimmed.trim().isEmpty()) {
      return values;
    }
    for (String entry : trimmed.split(",")) {
      int equals = entry.indexOf('=');
      if (equals <= 0) {
        continue;
      }
      String key = entry.substring(0, equals).trim();
      String value = entry.substring(equals + 1).trim();
      if (!key.isEmpty()) {
        values.put(key, value);
      }
    }
    return values;
  }

  static String quote(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  static Map<String, String> displayMetadata(String key, String value) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(key, value);
    return metadata;
  }
}
