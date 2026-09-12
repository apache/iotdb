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

import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Computes the TsFile CLI's logical statistics from the visible database snapshot. */
final class FsStatistics {
  private FsStatistics() {}

  static String value(SqlRow row, String column) {
    for (Map.Entry<String, String> entry : row.asMap().entrySet()) {
      if (column.equalsIgnoreCase(entry.getKey())) {
        return entry.getValue();
      }
    }
    return null;
  }

  static SqlRow schema(String model, String object, FsColumn column) {
    return SqlRow.of(
        "model",
        model,
        "object",
        object,
        "column",
        column.getName(),
        "category",
        column.getCategory(),
        "data_type",
        column.getDataType(),
        "encoding",
        column.getEncoding(),
        "compression",
        column.getCompression());
  }

  static List<SqlRow> count(
      String model, String object, List<FsColumn> columns, List<SqlRow> rows) {
    List<FsColumn> tags = tags(columns);
    Set<List<String>> entities = new LinkedHashSet<>();
    ValueStats timeline = new ValueStats("TIMESTAMP");
    for (SqlRow row : rows) {
      entities.add(key(tags, row));
      timeline.add(value(row, "time"), value(row, "time"));
    }
    List<SqlRow> result = new ArrayList<>();
    for (FsColumn column : columns) {
      if (!"TAG".equals(column.getCategory()) && !"FIELD".equals(column.getCategory())) {
        continue;
      }
      ValueStats statistic = new ValueStats(column.getDataType());
      for (SqlRow row : rows) {
        statistic.add(value(row, column.getName()), value(row, "time"));
      }
      boolean table = "table".equals(model);
      Map<String, String> cells = new LinkedHashMap<>();
      cells.put("model", model);
      cells.put("object", object);
      cells.put("column", column.getName());
      cells.put("category", column.getCategory());
      cells.put("row_count", Integer.toString(rows.size()));
      cells.put("entity_count", table ? Integer.toString(entities.size()) : null);
      cells.put("non_null_count", Long.toString(statistic.count));
      cells.put("null_count", Long.toString(rows.size() - statistic.count));
      cells.put("min_time", table ? timeline.minTime : statistic.minTime);
      cells.put("max_time", table ? timeline.maxTime : statistic.maxTime);
      cells.put("time_source", (table ? timeline.count : statistic.count) == 0 ? null : "scan");
      Map<String, String> types = new LinkedHashMap<>();
      for (String name : cells.keySet()) {
        types.put(name, name.endsWith("_count") || name.endsWith("_time") ? "INT64" : "STRING");
      }
      result.add(new SqlRow(cells, types));
    }
    return result;
  }

  static List<SqlRow> stats(
      String model, String object, List<FsColumn> columns, List<SqlRow> rows) {
    List<FsColumn> tags = tags(columns);
    Map<List<String>, List<SqlRow>> devices = new LinkedHashMap<>();
    for (SqlRow row : rows) {
      devices.computeIfAbsent(key(tags, row), ignored -> new ArrayList<>()).add(row);
    }
    if (rows.isEmpty() && "tree".equals(model)) {
      devices.put(new ArrayList<>(), rows);
    }
    List<SqlRow> result = new ArrayList<>();
    for (Map.Entry<List<String>, List<SqlRow>> device : devices.entrySet()) {
      ValueStats timeline = new ValueStats("TIMESTAMP");
      for (SqlRow row : device.getValue()) {
        timeline.add(value(row, "time"), value(row, "time"));
      }
      for (FsColumn column : columns) {
        if (!"FIELD".equals(column.getCategory())) {
          continue;
        }
        ValueStats statistic = new ValueStats(column.getDataType());
        for (SqlRow row : device.getValue()) {
          statistic.add(value(row, column.getName()), value(row, "time"));
        }
        Map<String, String> cells = new LinkedHashMap<>();
        cells.put("model", model);
        cells.put("object", object);
        for (int i = 0; i < tags.size(); i++) {
          cells.put("tag." + tags.get(i).getName(), device.getKey().get(i));
        }
        cells.put("field", column.getName());
        cells.put("data_type", column.getDataType());
        cells.put("non_null_count", Long.toString(statistic.count));
        cells.put("null_count", Long.toString(device.getValue().size() - statistic.count));
        boolean useTimeline = "table".equals(model) && statistic.count == 0;
        cells.put("min_time", useTimeline ? timeline.minTime : statistic.minTime);
        cells.put("max_time", useTimeline ? timeline.maxTime : statistic.maxTime);
        cells.put("min", statistic.min);
        cells.put("max", statistic.max);
        cells.put("first", statistic.first);
        cells.put("last", statistic.last);
        cells.put("sum", statistic.sum());
        cells.put("stats_source", timeline.count == 0 ? null : "scan");
        Map<String, String> types = new LinkedHashMap<>();
        for (String name : cells.keySet()) {
          types.put(name, "STRING");
        }
        for (String name : new String[] {"non_null_count", "null_count", "min_time", "max_time"}) {
          types.put(name, "INT64");
        }
        for (String name : new String[] {"min", "max", "first", "last"}) {
          types.put(name, column.getDataType());
        }
        types.put("sum", statistic.floating() ? "DOUBLE" : "INT64");
        result.add(new SqlRow(cells, types));
      }
    }
    return result;
  }

  private static List<FsColumn> tags(List<FsColumn> columns) {
    List<FsColumn> result = new ArrayList<>();
    for (FsColumn column : columns) {
      if ("TAG".equals(column.getCategory())) {
        result.add(column);
      }
    }
    return result;
  }

  private static List<String> key(List<FsColumn> tags, SqlRow row) {
    List<String> result = new ArrayList<>();
    for (FsColumn tag : tags) {
      result.add(value(row, tag.getName()));
    }
    return result;
  }

  private static final class ValueStats {
    private final String type;
    private long count;
    private String minTime;
    private String maxTime;
    private String min;
    private String max;
    private String first;
    private String last;
    private BigInteger integerSum = BigInteger.ZERO;
    private double floatingSum;

    private ValueStats(String type) {
      this.type = type;
    }

    private void add(String value, String time) {
      if (value == null) {
        return;
      }
      count++;
      boolean firstTime = minTime == null || compareTime(time, minTime) < 0;
      boolean lastTime = maxTime == null || compareTime(time, maxTime) >= 0;
      if (firstTime) {
        minTime = time;
      }
      if (lastTime) {
        maxTime = time;
      }
      if ("BLOB".equals(type)) {
        return;
      }
      if (firstTime) {
        first = value;
      }
      if (lastTime) {
        last = value;
      }
      if (!"BOOLEAN".equals(type) && !"TEXT".equals(type)) {
        if (min == null || compare(value, min) < 0) {
          min = value;
        }
        if (max == null || compare(value, max) > 0) {
          max = value;
        }
      }
      if ("BOOLEAN".equals(type)) {
        if (Boolean.parseBoolean(value)) {
          integerSum = integerSum.add(BigInteger.ONE);
        }
      } else if ("INT32".equals(type)) {
        integerSum = integerSum.add(new BigInteger(value));
      } else if (floating()) {
        floatingSum += "FLOAT".equals(type) ? Float.parseFloat(value) : Double.parseDouble(value);
      }
    }

    private boolean floating() {
      return "FLOAT".equals(type) || "DOUBLE".equals(type);
    }

    private String sum() {
      if (count == 0) {
        return null;
      }
      if ("BOOLEAN".equals(type) || "INT32".equals(type)) {
        return integerSum.toString();
      }
      return floating() ? Double.toString(floatingSum) : null;
    }

    private int compare(String left, String right) {
      if ("INT32".equals(type) || "INT64".equals(type) || "TIMESTAMP".equals(type)) {
        return new BigInteger(left).compareTo(new BigInteger(right));
      }
      if (floating()) {
        return Double.compare(Double.parseDouble(left), Double.parseDouble(right));
      }
      byte[] a = left.getBytes(StandardCharsets.UTF_8);
      byte[] b = right.getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < Math.min(a.length, b.length); i++) {
        int comparison = Integer.compare(a[i] & 255, b[i] & 255);
        if (comparison != 0) {
          return comparison;
        }
      }
      return Integer.compare(a.length, b.length);
    }

    private static int compareTime(String left, String right) {
      if (left == null) {
        return 0;
      }
      return new BigInteger(left).compareTo(new BigInteger(right));
    }
  }
}
