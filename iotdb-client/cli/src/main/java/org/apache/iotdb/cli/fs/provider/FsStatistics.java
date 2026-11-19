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

import org.apache.iotdb.cli.fs.FsRowReader;
import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.command.ReadOptions.Window;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlExecutor.RowVisitor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

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
    try (Accumulator accumulator = new Accumulator(model, object, columns, true, false)) {
      for (SqlRow row : rows) accumulator.add(row);
      return accumulator.finish();
    }
  }

  static List<SqlRow> stats(
      String model, String object, List<FsColumn> columns, List<SqlRow> rows) {
    try (Accumulator accumulator = new Accumulator(model, object, columns, false, true)) {
      for (SqlRow row : rows) accumulator.add(row);
      return accumulator.finish();
    }
  }

  @FunctionalInterface
  interface Scan {
    void visit(RowVisitor visitor) throws SQLException;
  }

  static List<SqlRow> scan(
      String model,
      String object,
      List<FsColumn> schema,
      ReadOptions options,
      boolean count,
      Scan scan)
      throws SQLException {
    List<FsColumn> selected =
        count ? schema : FsRowReader.selectColumns(schema, options.getColumns());
    Predicate<SqlRow> predicate = FsRowReader.predicate(schema, options);
    List<Window> windows = options.getWindows();
    boolean median =
        !count && (options.getAggregates().isEmpty() || options.getAggregates().contains("median"));
    List<Accumulator> accumulators = new ArrayList<>();
    try {
      for (int i = 0; i < Math.max(1, windows.size()); i++)
        accumulators.add(new Accumulator(model, object, selected, count, median));
      scan.visit(
          row -> {
            if (!predicate.test(row)) return true;
            if (windows.isEmpty()) accumulators.get(0).add(row);
            else {
              String timestamp = value(row, "time");
              if (timestamp != null) {
                long time = Long.parseLong(timestamp);
                for (int i = 0; i < windows.size(); i++)
                  if (windows.get(i).contains(time)) accumulators.get(i).add(row);
              }
            }
            return true;
          });
      List<SqlRow> result = new ArrayList<>();
      for (int i = 0; i < accumulators.size(); i++) {
        for (SqlRow row : accumulators.get(i).finish()) {
          if (windows.isEmpty()) result.add(row);
          else {
            Map<String, String> cells = new LinkedHashMap<>(), types = new LinkedHashMap<>();
            cells.put("window_index", Integer.toString(i));
            types.put("window_index", "INT32");
            cells.put("window_start", Long.toString(windows.get(i).getStart()));
            types.put("window_start", "INT64");
            cells.put("window_end", Long.toString(windows.get(i).getEnd()));
            types.put("window_end", "INT64");
            for (Map.Entry<String, String> cell : row.asMap().entrySet()) {
              cells.put(cell.getKey(), cell.getValue());
              types.put(cell.getKey(), row.getDataType(cell.getKey()));
            }
            result.add(new SqlRow(cells, types));
          }
        }
      }
      return result;
    } finally {
      RuntimeException failure = null;
      for (Accumulator accumulator : accumulators) {
        try {
          accumulator.close();
        } catch (RuntimeException e) {
          if (failure == null) failure = e;
          else failure.addSuppressed(e);
        }
      }
      if (failure != null) throw failure;
    }
  }

  /** State grows with result groups/fields, never with the number of input rows. */
  static final class Accumulator implements AutoCloseable {
    private final String model;
    private final String object;
    private final List<FsColumn> columns;
    private final List<FsColumn> tags;
    private final boolean countOnly;
    private final boolean median;
    private final Map<List<String>, Group> groups = new LinkedHashMap<>();
    private final Set<List<String>> entities = new LinkedHashSet<>();

    Accumulator(
        String model, String object, List<FsColumn> columns, boolean countOnly, boolean median) {
      this.model = model;
      this.object = object;
      this.columns = columns;
      this.tags = tags(columns);
      this.countOnly = countOnly;
      this.median = median;
      if (countOnly || "tree".equals(model)) groups.put(Collections.emptyList(), new Group());
    }

    void add(SqlRow row) {
      List<String> key = key(tags, row);
      if (countOnly) entities.add(key);
      Group group =
          groups.computeIfAbsent(countOnly ? Collections.emptyList() : key, ignored -> new Group());
      group.rowCount++;
      String time = value(row, "time");
      group.timeline.add(time, time);
      for (Map.Entry<String, ValueStats> entry : group.values.entrySet()) {
        entry.getValue().add(value(row, entry.getKey()), time);
      }
    }

    List<SqlRow> finish() {
      List<SqlRow> result = new ArrayList<>();
      for (Map.Entry<List<String>, Group> entry : groups.entrySet()) {
        Group group = entry.getValue();
        ValueStats timeline = group.timeline;
        long rowCount = group.rowCount;
        for (FsColumn column : columns) {
          ValueStats statistic = group.values.get(column.getName());
          if (statistic == null) continue;
          if (countOnly) {
            boolean table = "table".equals(model);
            Map<String, String> cells = new LinkedHashMap<>();
            cells.put("model", model);
            cells.put("object", object);
            cells.put("column", column.getName());
            cells.put("category", column.getCategory());
            cells.put("row_count", Long.toString(rowCount));
            cells.put("entity_count", table ? Integer.toString(entities.size()) : null);
            cells.put("non_null_count", Long.toString(statistic.count));
            cells.put("null_count", Long.toString(rowCount - statistic.count));
            cells.put("min_time", table ? timeline.minTime : statistic.minTime);
            cells.put("max_time", table ? timeline.maxTime : statistic.maxTime);
            cells.put(
                "time_source", (table ? timeline.count : statistic.count) == 0 ? null : "scan");
            Map<String, String> types = new LinkedHashMap<>();
            for (String name : cells.keySet()) {
              types.put(
                  name, name.endsWith("_count") || name.endsWith("_time") ? "INT64" : "STRING");
            }
            result.add(new SqlRow(cells, types));

          } else {
            Map<String, String> cells = new LinkedHashMap<>();
            cells.put("model", model);
            cells.put("object", object);
            for (int i = 0; i < tags.size(); i++) {
              cells.put("tag." + tags.get(i).getName(), entry.getKey().get(i));
            }
            cells.put("field", column.getName());
            cells.put("data_type", column.getDataType());
            cells.put("non_null_count", Long.toString(statistic.count));
            cells.put("null_count", Long.toString(group.rowCount - statistic.count));
            boolean useTimeline = "table".equals(model) && statistic.count == 0;
            cells.put("min_time", useTimeline ? timeline.minTime : statistic.minTime);
            cells.put("max_time", useTimeline ? timeline.maxTime : statistic.maxTime);
            cells.put("min", statistic.min);
            cells.put("max", statistic.max);
            cells.put("first", statistic.first);
            cells.put("last", statistic.last);
            cells.put("sum", statistic.sum());
            cells.put("avg", statistic.average());
            cells.put("median", statistic.median());
            cells.put("stats_source", timeline.count == 0 ? null : "scan");
            Map<String, String> types = new LinkedHashMap<>();
            for (String name : cells.keySet()) {
              types.put(name, "STRING");
            }
            for (String name :
                new String[] {"non_null_count", "null_count", "min_time", "max_time"}) {
              types.put(name, "INT64");
            }
            for (String name : new String[] {"min", "max", "first", "last"}) {
              types.put(name, column.getDataType());
            }
            types.put("sum", statistic.sumDataType());
            types.put("avg", statistic.averageDataType());
            types.put("median", statistic.averageDataType());
            result.add(new SqlRow(cells, types));
          }
        }
      }
      return result;
    }

    @Override
    public void close() {
      RuntimeException failure = null;
      for (Group group : groups.values())
        for (ValueStats value : group.values.values()) {
          if (value.numericValues != null) {
            try {
              value.numericValues.close();
            } catch (RuntimeException e) {
              if (failure == null) failure = e;
              else failure.addSuppressed(e);
            }
          }
        }
      if (failure != null) throw failure;
    }

    private final class Group {
      private long rowCount;
      private final ValueStats timeline = new ValueStats("TIMESTAMP", false);
      private final Map<String, ValueStats> values = new LinkedHashMap<>();

      private Group() {
        for (FsColumn column : columns) {
          if ("FIELD".equals(column.getCategory())
              || (countOnly && "TAG".equals(column.getCategory())))
            values.put(
                column.getName(), new ValueStats(column.getDataType(), median && !countOnly));
        }
      }
    }
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

  static final class ValueStats {
    private final String type;
    private long count;
    private String minTime;
    private String maxTime;
    private String min;
    private String max;
    String first;
    String last;
    private BigInteger integerSum = BigInteger.ZERO;
    private double floatingSum;
    private double floatingCompensation;
    private BigDecimal floatingWideSum;
    private final ExactMedian numericValues;

    ValueStats(String type, boolean median) {
      this.type = type;
      this.numericValues = median && numeric() ? new ExactMedian(this::compare) : null;
    }

    void add(String value, String time) {
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
      if (numericValues != null) {
        numericValues.add(value);
      }
      if ("BOOLEAN".equals(type)) {
        if (Boolean.parseBoolean(value)) {
          integerSum = integerSum.add(BigInteger.ONE);
        }
      } else if (integer()) {
        integerSum = integerSum.add(new BigInteger(value));
      } else if (floating()) {
        addFloating("FLOAT".equals(type) ? Float.parseFloat(value) : Double.parseDouble(value));
      }
    }

    private void addFloating(double value) {
      if (floatingWideSum != null) {
        if (Double.isFinite(value)) {
          floatingWideSum = floatingWideSum.add(BigDecimal.valueOf(value));
        } else {
          floatingWideSum = null;
          floatingSum = value;
        }
        return;
      }
      double next = floatingSum + value;
      if (Double.isFinite(floatingSum) && Double.isFinite(value) && !Double.isFinite(next)) {
        floatingWideSum =
            BigDecimal.valueOf(floatingSum)
                .add(BigDecimal.valueOf(floatingCompensation))
                .add(BigDecimal.valueOf(value));
        floatingSum = 0;
        floatingCompensation = 0;
        return;
      }
      if (Double.isFinite(floatingSum) && Double.isFinite(value) && Double.isFinite(next)) {
        floatingCompensation +=
            Math.abs(floatingSum) >= Math.abs(value)
                ? (floatingSum - next) + value
                : (value - next) + floatingSum;
      } else {
        floatingCompensation = 0;
      }
      floatingSum = next;
    }

    private double floatingTotal() {
      return floatingWideSum == null
          ? floatingSum + floatingCompensation
          : floatingWideSum.doubleValue();
    }

    private boolean floating() {
      return "FLOAT".equals(type) || "DOUBLE".equals(type);
    }

    private boolean integer() {
      return "INT32".equals(type) || "INT64".equals(type);
    }

    private boolean numeric() {
      return integer() || floating();
    }

    private String sumDataType() {
      if (floating()) {
        return "DOUBLE";
      }
      if ("BOOLEAN".equals(type) || "INT32".equals(type)) {
        return "INT64";
      }
      return "STRING";
    }

    String averageDataType() {
      return "INT64".equals(type) ? "STRING" : "DOUBLE";
    }

    private String sum() {
      if (count == 0) {
        return null;
      }
      if ("BOOLEAN".equals(type) || integer()) {
        return integerSum.toString();
      }
      return floating() ? Double.toString(floatingTotal()) : null;
    }

    String average() {
      if (!numeric() || count == 0) {
        return null;
      }
      if (integer()) {
        return new BigDecimal(integerSum)
            .divide(BigDecimal.valueOf(count), MathContext.DECIMAL128)
            .stripTrailingZeros()
            .toPlainString();
      }
      // Divide the wide total before narrowing: a finite mean can have an overflowing sum.
      if (floatingWideSum != null) {
        return Double.toString(
            floatingWideSum
                .divide(BigDecimal.valueOf(count), MathContext.DECIMAL128)
                .doubleValue());
      }
      double total = floatingTotal();
      if (!Double.isFinite(total)
          && Double.isFinite(floatingSum)
          && Double.isFinite(floatingCompensation)) {
        return Double.toString(
            BigDecimal.valueOf(floatingSum)
                .add(BigDecimal.valueOf(floatingCompensation))
                .divide(BigDecimal.valueOf(count), MathContext.DECIMAL128)
                .doubleValue());
      }
      return Double.toString(total / count);
    }

    private String median() {
      if (numericValues == null || count == 0) return null;
      String[] middle = numericValues.middle();
      if (count % 2 != 0) return middle[1];
      if (integer()) {
        return new BigDecimal(middle[0])
            .add(new BigDecimal(middle[1]))
            .divide(BigDecimal.valueOf(2))
            .stripTrailingZeros()
            .toPlainString();
      }
      return floatingMidpoint(middle[0], middle[1]);
    }

    private static String floatingMidpoint(String left, String right) {
      double lower = Double.parseDouble(left);
      double upper = Double.parseDouble(right);
      if (!Double.isFinite(lower) || !Double.isFinite(upper)) {
        return Double.toString((lower + upper) / 2);
      }
      return Double.toString(
          BigDecimal.valueOf(lower)
              .add(BigDecimal.valueOf(upper))
              .divide(BigDecimal.valueOf(2))
              .doubleValue());
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
