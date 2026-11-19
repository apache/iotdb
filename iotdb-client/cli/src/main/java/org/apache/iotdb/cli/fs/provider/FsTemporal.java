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
import org.apache.iotdb.cli.fs.command.TemporalOptions;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlExecutor.RowVisitor;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.FsTemporalMessages;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

/** Complete ordered scans; state is bounded per entity/window, sessions emit on closure. */
public final class FsTemporal {
  private FsTemporal() {}

  public static List<FsColumn> outputColumns(List<FsColumn> schema, TemporalOptions options) {
    List<String> names = new ArrayList<>(Arrays.asList("model", "object", "group_scope"));
    for (FsColumn c : schema) if ("TAG".equals(c.getCategory())) names.add("tag." + c.getName());
    names.add("field");
    switch (options.getMode()) {
      case GAPS:
        names.addAll(
            Arrays.asList(
                "window_index",
                "window_start",
                "window_end",
                "observed_at",
                "previous_at",
                "gap_ms",
                "selected_as"));
        break;
      case SESSIONS:
        names.addAll(
            Arrays.asList(
                "session_index", "session_start", "session_end", "count", "avg", "first", "last"));
        break;
      case EXTREME:
        names.addAll(
            Arrays.asList(
                "window_index",
                "window_start",
                "window_end",
                "mode",
                "observed_at",
                "extreme_value"));
        for (String name : associated(options)) names.add("value." + name);
        break;
      default:
        throw TemporalOptions.invalid();
    }
    List<FsColumn> result = new ArrayList<>();
    for (String name : names) result.add(new FsColumn(name, "FIELD", "STRING"));
    return result;
  }

  private static List<String> associated(TemporalOptions options) {
    List<String> names = new ArrayList<>();
    names.add(options.getField());
    for (String name : options.getRead().getColumns()) if (!names.contains(name)) names.add(name);
    return names;
  }

  static void validate(List<FsColumn> schema, TemporalOptions options) {
    FsColumn field = find(schema, options.getField());
    if (field == null || !"FIELD".equals(field.getCategory())) throw invalid(options.getField());
    if (options.getMode() != TemporalOptions.Mode.GAPS
        && !Arrays.asList("INT32", "INT64", "FLOAT", "DOUBLE").contains(field.getDataType()))
      throw invalid(options.getField());
    if (options.getMode() == TemporalOptions.Mode.EXTREME)
      for (String name : associated(options)) {
        FsColumn column = find(schema, name);
        if (column == null || !"FIELD".equals(column.getCategory())) throw invalid(name);
      }
  }

  private static FsColumn find(List<FsColumn> schema, String name) {
    for (FsColumn c : schema) if (c.getName().equals(name)) return c;
    return null;
  }

  private static IllegalArgumentException invalid(String field) {
    return new IllegalArgumentException(
        String.format(
            FsTemporalMessages
                .EXCEPTION_UNKNOWN_OR_UNSUPPORTED_ANALYSIS_FIELD_ARG_SESSIONS_EXTREME_REQUIRE_FINITE_NUMERIC_FIELD_VALUES_B9ADCF22,
            field));
  }

  static boolean run(
      String model,
      String object,
      List<FsColumn> schema,
      TemporalOptions options,
      FsStatistics.Scan scan,
      RowVisitor output)
      throws SQLException {
    validate(schema, options);
    Engine engine = new Engine(model, object, schema, options, output);
    Predicate<SqlRow> filter = FsRowReader.predicate(schema, options.scanOptions());
    scan.visit(row -> !filter.test(row) || engine.add(row));
    engine.finish();
    return !engine.stopped;
  }

  private static final class Engine {
    final String model, object, type;
    final List<FsColumn> schema, tags = new ArrayList<>();
    final TemporalOptions options;
    final RowVisitor output;
    final Map<List<String>, Group> groups = new LinkedHashMap<>();
    boolean stopped;

    Engine(
        String model,
        String object,
        List<FsColumn> schema,
        TemporalOptions options,
        RowVisitor output) {
      this.model = model;
      this.object = object;
      this.schema = schema;
      this.options = options;
      this.output = output;
      this.type = find(schema, options.getField()).getDataType();
      for (FsColumn c : schema) if ("TAG".equals(c.getCategory())) tags.add(c);
    }

    int windows() {
      return Math.max(1, options.getRead().getWindows().size());
    }

    Long lower(int i) {
      return options.getRead().getWindows().isEmpty()
          ? options.getRead().getStart()
          : Long.valueOf(options.getRead().getWindows().get(i).getStart());
    }

    Long upper(int i) {
      return options.getRead().getWindows().isEmpty()
          ? options.getRead().getEnd()
          : Long.valueOf(options.getRead().getWindows().get(i).getEnd());
    }

    boolean contains(int i, long time) {
      return (lower(i) == null || time >= lower(i)) && (upper(i) == null || time <= upper(i));
    }

    boolean add(SqlRow row) throws SQLException {
      if (stopped) return false;
      String value = FsStatistics.value(row, options.getField());
      if (value == null) return true;
      if (options.getMode() != TemporalOptions.Mode.GAPS) {
        try {
          new BigDecimal(value);
        } catch (NumberFormatException e) {
          throw invalid(options.getField());
        }
      }
      long time = Long.parseLong(FsStatistics.value(row, "time"));
      List<String> key = new ArrayList<>();
      for (FsColumn c : tags) key.add(FsStatistics.value(row, c.getName()));
      Group group = groups.computeIfAbsent(key, ignored -> new Group(key));
      if (group.previous != null && time <= group.previous)
        throw new IllegalArgumentException(
            FsTemporalMessages
                .EXCEPTION_ANALYSIS_REQUIRES_STRICTLY_INCREASING_TIMESTAMPS_WITHIN_EACH_ENTITY_655D0DE5);
      switch (options.getMode()) {
        case GAPS:
          if (group.previous != null) {
            Gap gap = new Gap(group.previous, time);
            for (int i = 0; i < windows(); i++) if (contains(i, time)) group.gaps.get(i).add(gap);
          }
          break;
        case SESSIONS:
          if (group.previous != null
              && distance(group.previous, time).compareTo(BigInteger.valueOf(options.getGap()))
                  > 0) {
            emit(group.sessionRow());
            if (stopped) return false;
            group.sessionIndex++;
            group.values = null;
          }
          if (group.values == null) {
            group.values = new FsStatistics.ValueStats(type, false);
            group.start = time;
            group.count = 0;
          }
          group.values.add(value, Long.toString(time));
          group.count++;
          group.end = time;
          break;
        case EXTREME:
          for (int i = 0; i < windows(); i++)
            if (contains(i, time)) {
              SqlRow prior = group.extremes.get(i);
              int cmp =
                  prior == null
                      ? 0
                      : new BigDecimal(value)
                          .compareTo(new BigDecimal(FsStatistics.value(prior, options.getField())));
              if (prior == null || (options.isMaximum() ? cmp > 0 : cmp < 0))
                group.extremes.set(i, row);
            }
          break;
        default:
          throw TemporalOptions.invalid();
      }
      group.previous = time;
      return !stopped;
    }

    void emit(SqlRow row) throws SQLException {
      if (!stopped) stopped = !output.visit(row);
    }

    void finish() throws SQLException {
      for (Group group : groups.values()) {
        if (stopped) break;
        if (options.getMode() == TemporalOptions.Mode.SESSIONS) {
          if (group.values != null) emit(group.sessionRow());
          continue;
        }
        for (int i = 0; i < windows() && !stopped; i++) {
          if (options.getMode() == TemporalOptions.Mode.EXTREME) {
            SqlRow selected = group.extremes.get(i);
            if (selected == null) continue;
            Result row = group.base();
            window(row, i);
            row.put("mode", options.isMaximum() ? "max" : "min");
            row.put("observed_at", FsStatistics.value(selected, "time"));
            row.put("extreme_value", FsStatistics.value(selected, options.getField()), type);
            for (String name : associated(options))
              row.put(
                  "value." + name,
                  FsStatistics.value(selected, name),
                  find(schema, name).getDataType());
            emit(row.row());
          } else {
            GapState state = group.gaps.get(i);
            Map<Long, Gap> selected = new TreeMap<>();
            for (Gap gap : Arrays.asList(state.first, state.last, state.min, state.max))
              if (gap != null) selected.put(gap.time, gap);
            for (Gap gap : selected.values()) {
              Result row = group.base();
              window(row, i);
              row.put("observed_at", Long.toString(gap.time));
              row.put("previous_at", Long.toString(gap.previous));
              row.put("gap_ms", gap.duration.toString());
              List<String> roles = new ArrayList<>();
              if (gap == state.first) roles.add("first");
              if (gap == state.last) roles.add("last");
              if (gap == state.min) roles.add("min");
              if (gap == state.max) roles.add("max");
              row.put("selected_as", String.join(",", roles));
              emit(row.row());
              if (stopped) break;
            }
          }
        }
      }
    }

    void window(Result row, int index) {
      row.put("window_index", Integer.toString(index), "INT32");
      row.put("window_start", lower(index) == null ? null : lower(index).toString());
      row.put("window_end", upper(index) == null ? null : upper(index).toString());
    }

    final class Group {
      final List<String> key;
      Long previous;
      long sessionIndex, start, end, count;
      FsStatistics.ValueStats values;
      final List<GapState> gaps = new ArrayList<>();
      final List<SqlRow> extremes = new ArrayList<>();

      Group(List<String> key) {
        this.key = key;
        for (int i = 0; i < windows(); i++) {
          gaps.add(new GapState());
          extremes.add(null);
        }
      }

      Result base() {
        Result row = new Result();
        row.put("model", model);
        row.put("object", object);
        row.put("group_scope", "entity");
        for (int i = 0; i < tags.size(); i++) row.put("tag." + tags.get(i).getName(), key.get(i));
        row.put("field", options.getField());
        return row;
      }

      SqlRow sessionRow() {
        Result row = base();
        row.put("session_index", Long.toString(sessionIndex));
        row.put("session_start", Long.toString(start));
        row.put("session_end", Long.toString(end));
        row.put("count", Long.toString(count));
        row.put("avg", values.average(), values.averageDataType());
        row.put("first", values.first, type);
        row.put("last", values.last, type);
        return row.row();
      }
    }
  }

  private static BigInteger distance(long from, long to) {
    return BigInteger.valueOf(to).subtract(BigInteger.valueOf(from));
  }

  private static final class Gap {
    final long previous, time;
    final BigInteger duration;

    Gap(long previous, long time) {
      this.previous = previous;
      this.time = time;
      duration = distance(previous, time);
    }
  }

  private static final class GapState {
    Gap first, last, min, max;

    void add(Gap gap) {
      if (first == null) first = gap;
      last = gap;
      if (min == null || gap.duration.compareTo(min.duration) < 0) min = gap;
      if (max == null || gap.duration.compareTo(max.duration) > 0) max = gap;
    }
  }

  private static final class Result {
    final Map<String, String> cells = new LinkedHashMap<>(), types = new LinkedHashMap<>();

    void put(String name, String value) {
      put(name, value, "STRING");
    }

    void put(String name, String value, String type) {
      cells.put(name, value);
      types.put(name, type);
    }

    SqlRow row() {
      return new SqlRow(cells, types);
    }
  }
}
