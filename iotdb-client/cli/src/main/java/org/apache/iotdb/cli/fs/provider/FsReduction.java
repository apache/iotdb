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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.cli.fs.provider;

import org.apache.iotdb.cli.fs.FsRowReader;
import org.apache.iotdb.cli.fs.command.ReductionOptions;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/** Merge raw observations at an explicit grain; rank only after complete aggregation. */
public final class FsReduction {
  private final ReductionOptions o;
  private final String model, scope;
  private final Map<Key, State> groups = new LinkedHashMap<>();

  public FsReduction(String model, String scope, ReductionOptions options) {
    this.model = model;
    this.scope = scope;
    this.o = options;
  }

  static void validate(List<FsColumn> schema, ReductionOptions o) {
    for (FsColumn c : schema)
      if (c.getName().equals(o.field)
          && "FIELD".equals(c.getCategory())
          && Arrays.asList("INT32", "INT64", "FLOAT", "DOUBLE").contains(c.getDataType())) return;
    throw ReductionOptions.invalid();
  }

  void scan(String object, List<FsColumn> schema, FsStatistics.Scan scan) throws SQLException {
    validate(schema, o);
    if (o.grid.bounds.isEmpty()) return;
    List<String> tags = new ArrayList<>();
    for (FsColumn c : schema) if ("TAG".equals(c.getCategory())) tags.add(c.getName());
    Collections.sort(tags);
    Predicate<SqlRow> filter = FsRowReader.predicate(schema, o.scanOptions());
    scan.visit(
        row -> {
          if (!filter.test(row)) return true;
          String raw = FsStatistics.value(row, o.field);
          if (raw == null) return true;
          BigDecimal value;
          try {
            value = new BigDecimal(raw);
          } catch (NumberFormatException e) {
            throw ReductionOptions.invalid();
          }
          long time = Long.parseLong(FsStatistics.value(row, "time"));
          List<String> identity = new ArrayList<>();
          identity.add(object);
          for (String t : tags) {
            identity.add(t);
            identity.add(FsStatistics.value(row, t));
          }
          for (int i = 0; i < o.grid.bounds.size(); i++)
            if (o.grid.bounds.get(i).contains(time)) {
              Key key = new Key(i, o.groupBy.equals("entity") ? identity : Collections.emptyList());
              State state = groups.get(key);
              if (state == null) {
                if (groups.size() >= 65536) throw ReductionOptions.invalid();
                state = new State(key);
                groups.put(key, state);
              }
              state.add(value, time, identity);
            }
          return true;
        });
  }

  private static int identityCompare(List<String> a, List<String> b) {
    for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
      int c = Comparator.nullsFirst(String::compareTo).compare(a.get(i), b.get(i));
      if (c != 0) return c;
    }
    return Integer.compare(a.size(), b.size());
  }

  public List<SqlRow> finish() {
    List<State> states = new ArrayList<>(groups.values());
    Comparator<State> stable =
        (a, b) -> {
          int c = Integer.compare(a.key.window, b.key.window);
          return c != 0 ? c : identityCompare(a.key.entity, b.key.entity);
        };
    Comparator<State> sort = stable;
    if (!o.rankBy.isEmpty())
      sort =
          (a, b) -> {
            BigDecimal av = a.metric(o.rankBy), bv = b.metric(o.rankBy);
            int c =
                av == null
                    ? (bv == null ? 0 : 1)
                    : (bv == null ? -1 : av.compareTo(bv) * (o.order.equals("asc") ? 1 : -1));
            return c == 0 ? stable.compare(a, b) : c;
          };
    states.sort(sort);
    List<SqlRow> out = new ArrayList<>();
    long selected = Math.min(o.top, states.size());
    for (int i = 0; i < selected; i++) {
      State s = states.get(i);
      Map<String, String> m = new LinkedHashMap<>();
      m.put("model", model);
      m.put("scope", scope);
      m.put("group_scope", o.groupBy);
      m.put("field", o.field);
      m.put("entity", o.groupBy.equals("entity") ? entityJson(s.key.entity) : null);
      m.put("object", o.groupBy.equals("entity") ? s.key.entity.get(0) : null);
      if (o.groupBy.equals("entity"))
        for (int j = 1; j < s.key.entity.size(); j += 2)
          m.put("tag." + s.key.entity.get(j), s.key.entity.get(j + 1));
      m.putAll(o.grid.descriptor(s.key.window));
      m.put("first_at", Long.toString(s.firstTime));
      m.put("last_at", Long.toString(s.lastTime));
      m.put("first_entity", entityJson(s.firstEntity));
      m.put("last_entity", entityJson(s.lastEntity));
      m.put("rank", o.rankBy.isEmpty() ? null : Integer.toString(i + 1));
      m.put("rank_by", o.rankBy.isEmpty() ? null : o.rankBy);
      m.put("rank_order", o.rankBy.isEmpty() ? null : o.order);
      for (String metric : o.metrics) {
        BigDecimal n = s.metric(metric);
        m.put(metric, n == null ? null : n.stripTrailingZeros().toPlainString());
      }
      out.add(new SqlRow(m));
    }
    return out;
  }

  private static String entityJson(List<String> e) {
    return new com.google.gson.Gson().toJson(e);
  }

  private static final class Key {
    final int window;
    final List<String> entity;

    Key(int w, List<String> e) {
      window = w;
      entity = new ArrayList<>(e);
    }

    public boolean equals(Object other) {
      return other instanceof Key
          && window == ((Key) other).window
          && entity.equals(((Key) other).entity);
    }

    public int hashCode() {
      return Objects.hash(window, entity);
    }
  }

  private static final class State {
    final Key key;
    BigInteger count = BigInteger.ZERO;
    BigDecimal sum = BigDecimal.ZERO, min, max, first, last;
    long firstTime, lastTime;
    List<String> firstEntity, lastEntity;

    State(Key key) {
      this.key = key;
    }

    void add(BigDecimal v, long t, List<String> id) {
      if (first == null
          || t < firstTime
          || (t == firstTime && identityCompare(id, firstEntity) < 0)) {
        first = v;
        firstTime = t;
        firstEntity = id;
      }
      if (last == null || t > lastTime || (t == lastTime && identityCompare(id, lastEntity) < 0)) {
        last = v;
        lastTime = t;
        lastEntity = id;
      }
      count = count.add(BigInteger.ONE);
      sum = sum.add(v);
      if (min == null || v.compareTo(min) < 0) min = v;
      if (max == null || v.compareTo(max) > 0) max = v;
    }

    BigDecimal metric(String name) {
      switch (name) {
        case "count":
          return new BigDecimal(count);
        case "sum":
          return sum;
        case "avg":
          return sum.divide(new BigDecimal(count), MathContext.DECIMAL128);
        case "min":
          return min;
        case "max":
          return max;
        case "first":
          return first;
        case "last":
          return last;
        case "delta":
          return last.subtract(first);
        case "elapsed_ms":
          return new BigDecimal(
              BigInteger.valueOf(lastTime).subtract(BigInteger.valueOf(firstTime)));
        case "rate_per_ms":
          BigDecimal span = metric("elapsed_ms");
          return span.signum() == 0 ? null : metric("delta").divide(span, MathContext.DECIMAL128);
        default:
          throw ReductionOptions.invalid();
      }
    }
  }
}
