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

import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.command.TemporalOptions;
import org.apache.iotdb.cli.fs.command.TemporalOptions.Mode;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class FsTemporalTest {
  private final List<FsColumn> schema =
      Arrays.asList(new FsColumn("x", "FIELD", "INT64"), new FsColumn("y", "FIELD", "DOUBLE"));

  private ReadOptions read(Long lo, Long hi, String windows) {
    return new ReadOptions(
            "ndjson",
            "",
            "",
            Collections.singletonList("y"),
            -1,
            0,
            lo,
            hi,
            Collections.emptyList(),
            "all")
        .withStatistics("", windows);
  }

  private List<SqlRow> run(List<FsColumn> columns, List<SqlRow> input, TemporalOptions options)
      throws Exception {
    List<SqlRow> out = new ArrayList<>();
    FsTemporal.run(
        "table",
        "db.t",
        columns,
        options,
        sink -> {
          for (SqlRow row : input) if (!sink.visit(row)) break;
        },
        row -> {
          out.add(row);
          return true;
        });
    return out;
  }

  @Test
  public void gapsCarryRealPredecessorAcrossWindowAndSkipNulls() throws Exception {
    List<SqlRow> out =
        run(
            schema,
            Arrays.asList(
                SqlRow.of("time", "0", "x", "1"),
                SqlRow.of("time", "10", "x", null),
                SqlRow.of("time", "20", "x", "2"),
                SqlRow.of("time", "25", "x", "3")),
            new TemporalOptions(
                Mode.GAPS, read(null, null, "10:20,21:30,10:20,100:200"), "x", "", 0));
    assertEquals(3, out.size());
    assertEquals("0", out.get(0).get("previous_at"));
    assertEquals("20", out.get(0).get("observed_at"));
    assertEquals("20", out.get(0).get("gap_ms"));
    assertEquals("first,last,min,max", out.get(0).get("selected_as"));
    assertEquals("20", out.get(1).get("previous_at"));
    assertEquals("5", out.get(1).get("gap_ms"));
    assertEquals("2", out.get(2).get("window_index"));
  }

  @Test
  public void gapTiesChooseEarliestAndDurationDoesNotOverflow() throws Exception {
    List<SqlRow> out =
        run(
            schema,
            Arrays.asList(
                SqlRow.of("time", Long.toString(Long.MIN_VALUE), "x", "1"),
                SqlRow.of("time", Long.toString(Long.MAX_VALUE), "x", "1")),
            new TemporalOptions(Mode.GAPS, read(null, null, ""), "x", "", 0));
    assertEquals("18446744073709551615", out.get(0).get("gap_ms"));
    out =
        run(
            schema,
            Arrays.asList(
                SqlRow.of("time", "0", "x", "1"),
                SqlRow.of("time", "10", "x", "1"),
                SqlRow.of("time", "20", "x", "1")),
            new TemporalOptions(Mode.GAPS, read(10L, 20L, ""), "x", "", 0));
    assertEquals("first,min,max", out.get(0).get("selected_as"));
    assertEquals("last", out.get(1).get("selected_as"));
  }

  @Test
  public void sessionsSplitStrictlyAboveThresholdWithExactIntegerMean() throws Exception {
    List<SqlRow> input =
        Arrays.asList(
            SqlRow.of("time", "0", "x", "9007199254740992"),
            SqlRow.of("time", "100", "x", "9007199254740993"),
            SqlRow.of("time", "201", "x", "5"),
            SqlRow.of("time", "250", "x", null),
            SqlRow.of("time", "401", "x", "7"));
    List<SqlRow> out =
        run(schema, input, new TemporalOptions(Mode.SESSIONS, read(null, null, ""), "x", "", 100));
    assertEquals(3, out.size());
    assertEquals("9007199254740992.5", out.get(0).get("avg"));
    assertEquals("2", out.get(0).get("count"));
    assertEquals("100", out.get(0).get("session_end"));
    assertEquals("201", out.get(1).get("session_start"));
    out =
        run(schema, input, new TemporalOptions(Mode.SESSIONS, read(100L, 201L, ""), "x", "", 100));
    assertEquals("100", out.get(0).get("session_start"));
    assertEquals("1", out.get(0).get("count"));
    assertEquals(2, out.size());
  }

  @Test
  public void fullScanFindsHiddenGapInsideNonemptyBroadWindow() throws Exception {
    List<SqlRow> input =
        Arrays.asList(
            SqlRow.of("time", "0", "x", "1"),
            SqlRow.of("time", "10", "x", "2"),
            SqlRow.of("time", "1000", "x", "3"),
            SqlRow.of("time", "1010", "x", "4"));
    List<SqlRow> out =
        run(schema, input, new TemporalOptions(Mode.SESSIONS, read(null, null, ""), "x", "", 100));
    assertEquals(2, out.size());
    assertEquals("10", out.get(0).get("session_end"));
    assertEquals("1000", out.get(1).get("session_start"));
  }

  @Test
  public void entitiesIncludingNullTagsNeverPool() throws Exception {
    List<FsColumn> columns = new ArrayList<>(schema);
    columns.add(new FsColumn("device", "TAG", "STRING"));
    List<SqlRow> out =
        run(
            columns,
            Arrays.asList(
                SqlRow.of("time", "0", "x", "1", "device", null),
                SqlRow.of("time", "0", "x", "100", "device", "b"),
                SqlRow.of("time", "10", "x", "3", "device", null),
                SqlRow.of("time", "10", "x", "200", "device", "b")),
            new TemporalOptions(Mode.SESSIONS, read(null, null, ""), "x", "", 100));
    assertEquals(2, out.size());
    assertNull(out.get(0).get("tag.device"));
    assertEquals("2", out.get(0).get("avg"));
    assertEquals("b", out.get(1).get("tag.device"));
    assertEquals("150", out.get(1).get("avg"));
    assertEquals("entity", out.get(1).get("group_scope"));
  }

  @Test
  public void extremePreservesAssociatedNullAndEarliestExactIntegerTie() throws Exception {
    List<SqlRow> input =
        Arrays.asList(
            SqlRow.of("time", "0", "x", null, "y", "4"),
            SqlRow.of("time", "1", "x", "9007199254740992", "y", "5"),
            SqlRow.of("time", "2", "x", "9007199254740993", "y", null),
            SqlRow.of("time", "3", "x", "9007199254740993", "y", "8"));
    List<SqlRow> out =
        run(
            schema,
            input,
            new TemporalOptions(Mode.EXTREME, read(null, null, "0:3,1:1,0:0"), "x", "max", 0));
    assertEquals(2, out.size());
    assertEquals("2", out.get(0).get("observed_at"));
    assertEquals("9007199254740993", out.get(0).get("extreme_value"));
    assertNull(out.get(0).get("value.y"));
    assertEquals("1", out.get(1).get("observed_at"));
  }

  @Test
  public void closedSessionsStreamAndConsumerStopsSource() throws Exception {
    int[] visited = {0}, emitted = {0};
    boolean complete =
        FsTemporal.run(
            "tree",
            "root.db.d",
            schema,
            new TemporalOptions(Mode.SESSIONS, read(null, null, ""), "x", "", 1),
            sink -> {
              for (int i = 0; i < 1000000; i++) {
                visited[0]++;
                if (!sink.visit(SqlRow.of("time", Long.toString(i * 2L), "x", "1"))) break;
              }
            },
            row -> {
              emitted[0]++;
              return false;
            });
    assertFalse(complete);
    assertEquals(2, visited[0]);
    assertEquals(1, emitted[0]);
  }

  @Test
  public void sessionAverageStaysFiniteWhenItsSumOverflows() throws Exception {
    String largest = Double.toString(Double.MAX_VALUE);
    List<SqlRow> out =
        run(
            schema,
            Arrays.asList(
                SqlRow.of("time", "0", "y", largest), SqlRow.of("time", "1", "y", largest)),
            new TemporalOptions(Mode.SESSIONS, read(null, null, ""), "y", "", 10));
    assertEquals(1, out.size());
    assertEquals("2", out.get(0).get("count"));
    assertEquals(Double.MAX_VALUE, Double.parseDouble(out.get(0).get("avg")), 0.0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nonFiniteNumericValuesFailExplicitly() throws Exception {
    run(
        schema,
        Collections.singletonList(SqlRow.of("time", "0", "x", "1", "y", "NaN")),
        new TemporalOptions(Mode.EXTREME, read(null, null, ""), "y", "max", 0));
  }
}
