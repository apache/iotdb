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
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FsStreamingStatisticsTest {
  private final List<FsColumn> columns =
      Arrays.asList(new FsColumn("x", "FIELD", "INT64"), new FsColumn("y", "FIELD", "DOUBLE"));

  private ReadOptions options(String aggregates, String windows) {
    return new ReadOptions(
            "ndjson",
            "",
            "",
            Collections.singletonList("x"),
            -1,
            0,
            null,
            null,
            Collections.emptyList(),
            "all")
        .withStatistics(aggregates, windows);
  }

  @Test
  public void overlappingDuplicateEmptyAndExtremeWindowsShareOneScan() throws Exception {
    List<SqlRow> rows =
        Arrays.asList(
            SqlRow.of("time", "-2", "x", "1"),
            SqlRow.of("time", "0", "x", null),
            SqlRow.of("time", "2", "x", "9007199254740993"),
            SqlRow.of("time", "3", "x", "5"));
    String windows = "-2:2,2:3,-2:2,8:9,-9223372036854775808:9223372036854775807";
    int[] scans = {0};
    List<SqlRow> batch =
        FsStatistics.scan(
            "tree",
            "root.d",
            columns,
            options("", windows),
            false,
            visitor -> {
              scans[0]++;
              for (SqlRow row : rows) visitor.visit(row);
            });
    assertEquals(1, scans[0]);
    assertEquals(5, batch.size());
    List<ReadOptions.Window> ranges = ReadOptions.parseWindows(windows);
    for (int i = 0; i < ranges.size(); i++) {
      List<SqlRow> selected = new ArrayList<>();
      for (SqlRow row : rows)
        if (ranges.get(i).contains(Long.parseLong(row.get("time")))) selected.add(row);
      SqlRow expected =
          FsStatistics.stats("tree", "root.d", Collections.singletonList(columns.get(0)), selected)
              .get(0);
      Map<String, String> actual = new LinkedHashMap<>(batch.get(i).asMap());
      actual.keySet().removeAll(Arrays.asList("window_index", "window_start", "window_end"));
      assertEquals(expected.asMap(), actual);
      assertEquals(Integer.toString(i), batch.get(i).get("window_index"));
    }
    assertEquals("1", batch.get(0).get("null_count"));
    assertEquals("0", batch.get(3).get("non_null_count"));
    assertNull(batch.get(3).get("stats_source"));
  }

  @Test
  public void scalarStatisticsHandleMillionRowsWithoutMedianRetention() throws Exception {
    List<SqlRow> result =
        FsStatistics.scan(
            "tree",
            "root.d",
            columns,
            options("count,avg,first,last", ""),
            false,
            visitor -> {
              for (int i = 0; i < 1000000; i++)
                visitor.visit(SqlRow.of("time", Integer.toString(i), "x", Integer.toString(i)));
            });
    assertEquals("1000000", result.get(0).get("non_null_count"));
    assertEquals("499999500000", result.get(0).get("sum"));
    assertEquals("499999.5", result.get(0).get("avg"));
    assertEquals("999999", result.get(0).get("last"));
    assertNull(result.get(0).get("median"));
  }

  @Test
  public void spilledMedianPreservesLargeIntegersAcrossMergeLevels() {
    try (FsStatistics.Accumulator accumulator =
        new FsStatistics.Accumulator(
            "tree", "root.d", Collections.singletonList(columns.get(0)), false, true)) {
      BigInteger base = new BigInteger("9007199254740993");
      for (int i = 199999; i >= 0; i--)
        accumulator.add(
            SqlRow.of(
                "time", Integer.toString(i), "x", base.add(BigInteger.valueOf(i)).toString()));
      SqlRow result = accumulator.finish().get(0);
      assertEquals("9007199254840992.5", result.get("median"));
      assertEquals(base.toString(), result.get("first"));
      assertEquals(base.add(BigInteger.valueOf(199999)).toString(), result.get("last"));
    }
  }

  @Test
  public void tableWindowsPreserveTagIdentityAndAllNullTimeline() throws Exception {
    List<FsColumn> schema = Arrays.asList(new FsColumn("site", "TAG", "STRING"), columns.get(0));
    List<SqlRow> result =
        FsStatistics.scan(
            "table",
            "t",
            schema,
            options("count,first,last", "0:10,20:30"),
            false,
            visitor -> {
              visitor.visit(SqlRow.of("time", "1", "site", "a", "x", null));
              visitor.visit(SqlRow.of("time", "2", "site", "b", "x", "7"));
            });
    assertEquals(2, result.size());
    assertEquals("a", result.get(0).get("tag.site"));
    assertEquals("1", result.get(0).get("min_time"));
    assertEquals("1", result.get(0).get("null_count"));
    assertEquals("7", result.get(1).get("first"));
  }
}
