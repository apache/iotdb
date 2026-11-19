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

import org.apache.iotdb.cli.fs.command.FilesystemCommand;
import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;
import org.apache.iotdb.cli.fs.command.ReductionOptions;
import org.apache.iotdb.cli.fs.command.WindowGrid;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FsReductionTest {
  private FilesystemCommand parse(String command) {
    FilesystemCommand c = FilesystemCommandParser.parse(command);
    assertNotEquals(c.getErrorMessage(), FilesystemCommand.Type.INVALID, c.getType());
    return c;
  }

  private WindowGrid grid(String flags) {
    return WindowGrid.from(parse("windows -f ndjson " + flags));
  }

  private ReductionOptions options(String flags) {
    return ReductionOptions.from(parse("reduce /root/db --field x " + flags));
  }

  private List<FsColumn> schema() {
    return Arrays.asList(new FsColumn("x", "FIELD", "INT64"), new FsColumn("tag", "TAG", "STRING"));
  }

  private void feed(FsReduction e, String object, SqlRow... rows) throws Exception {
    e.scan(
        object,
        schema(),
        sink -> {
          for (SqlRow r : rows) if (!sink.visit(r)) break;
        });
  }

  @Test
  public void rejectsUnusedGridPathAndSubmillisecondOrigin() {
    for (String command :
        Arrays.asList(
            "windows /root/db --start 0 --end 10 --origin 0 --width 1",
            "windows --start 0 --end 10 --origin 1970-01-01T00:00:00.0001Z --width 1",
            "reduce /root/db --field x --windows ''")) {
      assertEquals(
          FilesystemCommand.Type.INVALID, FilesystemCommandParser.parse(command).getType());
    }
  }

  @Test
  public void fixedGridNegativeIndicesClippingAndGaps() {
    WindowGrid g = grid("--start -15 --end 23 --origin 0 --width 7 --step 10");
    assertEquals(4, g.bounds.size());
    assertEquals(Long.valueOf(-10), g.bounds.get(0).start);
    assertEquals(Long.valueOf(-4), g.bounds.get(0).end);
    assertEquals(Long.valueOf(23), g.bounds.get(3).end);
    assertEquals("27", g.descriptor(3).get("nominal_end_exclusive"));
    assertEquals(0, grid("--start 1 --end 2 --origin 0 --width 10 --step 10").bounds.size());
  }

  @Test
  public void calendarDayDiffersFromFixedDurationAcrossDst() {
    long start = Instant.parse("2024-03-10T05:00:00Z").toEpochMilli(),
        end = Instant.parse("2024-03-12T03:59:59.999Z").toEpochMilli();
    String flags =
        "--start "
            + start
            + " --end "
            + end
            + " --origin 2024-03-10T00:00:00 --timezone America/New_York";
    WindowGrid g = grid(flags + " --width 1 --unit day");
    assertEquals(2, g.bounds.size());
    assertEquals(23 * 3600000L, g.bounds.get(0).nominalEnd - g.bounds.get(0).start);
    assertEquals(
        24 * 3600000L,
        grid(flags + " --width 86400000 --unit ms").bounds.get(0).nominalEnd - start);
    assertTrue(g.descriptor(1).get("start_local").contains("-04:00"));
  }

  @Test
  public void calendarMonthsStayAnchoredAndOverlapAllowed() {
    WindowGrid g =
        grid(
            "--start 1706659200000 --end 1714521599999 --origin 2024-01-31T00:00:00 --width 2 --step 1 --unit month");
    assertEquals(4, g.bounds.size());
    assertEquals("2024-03-31T00:00Z[UTC]", g.descriptor(2).get("start_local"));
    assertEquals("2024-02-29T00:00Z[UTC]", g.descriptor(1).get("start_local"));
    assertTrue(g.bounds.get(0).end >= g.bounds.get(1).start);
  }

  @Test
  public void invalidGridsRejectDstAmbiguityLimitsAndOverflow() {
    for (String flags :
        Arrays.asList(
            "--start 0 --end 10 --origin 2024-03-10T02:30:00 --timezone America/New_York --width 1 --unit day",
            "--start 0 --end 10 --origin 2024-11-03T01:30:00 --timezone America/New_York --width 1 --unit day",
            "--start 0 --end 300 --origin 0 --width 1",
            "--start 9223372036854775806 --end 9223372036854775807 --origin 9223372036854775806 --width 2",
            "--start 0 --end 10 --origin 0 --width 0"))
      assertEquals(
          FilesystemCommand.Type.INVALID,
          FilesystemCommandParser.parse("windows " + flags).getType());
  }

  @Test
  public void unequalEntitiesPoolObservationsNotMeans() throws Exception {
    FsReduction e =
        new FsReduction(
            "tree",
            "/root/db",
            options(
                "--group-by pooled --metrics count,sum,avg,delta,first,last,elapsed_ms,rate_per_ms"));
    feed(e, "root.db.b", SqlRow.of("time", "10", "x", "90"));
    feed(
        e,
        "root.db.a",
        SqlRow.of("time", "0", "x", "0"),
        SqlRow.of("time", "1", "x", "0"),
        SqlRow.of("time", "10", "x", "0"),
        SqlRow.of("time", "20", "x", null));
    SqlRow r = e.finish().get(0);
    assertEquals("4", r.get("count"));
    assertEquals("22.5", r.get("avg"));
    assertEquals("0", r.get("last"));
    assertEquals("0", r.get("delta"));
    assertTrue(r.get("last_entity").contains("root.db.a"));
    assertNull(r.get("object"));
  }

  @Test
  public void entityGrainNullTagsAndDuplicateWindows() throws Exception {
    FsReduction e =
        new FsReduction(
            "table", "db.t", options("--windows 0:10,0:10,20:30 --metrics count,delta"));
    feed(
        e,
        "db.t",
        SqlRow.of("time", "0", "tag", null, "x", "2"),
        SqlRow.of("time", "0", "tag", "null", "x", "100"),
        SqlRow.of("time", "10", "tag", null, "x", "5"));
    List<SqlRow> r = e.finish();
    assertEquals(4, r.size());
    assertNull(r.get(0).get("tag.tag"));
    assertEquals("3", r.get(0).get("delta"));
    assertEquals("null", r.get(1).get("tag.tag"));
    assertEquals("1", r.get(2).get("window_index"));
  }

  @Test
  public void globalRankingNotPoolingAndDeterministicTies() throws Exception {
    FsReduction e =
        new FsReduction(
            "tree",
            "/root/db",
            options("--metrics avg --rank-by avg --top 2 --windows 0:10,20:30"));
    feed(e, "b", SqlRow.of("time", "0", "x", "5"), SqlRow.of("time", "20", "x", "6"));
    feed(e, "a", SqlRow.of("time", "0", "x", "5"), SqlRow.of("time", "20", "x", "6"));
    List<SqlRow> r = e.finish();
    assertEquals(2, r.size());
    assertEquals("a", r.get(0).get("object"));
    assertEquals("1", r.get(0).get("window_index"));
    assertEquals("2", r.get(1).get("rank"));
  }

  @Test
  public void endpointDifferenceAndSpanAvoidInt64Overflow() throws Exception {
    FsReduction e =
        new FsReduction("tree", "x", options("--metrics first,last,delta,elapsed_ms,rate_per_ms"));
    feed(
        e,
        "a",
        SqlRow.of("time", Long.toString(Long.MIN_VALUE), "x", Long.toString(Long.MIN_VALUE)),
        SqlRow.of("time", Long.toString(Long.MAX_VALUE), "x", Long.toString(Long.MAX_VALUE)));
    SqlRow r = e.finish().get(0);
    assertEquals("18446744073709551615", r.get("delta"));
    assertEquals(r.get("delta"), r.get("elapsed_ms"));
    assertEquals("1", r.get("rate_per_ms"));
  }

  @Test
  public void zeroSpanNullLastAndAscendingRank() throws Exception {
    FsReduction e =
        new FsReduction(
            "tree", "x", options("--metrics delta,rate_per_ms --rank-by rate_per_ms --order asc"));
    feed(e, "a", SqlRow.of("time", "0", "x", "3"));
    feed(e, "b", SqlRow.of("time", "0", "x", "4"), SqlRow.of("time", "2", "x", "2"));
    List<SqlRow> r = e.finish();
    assertEquals("b", r.get(0).get("object"));
    assertEquals("-1", r.get(0).get("rate_per_ms"));
    assertNull(r.get(1).get("rate_per_ms"));
  }

  @Test
  public void rejectsNonfiniteAndInvalidMetric() throws Exception {
    FsReduction e = new FsReduction("tree", "x", options(""));
    try {
      feed(e, "a", SqlRow.of("time", "0", "x", "Infinity"));
      fail();
    } catch (IllegalArgumentException expected) {
    }
    for (String flags :
        Arrays.asList(
            "--metrics avg --rank-by delta",
            "--group-by unknown",
            "--metrics count,count",
            "--top 2",
            "--origin 0 --width 1"))
      assertEquals(
          FilesystemCommand.Type.INVALID,
          FilesystemCommandParser.parse("reduce /root/db --field x " + flags).getType());
  }
}
