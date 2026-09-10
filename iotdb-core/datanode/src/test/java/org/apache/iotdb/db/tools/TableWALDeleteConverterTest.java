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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class TableWALDeleteConverterTest {

  private static final List<String> TAGS = Arrays.asList("tag1", "tag2");
  private static final long[] TIMES = {
    Long.MIN_VALUE,
    Long.MIN_VALUE + 1,
    -11,
    -10,
    -1,
    0,
    9,
    10,
    20,
    21,
    30,
    40,
    41,
    Long.MAX_VALUE - 1,
    Long.MAX_VALUE
  };

  /**
   * OR must preserve each device filter's own inclusive interval instead of mixing their ranges.
   */
  @Test
  public void testMergeDisjointRanges() throws Exception {
    assertEquivalent(
        "table1",
        "time",
        TAGS,
        Arrays.asList(
            entry(new TagPredicate.SegmentExactMatch("a", 1), 10, 20),
            entry(new TagPredicate.SegmentExactMatch("b", 1), 30, 40)),
        sampleDevices());
  }

  /** Every serialized TAG predicate must survive SQL parsing and deletion analysis unchanged. */
  @Test
  public void testTagPredicateRoundTrip() throws Exception {
    final List<TagPredicate> predicates =
        Arrays.asList(
            new TagPredicate.NOP(),
            new TagPredicate.SegmentExactMatch("a", 1),
            new TagPredicate.SegmentExactMatch(null, 2),
            new TagPredicate.SegmentNotNull(2),
            new TagPredicate.FullExactMatch(device("table1", "a", "x")),
            new TagPredicate.DeviceIn(
                Arrays.asList(device("table1", "a"), device("table1", "b", "x"))),
            new TagPredicate.And(
                new TagPredicate.SegmentExactMatch("table1", 0),
                new TagPredicate.And(
                    new TagPredicate.NOP(), new TagPredicate.SegmentExactMatch("a", 1)),
                new TagPredicate.SegmentNotNull(2)),
            new TagPredicate.SegmentNotNull(0));
    for (final TagPredicate predicate : predicates) {
      assertEquivalent(
          "table1",
          "time",
          TAGS,
          Collections.singletonList(entry(predicate, -10, 20)),
          sampleDevices());
    }
  }

  /**
   * An exact device with trimmed trailing nulls must not also delete devices with non-null TAGs.
   */
  @Test
  public void testExactDeviceWithTrailingNullTags() throws Exception {
    for (final IDeviceID device : Arrays.asList(device("table1", "a"), device("table1"))) {
      assertEquivalent(
          "table1",
          "time",
          TAGS,
          Collections.singletonList(
              entry(new TagPredicate.FullExactMatch(device), Long.MIN_VALUE, Long.MAX_VALUE)),
          sampleDevices());
    }
  }

  /**
   * Empty device sets and mismatching table-name segments must never become unqualified DELETEs.
   */
  @Test
  public void testEmptyPredicates() throws Exception {
    final TableWALDeleteConverter converter = new TableWALDeleteConverter("table1", "time", TAGS);
    assertNull(converter.toSql(Collections.emptyList()));
    for (final TagPredicate predicate :
        Arrays.asList(
            new TagPredicate.DeviceIn(Collections.emptySet()),
            new TagPredicate.SegmentExactMatch("other", 0),
            new TagPredicate.And(
                new TagPredicate.NOP(), new TagPredicate.DeviceIn(Collections.emptySet())))) {
      assertNull(converter.toSql(Collections.singletonList(entry(predicate, 10, 20))));
      assertEquivalent(
          "table1",
          "time",
          TAGS,
          Arrays.asList(
              entry(predicate, 10, 20), entry(new TagPredicate.SegmentExactMatch("b", 1), 30, 40)),
          sampleDevices());
    }
  }

  /**
   * Full-range and tagless deletions omit WHERE instead of emitting unsupported boolean literals.
   */
  @Test
  public void testUnboundedAndTaglessDeletes() throws Exception {
    final List<TableDeletionEntry> entries =
        Collections.singletonList(entry(new TagPredicate.NOP(), Long.MIN_VALUE, Long.MAX_VALUE));
    assertEquals(
        "DELETE FROM \"table1\"",
        new TableWALDeleteConverter("table1", "time", TAGS).toSql(entries));
    assertEquivalent("table1", "time", TAGS, entries, sampleDevices());
    assertEquivalent(
        "table1",
        "time",
        Collections.emptyList(),
        Collections.singletonList(entry(new TagPredicate.FullExactMatch(device("table1")), 10, 20)),
        Collections.singletonList(device("table1")));
  }

  /** Inclusive long endpoints, negative timestamps, and renamed TIME columns must remain exact. */
  @Test
  public void testTimeBoundariesAndRenamedTimeColumn() throws Exception {
    for (final long[] range :
        new long[][] {
          {Long.MIN_VALUE, Long.MIN_VALUE},
          {Long.MAX_VALUE, Long.MAX_VALUE},
          {Long.MIN_VALUE, -10},
          {-10, Long.MAX_VALUE},
          {-10, -1},
          {0, 0}
        }) {
      assertEquivalent(
          "table1",
          "ts",
          TAGS,
          Collections.singletonList(entry(new TagPredicate.NOP(), range[0], range[1])),
          sampleDevices());
    }
  }

  /** Identifiers and values containing SQL punctuation must remain literal names and TAG values. */
  @Test
  public void testQuotedIdentifiersAndValues() throws Exception {
    final String table = "ta\"ble;1";
    final String value = "a' OR '1'='1; -- \\ 中文\n";
    assertEquivalent(
        table,
        "event\"time",
        Arrays.asList("tag\"name", "select"),
        Collections.singletonList(
            new TableDeletionEntry(
                new DeletionPredicate(table, new TagPredicate.SegmentExactMatch(value, 1)),
                new TimeRange(10, 20))),
        Arrays.asList(device(table, value), device(table, "a"), device(table)));
  }

  /**
   * A later column tombstone must be rejected even after an earlier predicate matches every row.
   */
  @Test
  public void testRejectColumnSpecificDeletion() {
    final TableDeletionEntry columnDelete =
        new TableDeletionEntry(
            new DeletionPredicate(
                "table1", new TagPredicate.NOP(), Collections.singletonList("field1")),
            new TimeRange(10, 20));
    assertThrows(
        StatementExecutionException.class,
        () ->
            new TableWALDeleteConverter("table1", "time", TAGS)
                .toSql(
                    Arrays.asList(
                        entry(new TagPredicate.NOP(), Long.MIN_VALUE, Long.MAX_VALUE),
                        columnDelete)));
  }

  /**
   * Incompatible TAG indices/devices and missing TIME metadata must fail instead of widening SQL.
   */
  @Test
  public void testRejectIncompatibleSchema() {
    final List<TagPredicate> predicates =
        Arrays.asList(
            new TagPredicate.SegmentExactMatch("a", -1),
            new TagPredicate.SegmentExactMatch(null, 3),
            new TagPredicate.SegmentNotNull(3),
            new TagPredicate.FullExactMatch(device("other", "a")),
            new TagPredicate.FullExactMatch(device("table1", "a", "b", "c")),
            new TagPredicate.DeviceIn(Collections.singletonList(device("other", "a"))));
    for (final TagPredicate predicate : predicates) {
      assertThrows(
          StatementExecutionException.class,
          () ->
              new TableWALDeleteConverter("table1", "time", TAGS)
                  .toSql(Collections.singletonList(entry(predicate, 10, 20))));
    }
    assertThrows(
        StatementExecutionException.class,
        () ->
            new TableWALDeleteConverter("table1", null, TAGS)
                .toSql(Collections.singletonList(entry(new TagPredicate.NOP(), 10, 20))));
  }

  /** A newly introduced or unknown predicate must fail until its SQL semantics are implemented. */
  @Test
  public void testRejectUnsupportedPredicate() {
    assertThrows(
        StatementExecutionException.class,
        () ->
            new TableWALDeleteConverter("table1", "time", TAGS)
                .toSql(Collections.singletonList(entry(mock(TagPredicate.class), 10, 20))));
  }

  private static TableDeletionEntry entry(
      final TagPredicate predicate, final long start, final long end) {
    return new TableDeletionEntry(
        new DeletionPredicate("table1", predicate), new TimeRange(start, end));
  }

  private static IDeviceID device(final String... segments) {
    return new StringArrayDeviceID(segments);
  }

  private static List<IDeviceID> sampleDevices() {
    final List<IDeviceID> devices = new ArrayList<>();
    for (final String first : Arrays.asList(null, "a", "b")) {
      for (final String second : Arrays.asList(null, "x", "y")) {
        devices.add(device("table1", first, second));
      }
    }
    return devices;
  }

  private static void assertEquivalent(
      final String tableName,
      final String timeColumn,
      final List<String> tags,
      final List<TableDeletionEntry> original,
      final List<IDeviceID> devices)
      throws Exception {
    final String sql = new TableWALDeleteConverter(tableName, timeColumn, tags).toSql(original);
    final Delete delete = (Delete) new SqlParser().createStatement(sql, ZoneOffset.UTC, null);
    assertEquals(tableName, delete.getTable().getName().getSuffix());
    final TsTable table = new TsTable(tableName);
    table.addColumnSchema(new TimeColumnSchema(timeColumn, TSDataType.TIMESTAMP));
    for (final String tag : tags) {
      table.addColumnSchema(new TagColumnSchema(tag, TSDataType.STRING));
    }
    final List<TableDeletionEntry> parsed =
        AnalyzeUtils.parseExpressions2ModEntries(
            delete.getWhere().orElse(null),
            table,
            "target_db",
            new MPPQueryContext(new QueryId("1")));
    for (final IDeviceID device : devices) {
      for (final long time : TIMES) {
        assertEquals(
            sql + "; device=" + device + "; time=" + time,
            original.stream().anyMatch(entry -> entry.affects(device, time, time)),
            parsed.stream().anyMatch(entry -> entry.affects(device, time, time)));
      }
    }
  }
}
