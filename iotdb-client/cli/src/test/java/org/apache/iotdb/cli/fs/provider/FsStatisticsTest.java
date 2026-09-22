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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FsStatisticsTest {
  @Test
  public void statisticsFollowTsFileTypeSpecificValueAvailability() {
    List<FsColumn> columns =
        Arrays.asList(
            new FsColumn("i", "FIELD", "INT32"),
            new FsColumn("f", "FIELD", "FLOAT"),
            new FsColumn("d", "FIELD", "DATE"),
            new FsColumn("text", "FIELD", "TEXT"),
            new FsColumn("blob", "FIELD", "BLOB"));
    List<SqlRow> stats =
        FsStatistics.stats(
            "tree",
            "root.db.d",
            columns,
            SqlRow.list(
                SqlRow.of(
                    "time",
                    "1",
                    "i",
                    "2147483647",
                    "f",
                    "0.1",
                    "d",
                    "2020-01-02",
                    "text",
                    "z",
                    "blob",
                    "0x01"),
                SqlRow.of(
                    "time",
                    "2",
                    "i",
                    "2147483647",
                    "f",
                    "0.2",
                    "d",
                    "2020-01-01",
                    "text",
                    "a",
                    "blob",
                    "0x02")));
    assertEquals("4294967294", stats.get(0).get("sum"));
    assertEquals(Double.toString((double) 0.1f + (double) 0.2f), stats.get(1).get("sum"));
    assertEquals("2020-01-01", stats.get(2).get("min"));
    assertNull(stats.get(2).get("sum"));
    assertNull(stats.get(3).get("min"));
    assertNull(stats.get(3).get("max"));
    assertEquals("z", stats.get(3).get("first"));
    assertEquals("a", stats.get(3).get("last"));
    assertEquals("2", stats.get(4).get("non_null_count"));
    assertNull(stats.get(4).get("min"));
    assertNull(stats.get(4).get("first"));
    assertNull(stats.get(4).get("sum"));
  }

  @Test
  public void populatedZeroTagTableContainsOneEntityAndCountsOnlyFields() {
    List<FsColumn> columns =
        Arrays.asList(
            new FsColumn("time", "TIME", "TIMESTAMP"),
            new FsColumn("field", "FIELD", "STRING"),
            new FsColumn("attribute", "ATTRIBUTE", "STRING"));
    List<SqlRow> rows =
        FsStatistics.count(
            "table",
            "t",
            columns,
            SqlRow.list(
                SqlRow.of("time", "-10", "field", ""), SqlRow.of("time", "0", "field", null)));
    assertEquals(1, rows.size());
    assertEquals("1", rows.get(0).get("entity_count"));
    assertEquals("1", rows.get(0).get("non_null_count"));
    assertEquals("1", rows.get(0).get("null_count"));
    assertEquals("-10", rows.get(0).get("min_time"));
    assertEquals("0", rows.get(0).get("max_time"));
  }

  @Test
  public void numericStatisticsIncludeAverageAndMedian() {
    List<FsColumn> columns =
        Arrays.asList(
            new FsColumn("long_value", "FIELD", "INT64"),
            new FsColumn("double_value", "FIELD", "DOUBLE"));
    List<SqlRow> stats =
        FsStatistics.stats(
            "tree",
            "root.db.d",
            columns,
            SqlRow.list(
                SqlRow.of("time", "1", "long_value", "1", "double_value", "1.5"),
                SqlRow.of("time", "2", "long_value", "2", "double_value", null),
                SqlRow.of("time", "3", "long_value", "9", "double_value", "3.5"),
                SqlRow.of("time", "4", "long_value", "10", "double_value", "8.5")));

    assertNull(stats.get(0).get("sum"));
    assertEquals("5.5", stats.get(0).get("avg"));
    assertEquals("5.5", stats.get(0).get("median"));
    assertEquals("4.5", stats.get(1).get("avg"));
    assertEquals("3.5", stats.get(1).get("median"));
  }
}
