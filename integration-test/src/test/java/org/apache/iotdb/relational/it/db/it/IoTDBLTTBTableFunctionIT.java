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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLTTBTableFunctionIT {

  private static final String DATABASE_NAME = "lttb";

  @BeforeClass
  public static void setUp() throws Exception {
    // Four data region groups per database, so a partition can be split across regions and a
    // per-fragment LTTB or an unordered merge would produce visibly wrong results.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(4);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE metrics(device STRING TAG, s1 DOUBLE FIELD, s2 DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO metrics(time, device, s1, s2) VALUES "
              + "(0, 'd1', 0.0, 0.0),"
              + "(1, 'd1', 1.0, 0.0),"
              + "(2, 'd1', 8.0, null),"
              + "(3, 'd1', 2.0, 9.0),"
              + "(4, 'd1', 0.0, 1.0),"
              + "(5, 'd1', 0.0, 0.0),"
              + "(0, 'd2', 0.0, 0.0),"
              + "(1, 'd2', null, null)");
      statement.execute(
          "CREATE TABLE sparse(device STRING TAG, value DOUBLE FIELD, marker INT32 FIELD)");
      statement.execute(
          "INSERT INTO sparse(time, device, value, marker) VALUES "
              + "(0, 'd1', 0.0, 1), (1, 'd1', 1.0, 1), (2, 'd1', 2.0, 1),"
              + "(3, 'd1', null, 1), (4, 'd1', null, 1), (5, 'd1', null, 1),"
              + "(6, 'd1', 0.0, 1), (7, 'd1', 0.0, 1), (8, 'd1', 0.0, 1)");
      // Two devices share one site tag; both write t = 0 so the partition has a duplicate
      // timestamp.
      statement.execute("CREATE TABLE shared(site STRING TAG, device STRING TAG, s1 DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO shared(time, site, device, s1) VALUES "
              + "(0, 'a', 'x1', 1.0), (1, 'a', 'x1', 2.0), (0, 'a', 'x2', 3.0), (1, 'a', 'x2', 4.0)");
      statement.execute("CREATE TABLE integral_values(device STRING TAG, value INT64 FIELD)");
      long baseline = 1L << 60;
      long[] deltas = {0, 1, 8, 2, 0, 0};
      for (int i = 0; i < deltas.length; i++) {
        statement.execute(
            "INSERT INTO integral_values(time, device, value) VALUES ("
                + i
                + ", 'd1', "
                + (baseline + deltas[i])
                + ")");
      }
      // Two partitions ('p' and 'q'), eight devices each, interleaved timestamps: the devices of
      // one
      // partition carry disjoint time ranges, so the partition is ordered once the rows are merged
      // and a fragment-local sample would return one sample per region.
      statement.execute(
          "CREATE TABLE regions(part STRING TAG, device STRING TAG, s1 DOUBLE FIELD)");
      for (int device = 0; device < 8; device++) {
        for (int row = 0; row < 4; row++) {
          long time = device * 4L + row;
          statement.execute(
              "INSERT INTO regions(time, part, device, s1) VALUES ("
                  + time
                  + ", 'p', 'p"
                  + device
                  + "', "
                  + (time % 7)
                  + ".0)");
          statement.execute(
              "INSERT INTO regions(time, part, device, s1) VALUES ("
                  + time
                  + ", 'q', 'q"
                  + device
                  + "', "
                  + (time % 5)
                  + ".0)");
        }
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      fail("insertData failed: " + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTargetCountPreservesEndpointsAndAlignsColumns() {
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, "
            + "TIMECOL => 'time', N => 3) ORDER BY device, s1_time",
        new String[] {"window_index", "device", "s1_time", "s1", "s2_time", "s2"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,0.0,1970-01-01T00:00:00.000Z,0.0,",
          "0,d1,1970-01-01T00:00:00.002Z,8.0,1970-01-01T00:00:00.003Z,9.0,",
          "0,d1,1970-01-01T00:00:00.005Z,0.0,1970-01-01T00:00:00.005Z,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testCountWindowAndTimeWindowOutput() {
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', SIZE => 2) "
            + "ORDER BY window_index",
        new String[] {"window_index", "device", "s1_time", "s1"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,0.0,",
          "0,d1,1970-01-01T00:00:00.001Z,1.0,",
          "1,d1,1970-01-01T00:00:00.002Z,8.0,",
          "2,d1,1970-01-01T00:00:00.004Z,0.0,",
          "2,d1,1970-01-01T00:00:00.005Z,0.0,"
        },
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT * FROM LTTB(DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', SIZE => 2ms, "
            + "ORIGIN => 1970-01-01T00:00:00.000+00:00) ORDER BY window_start",
        new String[] {"window_start", "window_end", "device", "s1_time", "s1"},
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.002Z,d1,1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.002Z,d1,1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,1970-01-01T00:00:00.004Z,d1,1970-01-01T00:00:00.002Z,8.0,",
          "1970-01-01T00:00:00.004Z,1970-01-01T00:00:00.006Z,d1,1970-01-01T00:00:00.004Z,0.0,",
          "1970-01-01T00:00:00.004Z,1970-01-01T00:00:00.006Z,d1,1970-01-01T00:00:00.005Z,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testDuplicateTimestampsAreRejectedWith701() {
    // the same six d1 rows twice: (0, 1, 2, 3, 4, 5) each occurs twice in one partition
    String duplicated =
        "SELECT * FROM LTTB(DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1' "
            + "UNION ALL SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', ";
    String expected =
        "701: LTTB does not support duplicate timestamps, but timestamp 0 appears more than once"
            + " in the same partition.";

    tableAssertTestFail(duplicated + "N => 3)", expected, DATABASE_NAME);
    tableAssertTestFail(duplicated + "SIZE => 2)", expected, DATABASE_NAME);
    tableAssertTestFail(
        duplicated + "SIZE => 2ms, ORIGIN => 1970-01-01T00:00:00.000+00:00)",
        expected,
        DATABASE_NAME);
  }

  @Test
  public void testDuplicateTimestampsInDifferentPartitionsRemainValid() {
    // each partition is unique on its own, so the query must succeed and keep both rows
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(DATA => (SELECT time, device, s1 FROM metrics "
            + "WHERE time = 0 AND (device = 'd1' OR device = 'd2')) "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', N => 3) "
            + "ORDER BY device",
        new String[] {"window_index", "device", "s1_time", "s1"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,0.0,", "0,d2,1970-01-01T00:00:00.000Z,0.0,",
        },
        DATABASE_NAME);
  }

  @Test
  public void testWindowSkipsNullBucketForLookahead() {
    String data =
        "DATA => (SELECT time, device, value FROM sparse) PARTITION BY device ORDER BY time, TIMECOL => 'time', ";
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(" + data + "SIZE => 3) ORDER BY window_index",
        new String[] {"window_index", "device", "value_time", "value"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,0.0,",
          "0,d1,1970-01-01T00:00:00.002Z,2.0,",
          "2,d1,1970-01-01T00:00:00.006Z,0.0,",
          "2,d1,1970-01-01T00:00:00.008Z,0.0,"
        },
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(" + data + "SIZE => 3ms) ORDER BY window_start",
        new String[] {"window_start", "window_end", "device", "value_time", "value"},
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.003Z,d1,1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.003Z,d1,1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.006Z,1970-01-01T00:00:00.009Z,d1,1970-01-01T00:00:00.006Z,0.0,",
          "1970-01-01T00:00:00.006Z,1970-01-01T00:00:00.009Z,d1,1970-01-01T00:00:00.008Z,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testNonFiniteValuesAreKeptInShortSeries() {
    // three non-NULL input points whose values are NaN/Infinity for N => 3
    String nonFinite =
        "SELECT * FROM LTTB(DATA => (SELECT time, device, s1 / 0.0 AS s1 FROM metrics "
            + "WHERE device = 'd1' AND time <= 2) "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', ";

    tableResultSetEqualTest(
        nonFinite + "N => 3) ORDER BY s1_time",
        new String[] {"window_index", "device", "s1_time", "s1"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,NaN,",
          "0,d1,1970-01-01T00:00:00.001Z,Infinity,",
          "0,d1,1970-01-01T00:00:00.002Z,Infinity,",
        },
        DATABASE_NAME);
  }

  @Test
  public void testInt64SamplingPreservesSmallPeaksAtLargeBaselines() {
    String prefix =
        "SELECT * FROM LTTB(DATA => integral_values PARTITION BY device ORDER BY time, "
            + "TIMECOL => 'time', ";
    tableResultSetEqualTest(
        prefix + "N => 3) ORDER BY value_time",
        new String[] {"window_index", "device", "value_time", "value"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,1152921504606846976,",
          "0,d1,1970-01-01T00:00:00.002Z,1152921504606846984,",
          "0,d1,1970-01-01T00:00:00.005Z,1152921504606846976,"
        },
        DATABASE_NAME);
    tableResultSetEqualTest(
        prefix + "SIZE => 2) ORDER BY window_index",
        new String[] {"window_index", "device", "value_time", "value"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,1152921504606846976,",
          "0,d1,1970-01-01T00:00:00.001Z,1152921504606846977,",
          "1,d1,1970-01-01T00:00:00.002Z,1152921504606846984,",
          "2,d1,1970-01-01T00:00:00.004Z,1152921504606846976,",
          "2,d1,1970-01-01T00:00:00.005Z,1152921504606846976,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testSlideSmallerThanSizeKeepsOneAnchorChain() {
    // SIZE => 2, SLIDE => 1: overlapping count windows. F(t0) is reported by the first window and
    // L(t5) by the last one, and the windows in between continue the same anchor chain.
    String data =
        "DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', ";
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(" + data + "SIZE => 2, SLIDE => 1) ORDER BY window_index, s1_time",
        new String[] {"window_index", "device", "s1_time", "s1"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,0.0,",
          "0,d1,1970-01-01T00:00:00.001Z,1.0,",
          "1,d1,1970-01-01T00:00:00.002Z,8.0,",
          "2,d1,1970-01-01T00:00:00.003Z,2.0,",
          "3,d1,1970-01-01T00:00:00.003Z,2.0,",
          "4,d1,1970-01-01T00:00:00.004Z,0.0,",
          "5,d1,1970-01-01T00:00:00.005Z,0.0,"
        },
        DATABASE_NAME);
    // The time-window form with SLIDE < SIZE starts a window one slide before the first point.
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(" + data + "SIZE => 2ms, SLIDE => 1ms) ORDER BY window_start, s1_time",
        new String[] {"window_start", "window_end", "device", "s1_time", "s1"},
        new String[] {
          "1969-12-31T23:59:59.999Z,1970-01-01T00:00:00.001Z,d1,1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.002Z,d1,1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.003Z,d1,1970-01-01T00:00:00.002Z,8.0,",
          "1970-01-01T00:00:00.002Z,1970-01-01T00:00:00.004Z,d1,1970-01-01T00:00:00.003Z,2.0,",
          "1970-01-01T00:00:00.003Z,1970-01-01T00:00:00.005Z,d1,1970-01-01T00:00:00.003Z,2.0,",
          "1970-01-01T00:00:00.004Z,1970-01-01T00:00:00.006Z,d1,1970-01-01T00:00:00.004Z,0.0,",
          "1970-01-01T00:00:00.005Z,1970-01-01T00:00:00.007Z,d1,1970-01-01T00:00:00.005Z,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testSlideLargerThanSizeSkipsTheGap() {
    // SIZE => 1, SLIDE => 2 keeps only the windows that receive a row: [0,1), [2,3), [4,5).
    String data =
        "DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', ";
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(" + data + "SIZE => 1, SLIDE => 2) ORDER BY window_index, s1_time",
        new String[] {"window_index", "device", "s1_time", "s1"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,0.0,",
          "1,d1,1970-01-01T00:00:00.002Z,8.0,",
          "2,d1,1970-01-01T00:00:00.004Z,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testTimeWindowWithUnalignedOriginShiftsBoundaries() {
    String data =
        "DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', ";
    tableResultSetEqualTest(
        "SELECT * FROM LTTB("
            + data
            + "SIZE => 2ms, ORIGIN => 1970-01-01T00:00:00.001+00:00) "
            + "ORDER BY window_start, s1_time",
        new String[] {"window_start", "window_end", "device", "s1_time", "s1"},
        new String[] {
          "1969-12-31T23:59:59.999Z,1970-01-01T00:00:00.001Z,d1,1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.003Z,d1,1970-01-01T00:00:00.002Z,8.0,",
          "1970-01-01T00:00:00.003Z,1970-01-01T00:00:00.005Z,d1,1970-01-01T00:00:00.003Z,2.0,",
          "1970-01-01T00:00:00.005Z,1970-01-01T00:00:00.007Z,d1,1970-01-01T00:00:00.005Z,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testInvalidTimeColumnIsRejected() {
    tableAssertTestFail(
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, "
            + "TIMECOL => 's1', N => 3)",
        "701: ",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, "
            + "TIMECOL => 'missing', N => 3)",
        "701: ",
        DATABASE_NAME);
  }

  @Test
  public void testDuplicateTimestampsAcrossDevicesInOnePartitionAreRejected() {
    // Both devices are tagged 'shared', so PARTITION BY site puts t = 0 of each device into one
    // partition and the duplicate check has to reject it.
    tableAssertTestFail(
        "SELECT * FROM LTTB(DATA => (SELECT time, site, s1 FROM shared ORDER BY time) "
            + "PARTITION BY site ORDER BY time, TIMECOL => 'time', N => 3)",
        "701: LTTB does not support duplicate timestamps, but timestamp 0 appears more than once"
            + " in the same partition.",
        DATABASE_NAME);
  }

  /**
   * A partitioned GroupNode does not make a partition complete within one branch, so LTTB has to
   * run once over the partitions merged in order. With four data region groups and a partition
   * whose devices carry disjoint time ranges, a per-region sample or an unordered merge changes the
   * result; {@code N => 1000} returns every eligible point, so all three inputs must agree.
   */
  @Test
  public void testNonDirectScanInputsMatchTheDirectScan() throws SQLException {
    String direct =
        "SELECT window_index, part, s1_time, s1 FROM LTTB("
            + "DATA => regions PARTITION BY part ORDER BY time, TIMECOL => 'time', N => 1000) "
            + "ORDER BY part, s1_time";
    String windowFunctionInput =
        "SELECT window_index, part, s1_time, s1 FROM LTTB("
            + "DATA => (SELECT time, part, s1, "
            + "row_number() OVER (PARTITION BY part, device ORDER BY time) AS rn FROM regions) "
            + "PARTITION BY part ORDER BY time, TIMECOL => 'time', N => 1000) "
            + "ORDER BY part, s1_time";
    String sessionInput =
        "SELECT window_index, part, s1_time, s1 FROM LTTB("
            + "DATA => (SELECT time, part, s1 FROM SESSION("
            + "DATA => regions PARTITION BY (part, device) ORDER BY time, TIMECOL => 'time', "
            + "GAP => 1m)) PARTITION BY part ORDER BY time, TIMECOL => 'time', N => 1000) "
            + "ORDER BY part, s1_time";

    List<String> expected = queryRows(direct);
    // 8 devices x 4 rows per partition, 2 partitions.
    assertEquals(64, expected.size());
    assertEquals(expected, queryRows(windowFunctionInput));
    assertEquals(expected, queryRows(sessionInput));
  }

  private static List<String> queryRows(String sql) throws SQLException {
    List<String> rows = new ArrayList<>();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
          StringBuilder row = new StringBuilder();
          for (int i = 1; i <= columnCount; i++) {
            row.append(resultSet.getString(i)).append('|');
          }
          rows.add(row.toString());
        }
      }
    }
    return rows;
  }

  @Test
  public void testInvalidParameterCombinations() {
    String prefix =
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, TIMECOL => 'time', ";
    tableAssertTestFail(
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, TIMECOL => 'time')",
        "701: Exactly one of the N and SIZE arguments must be specified for LTTB.",
        DATABASE_NAME);
    tableAssertTestFail(
        prefix + "N => 3, SIZE => 2)",
        "701: Exactly one of the N and SIZE arguments must be specified for LTTB.",
        DATABASE_NAME);
    tableAssertTestFail(
        prefix + "N => 2)", "701: The N argument of LTTB must be at least 3.", DATABASE_NAME);
    tableAssertTestFail(
        prefix + "N => 3, SLIDE => 2)",
        "701: The N argument of LTTB cannot be combined with the SLIDE or ORIGIN arguments.",
        DATABASE_NAME);
    tableAssertTestFail(
        prefix + "SIZE => 2, ORIGIN => 1970-01-01T00:00:00.000+00:00)",
        "701: The ORIGIN argument is only supported in time window mode.",
        DATABASE_NAME);
  }
}
