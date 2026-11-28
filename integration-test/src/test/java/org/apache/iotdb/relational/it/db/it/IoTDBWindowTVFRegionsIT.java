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
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

/**
 * Multi-region coverage for nested set-semantic TVFs and window functions whose PARTITION BY
 * reorders or prefixes the child's partition key, and for window functions over a hash aggregation.
 * Every device is a single row of {@code t}, so one outer partition spans all DataRegions and any
 * accidental per-region split changes the window count.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWindowTVFRegionsIT {

  private static final String DATABASE_NAME = "test";
  private static final int DEVICE_NUM = 40;
  private static final int HASH_AGGREGATION_ROW_NUM = 6000;
  private static final int SUBSET_DEVICE_NUM = 40;
  private static final int SUBSET_ROWS_PER_DEVICE = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    // The single-instance plans here hold several ~8MB fragment instances at once, which exceeds
    // the operator memory pool under the default proportions.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataNodeMemoryProportion("2:4:1:1:1:1")
        .setQueryMemoryProportion("1:100:100:10:400:200:100:50")
        .setSortBufferSize(1024 * 1024)
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setDefaultDataRegionGroupNumPerDatabase(4);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
    insertHashAggregationData();
    insertStreamingAggregationData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE t(a STRING TAG, b STRING TAG, c STRING TAG, s1 DOUBLE FIELD)");
      for (int i = 0; i < DEVICE_NUM; i++) {
        statement.execute(
            String.format(
                "INSERT INTO t(time, a, b, c, s1) VALUES (%d, 'A', 'B', 'c%02d', %d.0)",
                i + 1, i, i + 1));
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private static void insertHashAggregationData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE t2(d STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD, s3 INT32 FIELD)");
      for (int start = 0; start < HASH_AGGREGATION_ROW_NUM; start += 1000) {
        StringBuilder sql = new StringBuilder("INSERT INTO t2(time, d, s1, s2, s3) VALUES ");
        for (int row = start; row < start + 1000; row++) {
          // Pseudo-random s1 in {0, 1, 2} and s2 in {0..4}; s3 is unique.
          int s1 = (int) ((((long) row * 0x9E3779B1L) & 0xFFFFFFFFL) >> 20) % 3;
          int s2 = (int) ((((long) row * 0x85EBCA6BL) & 0xFFFFFFFFL) >> 20) % 5;
          if (row > start) {
            sql.append(", ");
          }
          sql.append('(')
              .append(row + 1)
              .append(", 'd1', ")
              .append(s1)
              .append(", ")
              .append(s2)
              .append(", ")
              .append(row)
              .append(')');
        }
        statement.execute(sql.toString());
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
      fail(String.valueOf(e.getMessage()));
    }
  }

  private static void insertStreamingAggregationData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      // 40 devices all share d = 'A', so (d, s1) groups span regions.
      statement.execute("CREATE TABLE t3(d STRING TAG, e STRING TAG, s1 INT32 FIELD)");
      for (int dev = 0; dev < SUBSET_DEVICE_NUM; dev++) {
        StringBuilder sql = new StringBuilder("INSERT INTO t3(time, d, e, s1) VALUES ");
        for (int i = 0; i < SUBSET_ROWS_PER_DEVICE; i++) {
          if (i > 0) {
            sql.append(", ");
          }
          sql.append('(')
              .append(i * 100 + dev + 1)
              .append(", 'A', 'e")
              .append(dev)
              .append("', ")
              .append((i + dev) % 3)
              .append(')');
        }
        statement.execute(sql.toString());
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
      fail(String.valueOf(e.getMessage()));
    }
  }

  @Test
  public void testStreamingAggregationSubsetPreGroupedSessionKeepsPartitionsWhole() {
    // The aggregation streams on the pre-grouped tag d only, a strict subset of (d, s1). The
    // final aggregation merges the regions before the session, so each partition stays whole:
    // three rows in, three single-row windows, not three windows per region.
    tableResultSetEqualTest(
        "SELECT d, s1, window_start, window_end, count(*) AS cnt FROM SESSION(DATA => ("
            + "    SELECT d, s1, max(time) AS time FROM t3 GROUP BY d, s1"
            + ") PARTITION BY (d, s1) ORDER BY time, GAP => 1000000ms) "
            + "GROUP BY d, s1, window_start, window_end ORDER BY window_start",
        new String[] {"d", "s1", "window_start", "window_end", "cnt"},
        new String[] {
          "A,1,1970-01-01T00:00:00.938Z,1970-01-01T00:00:00.938Z,1,",
          "A,2,1970-01-01T00:00:00.939Z,1970-01-01T00:00:00.939Z,1,",
          "A,0,1970-01-01T00:00:00.940Z,1970-01-01T00:00:00.940Z,1,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testStreamingAggregationSubsetPreGroupedWindowKeepsPartitionsWhole() {
    // Same shape for a plain window function.
    tableResultSetEqualTest(
        "SELECT count(*) AS rows, max(cnt) AS mx, min(cnt) AS mn FROM ("
            + "    SELECT count(s1) OVER (PARTITION BY d, s1) AS cnt "
            + "    FROM (SELECT d, s1, max(time) AS time FROM t3 GROUP BY d, s1))",
        new String[] {"rows", "mx", "mn"},
        new String[] {"3,1,1,"},
        DATABASE_NAME);
  }

  @Test
  public void testSameOrderPrefixPartitionStaysSingleInstance() {
    // The outer PARTITION BY (a, b) is a strict prefix of the inner one. One outer partition still
    // spans every DataRegion, so the outer SESSION must stay single-instance.
    tableResultSetEqualTest(
        "SELECT window_start, window_end, count(*) AS cnt FROM SESSION(DATA => ("
            + "    SELECT window_start AS inner_start, window_end AS inner_end, time, a, b, c, s1 "
            + "    FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms)"
            + ") PARTITION BY (a, b) ORDER BY time, GAP => 100ms) "
            + "GROUP BY window_start, window_end ORDER BY window_start",
        new String[] {"window_start", "window_end", "cnt"},
        new String[] {"1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.040Z,40,"},
        DATABASE_NAME);
  }

  @Test
  public void testSameOrderPrefixWindowPartitionStaysSingleInstance() {
    tableResultSetEqualTest(
        "SELECT max(cnt) AS max_cnt, min(cnt) AS min_cnt FROM ("
            + "    SELECT count(s1) OVER (PARTITION BY a, b) AS cnt "
            + "    FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms))",
        new String[] {"max_cnt", "min_cnt"},
        new String[] {"40,40,"},
        DATABASE_NAME);
  }

  @Test
  public void testHashAggregationWindowKeepsPartitionsWhole() {
    // A hash aggregation's output is not clustered by its grouping keys. The window on top must
    // see every (s1, s3) row of a partition together, not blocks with mixed s1 values.
    tableResultSetEqualTest(
        "SELECT s1, cnt, count(*) AS n FROM ("
            + "    SELECT s1, count(*) OVER (PARTITION BY s1) AS cnt "
            + "    FROM (SELECT s1, s3 FROM t2 GROUP BY s1, s3)) "
            + "GROUP BY s1, cnt ORDER BY s1, cnt",
        new String[] {"s1", "cnt", "n"},
        new String[] {"0,1980,1980,", "1,2013,2013,", "2,2007,2007,"},
        DATABASE_NAME);
  }

  @Test
  public void testHashAggregationSessionKeepsPartitionsWhole() {
    tableResultSetEqualTest(
        "SELECT s1, window_start, window_end, count(*) AS cnt FROM SESSION(DATA => ("
            + "    SELECT s1, s3, max(time) AS time FROM t2 GROUP BY s1, s3"
            + ") PARTITION BY (s1) ORDER BY time, GAP => 100000ms) "
            + "GROUP BY s1, window_start, window_end ORDER BY s1, window_start",
        new String[] {"s1", "window_start", "window_end", "cnt"},
        new String[] {
          "0,1970-01-01T00:00:00.001Z,1970-01-01T00:00:05.997Z,1980,",
          "1,1970-01-01T00:00:00.005Z,1970-01-01T00:00:05.999Z,2013,",
          "2,1970-01-01T00:00:00.002Z,1970-01-01T00:00:06.000Z,2007,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testWindowOverRowNumberKeepsPartitionsWhole() {
    // Distribution drops the sort below a row number, so the window must re-sort. s3 is unique,
    // so each (s1, s3) partition is one row; a per-region split fragments each s1.
    tableResultSetEqualTest(
        "SELECT s1, cnt, count(*) AS n, max(rn) AS mrn FROM ("
            + "    SELECT s1, rn, count(*) OVER (PARTITION BY s1) AS cnt "
            + "    FROM (SELECT s1, s3, row_number() OVER (PARTITION BY s1, s3) AS rn FROM t2)) "
            + "GROUP BY s1, cnt ORDER BY s1, cnt",
        new String[] {"s1", "cnt", "n", "mrn"},
        new String[] {"0,1980,1980,1,", "1,2013,2013,1,", "2,2007,2007,1,"},
        DATABASE_NAME);
  }

  @Test
  public void testWindowOverTopKRankingKeepsPartitionsWhole() {
    // Same shape through the ranking node: rn <= 1 keeps every row because s3 is unique.
    tableResultSetEqualTest(
        "SELECT s1, cnt, count(*) AS n, count(s2) AS c2, count(s3) AS c3 FROM ("
            + "    SELECT s1, s2, s3, count(*) OVER (PARTITION BY s1) AS cnt "
            + "    FROM (SELECT s1, s2, s3 FROM ("
            + "        SELECT s1, s2, s3, row_number() OVER (PARTITION BY s1, s3 ORDER BY s2) AS rn "
            + "        FROM t2) WHERE rn <= 1)) "
            + "GROUP BY s1, cnt ORDER BY s1, cnt",
        new String[] {"s1", "cnt", "n", "c2", "c3"},
        new String[] {"0,1980,1980,1980,1980,", "1,2013,2013,2013,2013,", "2,2007,2007,2007,2007,"},
        DATABASE_NAME);
  }

  @Test
  public void testSameOrderPrefixTopKRankingStaysSingleInstance() {
    // row_number() over (a, b) on top of row_number() over (a, b, c): the outer partition is the
    // whole table, so the global top 2 is two rows. A per-region split returns 2 rows per region.
    tableResultSetEqualTest(
        "SELECT time, c, rn2 FROM ("
            + "    SELECT *, row_number() OVER (PARTITION BY a, b ORDER BY time) AS rn2 "
            + "    FROM (SELECT time, a, b, c, s1, "
            + "                 row_number() OVER (PARTITION BY a, b, c ORDER BY time) AS rn1 "
            + "          FROM t)) "
            + "WHERE rn2 <= 2 ORDER BY time",
        new String[] {"time", "c", "rn2"},
        new String[] {"1970-01-01T00:00:00.001Z,c00,1,", "1970-01-01T00:00:00.002Z,c01,2,"},
        DATABASE_NAME);
  }

  @Test
  public void testSameOrderPrefixMatchRecognizeStaysSingleInstance() {
    // MATCH_RECOGNIZE partitioned by (a, b) over a SESSION partitioned by (a, b, c): the outer
    // partition is the whole table, so the pattern matches one run of 40 rows.
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT time, a, b, c, s1 FROM SESSION(DATA => t "
            + "    PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms)) "
            + "MATCH_RECOGNIZE (PARTITION BY a, b ORDER BY time "
            + "    MEASURES RPR_FIRST(A.time) AS first_t, RPR_LAST(A.time) AS last_t, "
            + "             COUNT(A.s1) AS cnt "
            + "    ONE ROW PER MATCH PATTERN (A+) DEFINE A AS true) "
            + "ORDER BY first_t",
        new String[] {"a", "b", "first_t", "last_t", "cnt"},
        new String[] {"A,B,1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.040Z,40,"},
        DATABASE_NAME);
  }

  @Test
  public void testReorderedSubsetPartitionStaysSingleInstance() {
    // The outer PARTITION BY (b, a) is a strict subset of the inner one in a different order. One
    // outer partition spans every DataRegion, so the outer SESSION must stay single-instance:
    // exactly one window covering all 40 devices. A per-region split would return one window per
    // region, each counting only that region's devices.
    tableResultSetEqualTest(
        "SELECT window_start, window_end, count(*) AS cnt FROM SESSION(DATA => ("
            + "    SELECT window_start AS inner_start, window_end AS inner_end, time, a, b, c, s1 "
            + "    FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms)"
            + ") PARTITION BY (b, a) ORDER BY time, GAP => 100ms) "
            + "GROUP BY window_start, window_end ORDER BY window_start",
        new String[] {"window_start", "window_end", "cnt"},
        new String[] {"1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.040Z,40,"},
        DATABASE_NAME);
  }

  @Test
  public void testReorderedSubsetWindowPartitionStaysSingleInstance() {
    // Same shape for a plain window function: count(s1) over (b, a) must see rows from every
    // region, so every row gets the full-device count.
    tableResultSetEqualTest(
        "SELECT max(cnt) AS max_cnt, min(cnt) AS min_cnt FROM ("
            + "    SELECT count(s1) OVER (PARTITION BY b, a) AS cnt "
            + "    FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms))",
        new String[] {"max_cnt", "min_cnt"},
        new String[] {"40,40,"},
        DATABASE_NAME);
  }

  @Test
  public void testPermutedExactPartitionColumnsStayParallel() {
    // The intended case: the outer PARTITION BY lists exactly the inner columns in a different
    // order, so each device is its own outer partition and both levels run per region. 40 devices
    // yield 40 windows of one row each.
    tableResultSetEqualTest(
        "SELECT count(*) AS windows, max(cnt) AS max_cnt, min(cnt) AS min_cnt FROM ("
            + "    SELECT window_start, window_end, count(*) AS cnt FROM SESSION(DATA => ("
            + "        SELECT window_start AS inner_start, window_end AS inner_end, time, a, b, c, s1 "
            + "        FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms)"
            + "    ) PARTITION BY (c, b, a) ORDER BY time, GAP => 100ms) "
            + "    GROUP BY window_start, window_end)",
        new String[] {"windows", "max_cnt", "min_cnt"},
        new String[] {"40,1,1,"},
        DATABASE_NAME);
  }
}
