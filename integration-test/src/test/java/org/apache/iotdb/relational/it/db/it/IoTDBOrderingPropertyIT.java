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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.confignode.it.partition.IoTDBPartitionShuffleStrategyIT.SHUFFLE;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBOrderingPropertyIT {
  private static final String DATABASE = "ordering_review";
  private static final String TAGS = "tag1, tag2, tag3";
  private static final long[] TIMES = {1, 60_001, 120_001};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimePartitionInterval(60_000)
        .setDataPartitionAllocationStrategy(SHUFFLE)
        .setMaxTsBlockLineNumber(2);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE devices(tag1 STRING TAG, tag2 STRING TAG, tag3 STRING TAG, v DOUBLE FIELD)");
      for (String tag1 : List.of("A", "B")) {
        for (String tag2 : List.of("X", "Y")) {
          for (int i = 0; i < TIMES.length; i++) {
            statement.execute(
                "INSERT INTO devices(time,tag1,tag2,tag3,v) VALUES ("
                    + TIMES[i]
                    + ",'"
                    + tag1
                    + "','"
                    + tag2
                    + "','d',"
                    + (i + 1)
                    + ")");
          }
        }
      }
      // All rows share a device and a time partition, hence one DataRegion even under SHUFFLE.
      statement.execute("CREATE TABLE topk(device STRING TAG, bucket STRING FIELD, v INT32 FIELD)");
      statement.execute(
          "INSERT INTO topk(time,device,bucket,v) VALUES"
              + " (1,'solo','A',50),(2,'solo','B',1),(3,'solo','A',40),"
              + " (4,'solo','B',2),(5,'solo','A',30),(6,'solo','B',3)");
      statement.execute("CREATE TABLE nullable_devices(device STRING TAG, v DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO nullable_devices(time,device,v) VALUES (1,'a',1),(2,'z',2),(3,null,3)");
      statement.execute("FLUSH");
      statement.execute("CLEAR ATTRIBUTE CACHE");
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNonPrefixTvfGrouping() {
    for (String function :
        List.of(
            "CAPACITY(DATA => devices PARTITION BY (tag1,tag2) ORDER BY time, SIZE => 3, SLIDE => 1)",
            "SESSION(DATA => devices PARTITION BY (tag1,tag2) ORDER BY time, GAP => 10m)")) {
      int count = function.startsWith("CAPACITY") ? 12 : 6;
      assertTable(
          "SELECT tag2, max(tag1) AS mx, count(*) AS cnt FROM "
              + function
              + " GROUP BY tag2 ORDER BY tag2",
          new String[] {"tag2", "mx", "cnt"},
          new String[] {"X,B," + count + ",", "Y,B," + count + ","});
      assertTable(
          "SELECT tag1, max(tag2) AS mx, count(*) AS cnt FROM "
              + function
              + " GROUP BY tag1 ORDER BY tag1",
          new String[] {"tag1", "mx", "cnt"},
          new String[] {"A,Y," + count + ",", "B,Y," + count + ","});
    }
    assertTable(
        "SELECT tag2, tag3, max(tag1) AS mx, count(*) AS cnt FROM CAPACITY("
            + "DATA => devices PARTITION BY ("
            + TAGS
            + ") ORDER BY time, SIZE => 3, SLIDE => 1)"
            + " GROUP BY tag2, tag3 ORDER BY tag2, tag3",
        new String[] {"tag2", "tag3", "mx", "cnt"},
        new String[] {"X,d,B,12,", "Y,d,B,12,"});
    assertTable(
        "SELECT tag1, max(tag2) AS mx, count(*) AS cnt FROM CAPACITY("
            + "DATA => devices PARTITION BY (tag2,tag1) ORDER BY time, SIZE => 3, SLIDE => 1)"
            + " GROUP BY tag1 ORDER BY tag1",
        new String[] {"tag1", "mx", "cnt"},
        new String[] {"A,Y,12,", "B,Y,12,"});
  }

  @Test
  public void testWindowOrderingAndLimitAcrossBlocks() {
    String query =
        "SELECT tag1,tag2,time,lag(v) OVER (PARTITION BY "
            + TAGS
            + " ORDER BY time) AS prev FROM devices ORDER BY "
            + TAGS
            + ", time";
    List<String> expected = windowRows(false);
    assertTable(
        query, new String[] {"tag1", "tag2", "time", "prev"}, expected.toArray(new String[0]));
    assertTable(
        query + " LIMIT 5",
        new String[] {"tag1", "tag2", "time", "prev"},
        expected.subList(0, 5).toArray(new String[0]));
    assertTable(
        query + " DESC",
        new String[] {"tag1", "tag2", "time", "prev"},
        windowRows(true).toArray(new String[0]));
  }

  private static List<String> windowRows(boolean descending) {
    List<String> result = new ArrayList<>();
    for (String tag1 : List.of("A", "B")) {
      for (String tag2 : List.of("X", "Y")) {
        for (int n = 0; n < TIMES.length; n++) {
          int i = descending ? TIMES.length - n - 1 : n;
          result.add(
              tag1
                  + ","
                  + tag2
                  + ","
                  + Instant.ofEpochMilli(TIMES[i])
                  + ","
                  + (i == 0 ? "null" : i + ".0")
                  + ",");
        }
      }
    }
    return result;
  }

  @Test
  public void testPatternPartitionOrderingAcrossBlocks() {
    assertTable(
        "SELECT * FROM devices MATCH_RECOGNIZE (PARTITION BY "
            + TAGS
            + " ORDER BY time MEASURES A.v AS firstv ONE ROW PER MATCH PATTERN (A B+)"
            + " DEFINE B AS B.v > A.v) AS m ORDER BY tag1 DESC, tag2, tag3, firstv DESC",
        new String[] {"tag1", "tag2", "tag3", "firstv"},
        new String[] {"B,X,d,1.0,", "B,Y,d,1.0,", "A,X,d,1.0,", "A,Y,d,1.0,"});
  }

  @Test
  public void testTopKAscendingTimeOrderingRetainsAllRegions() {
    assertTopKTimeOrdering(false);
  }

  @Test
  public void testTopKDescendingTimeOrderingRetainsAllRegions() {
    assertTopKTimeOrdering(true);
  }

  private static void assertTopKTimeOrdering(boolean descending) {
    List<String> expected = new ArrayList<>();
    for (String tag1 : List.of("A", "B")) {
      for (String tag2 : List.of("X", "Y")) {
        for (int i = 0; i < 2; i++) {
          int timeIndex = descending ? i + 1 : i;
          int rank = descending ? 2 - i : i + 1;
          expected.add(
              tag1 + "," + tag2 + "," + Instant.ofEpochMilli(TIMES[timeIndex]) + "," + rank + ",");
        }
      }
    }
    assertTable(
        "SELECT tag1,tag2,time,rn FROM (SELECT *,row_number() OVER (PARTITION BY "
            + TAGS
            + " ORDER BY time "
            + (descending ? "DESC" : "ASC")
            + ") AS rn FROM devices) WHERE rn <= 2 ORDER BY "
            + TAGS
            + ", time",
        new String[] {"tag1", "tag2", "time", "rn"},
        expected.toArray(new String[0]));
  }

  @Test
  public void testSingleRegionTopKNeedsGlobalValueOrder() {
    assertTable(
        "SELECT time,bucket,v,rn FROM (SELECT *,row_number() OVER (PARTITION BY bucket"
            + " ORDER BY v,time) AS rn FROM topk) WHERE rn <= 2 ORDER BY v,time",
        new String[] {"time", "bucket", "v", "rn"},
        new String[] {
          "1970-01-01T00:00:00.002Z,B,1,1,", "1970-01-01T00:00:00.004Z,B,2,2,",
          "1970-01-01T00:00:00.005Z,A,30,1,", "1970-01-01T00:00:00.003Z,A,40,2,"
        });
    assertTable(
        "SELECT time,v,rn FROM (SELECT *,row_number() OVER (PARTITION BY device"
            + " ORDER BY v,time) AS rn FROM topk) WHERE rn <= 2 ORDER BY v,time",
        new String[] {"time", "v", "rn"},
        new String[] {"1970-01-01T00:00:00.002Z,1,1,", "1970-01-01T00:00:00.004Z,2,2,"});
  }

  @Test
  public void testFillKeepsRequiredDescendingTimeOrder() {
    String window = "(SELECT *,count(v) OVER (ORDER BY " + TAGS + ") AS c FROM devices)";
    List<String> expected = new ArrayList<>();
    int group = 0;
    for (String tag1 : List.of("A", "B")) {
      for (String tag2 : List.of("X", "Y")) {
        group++;
        for (int i = TIMES.length - 1; i >= 0; i--) {
          expected.add(
              tag1 + "," + tag2 + "," + Instant.ofEpochMilli(TIMES[i]) + "," + group * 3 + ",");
        }
      }
    }
    for (String source : List.of(window, "HOP(DATA => " + window + ", SLIDE => 1m, SIZE => 1m)")) {
      assertTable(
          "SELECT tag1,tag2,time,c FROM "
              + source
              + " FILL METHOD CONSTANT 0 ORDER BY "
              + TAGS
              + ", time DESC",
          new String[] {"tag1", "tag2", "time", "c"},
          expected.toArray(new String[0]));
    }
  }

  @Test
  public void testNullableDeviceOrdering() {
    String query =
        "SELECT device,time,count(*) OVER (PARTITION BY device ORDER BY time) AS c"
            + " FROM nullable_devices ORDER BY device ";
    assertTable(
        query + "DESC NULLS LAST, time",
        new String[] {"device", "time", "c"},
        new String[] {
          "z,1970-01-01T00:00:00.002Z,1,",
          "a,1970-01-01T00:00:00.001Z,1,",
          "null,1970-01-01T00:00:00.003Z,1,"
        });
    assertTable(
        query + "ASC NULLS FIRST, time",
        new String[] {"device", "time", "c"},
        new String[] {
          "null,1970-01-01T00:00:00.003Z,1,",
          "a,1970-01-01T00:00:00.001Z,1,",
          "z,1970-01-01T00:00:00.002Z,1,"
        });
  }

  @Test
  public void testFillChangingNullableSortKeyRequiresSorting() {
    assertTable(
        "SELECT device,time,count(*) OVER (PARTITION BY device ORDER BY time) AS c"
            + " FROM nullable_devices FILL METHOD CONSTANT 'm' ORDER BY device,time",
        new String[] {"device", "time", "c"},
        new String[] {
          "a,1970-01-01T00:00:00.001Z,1,",
          "m,1970-01-01T00:00:00.003Z,1,",
          "z,1970-01-01T00:00:00.002Z,1,"
        });
  }

  private static void assertTable(String sql, String[] header, String[] rows) {
    tableResultSetEqualTest(sql, header, rows, DATABASE);
  }
}
