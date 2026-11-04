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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
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
  private static final String[] FILL_DEVICES = {"a", "b", null};
  private static final double[][] FILL_ENDPOINTS = {{10, 30}, {100, 300}, {7, 11}};
  private static final DateTimeFormatter TIME_FORMAT =
      new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSortBufferSize(128 * 1024)
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
      statement.execute(
          "CREATE TABLE fill_values(device STRING TAG, v DOUBLE FIELD, marker INT32 FIELD)");
      for (int group = 0; group < FILL_DEVICES.length; group++) {
        String device = FILL_DEVICES[group] == null ? "NULL" : "'" + FILL_DEVICES[group] + "'";
        for (int i = 0; i < 5; i++) {
          String value =
              i == 1
                  ? Double.toString(FILL_ENDPOINTS[group][0])
                  : i == 3 ? Double.toString(FILL_ENDPOINTS[group][1]) : "NULL";
          statement.execute(
              "INSERT INTO fill_values(time,device,v,marker) VALUES ("
                  + (i * 60_000L + 1)
                  + ","
                  + device
                  + ","
                  + value
                  + ","
                  + i
                  + ")");
        }
      }
      statement.execute(
          "CREATE TABLE fill_keys(device STRING TAG, tie STRING TAG, t TIMESTAMP FIELD, marker INT32 FIELD)");
      statement.execute(
          "INSERT INTO fill_keys(time,device,tie,t,marker) VALUES"
              + " (1,'a','m',60000,1),(2,'a','z',60000,2),(3,'a','a',NULL,3),"
              + " (4,'b','a',NULL,4),(5,'b','z',NULL,5),"
              + " (6,NULL,'b',120000,6),(7,NULL,'a',NULL,7)");
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

  @Test
  public void testStatefulFillGroupOrderingAndValues() {
    for (String method : statefulFillMethods()) {
      for (String source :
          List.of("fill_values", "HOP(DATA => fill_values, SLIDE => 1m, SIZE => 1m)")) {
        assertTable(
            "SELECT time,device,v FROM "
                + source
                + " FILL METHOD "
                + method
                + " TIME_COLUMN 1 FILL_GROUP 2 ORDER BY device,time",
            new String[] {"time", "device", "v"},
            fillValueRows(method, false, false, false));
      }
      assertTable(
          "SELECT time,device,lag(v) OVER (PARTITION BY device ORDER BY time) AS v"
              + " FROM fill_values FILL METHOD "
              + method
              + " TIME_COLUMN 1 FILL_GROUP 2 ORDER BY device,time",
          new String[] {"time", "device", "v"},
          fillValueRows(method, true, false, false));
    }
  }

  @Test
  public void testStatefulFillAfterTimeBucketAggregation() {
    for (String bucket : List.of("date_bin", "date_bin_gapfill")) {
      for (String method : statefulFillMethods()) {
        if (method.contains("30s")) {
          continue;
        }
        assertTable(
            "SELECT "
                + bucket
                + "(1m,time) AS t,device,avg(v) AS v,count(marker) AS n"
                + " FROM fill_values WHERE time >= 0 AND time < 300000 AND marker <> 2"
                + " GROUP BY 1,2 FILL METHOD "
                + method
                + " TIME_COLUMN 1 FILL_GROUP 2 ORDER BY device,t",
            new String[] {"t", "device", "v", "n"},
            fillValueRows(method, false, true, bucket.equals("date_bin")));
      }
    }
  }

  @Test
  public void testNullableHelperPreservesOnlyItsOrderingPrefix() {
    for (String method : statefulFillMethods()) {
      String input =
          "SELECT * FROM (SELECT t,device,tie,marker FROM fill_keys"
              + " ORDER BY device,t,tie LIMIT 100) FILL METHOD "
              + method
              + " TIME_COLUMN 1 FILL_GROUP 2";
      String minute = TIME_FORMAT.format(Instant.ofEpochMilli(60_000));
      String twoMinutes = TIME_FORMAT.format(Instant.ofEpochMilli(120_000));
      String[] expected =
          method.equals("PREVIOUS")
              ? new String[] {
                minute + ",a,a,3,",
                minute + ",a,m,1,",
                minute + ",a,z,2,",
                "null,b,a,4,",
                "null,b,z,5,",
                twoMinutes + ",null,a,7,",
                twoMinutes + ",null,b,6,"
              }
              : new String[] {
                minute + ",a,m,1,",
                minute + ",a,z,2,",
                "null,a,a,3,",
                "null,b,a,4,",
                "null,b,z,5,",
                twoMinutes + ",null,b,6,",
                "null,null,a,7,"
              };
      assertTable(
          input + " ORDER BY device,t,tie",
          new String[] {"t", "device", "tie", "marker"},
          expected);
      List<String> prefixRows = new ArrayList<>();
      for (String row : expected) {
        String[] columns = row.split(",");
        prefixRows.add(columns[0] + "," + columns[1] + ",");
      }
      assertTable(
          "SELECT t,device FROM (" + input + ") ORDER BY device,t",
          new String[] {"t", "device"},
          prefixRows.toArray(new String[0]));
    }
  }

  private static List<String> statefulFillMethods() {
    return List.of(
        "PREVIOUS",
        "PREVIOUS TIME_BOUND 1h",
        "PREVIOUS TIME_BOUND 30s",
        "NEXT",
        "NEXT TIME_BOUND 1h",
        "NEXT TIME_BOUND 30s",
        "LINEAR");
  }

  private static String[] fillValueRows(
      String method, boolean window, boolean bucket, boolean omitGap) {
    List<String> result = new ArrayList<>();
    for (int group = 0; group < FILL_DEVICES.length; group++) {
      double low = FILL_ENDPOINTS[group][0];
      double high = FILL_ENDPOINTS[group][1];
      Double[] values;
      if (method.contains("30s")) {
        values =
            window
                ? new Double[] {null, null, low, null, high}
                : new Double[] {null, low, null, high, null};
      } else if (method.startsWith("PREVIOUS")) {
        values =
            window
                ? new Double[] {null, null, low, low, high}
                : new Double[] {null, low, low, high, high};
      } else if (method.startsWith("NEXT")) {
        values =
            window
                ? new Double[] {low, low, low, high, high}
                : new Double[] {low, low, high, high, null};
      } else {
        values =
            window
                ? new Double[] {null, null, low, (low + high) / 2, high}
                : new Double[] {null, low, (low + high) / 2, high, null};
      }
      for (int i = 0; i < values.length; i++) {
        if (omitGap && i == 2) {
          continue;
        }
        result.add(
            TIME_FORMAT.format(Instant.ofEpochMilli(i * 60_000L + (bucket ? 0 : 1)))
                + ","
                + FILL_DEVICES[group]
                + ","
                + values[i]
                + ","
                + (bucket ? "1," : ""));
      }
    }
    return result.toArray(new String[0]);
  }

  private static void assertTable(String sql, String[] header, String[] rows) {
    tableResultSetEqualTest(sql, header, rows, DATABASE);
  }
}
