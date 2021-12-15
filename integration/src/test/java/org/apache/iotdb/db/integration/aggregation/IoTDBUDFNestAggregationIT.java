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

package org.apache.iotdb.db.integration.aggregation;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBUDFNestAggregationIT {

  private static final double E = 1e-6;
  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "SET STORAGE GROUP TO root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
      };
  private static String[] dataSet2 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)"
      };
  private static String[] dataSet3 =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "insert into root.sg.d1(timestamp,s1) values(5,5)",
        "insert into root.sg.d1(timestamp,s1) values(12,12)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(15,15)",
        "insert into root.sg.d1(timestamp,s1) values(25,25)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(1,111)",
        "insert into root.sg.d1(timestamp,s1) values(20,200)",
        "flush"
      };

  private static final String[] dataSet4 =
      new String[] {
        "SET STORAGE GROUP TO root.sg1",
        "SET STORAGE GROUP TO root.sg2",
        "CREATE TIMESERIES root.sg1.d1.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d1.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d2.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d2.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg2.d1.status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg2.d1.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg2.d2.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(timestamp,status) values(150,true)",
        "INSERT INTO root.sg1.d1(timestamp,status,temperature) values(200,false,20.71)",
        "INSERT INTO root.sg1.d1(timestamp,status,temperature) values(600,false,71.12)",
        "INSERT INTO root.sg1.d2(timestamp,status,temperature) values(200,false,42.66)",
        "INSERT INTO root.sg1.d2(timestamp,status,temperature) values(300,false,46.77)",
        "INSERT INTO root.sg1.d2(timestamp,status,temperature) values(700,true,62.15)",
        "INSERT INTO root.sg2.d1(timestamp,status,temperature) values(100,3,88.24)",
        "INSERT INTO root.sg2.d1(timestamp,status,temperature) values(500,5,125.5)",
        "INSERT INTO root.sg2.d2(timestamp,temperature) values(200,105.5)",
        "INSERT INTO root.sg2.d2(timestamp,temperature) values(800,61.22)",
      };

  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)";
  private static long prevPartitionInterval;

  @BeforeClass
  public static void setUp() throws Exception {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initBeforeClass();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
  }

  // Test FunctionExpression, NegativeExpression and BinaryExpression with time filter
  @Test
  public void complexExpressionsWithTimeFilterTest() {
    Object[] retResults = {
      0L, 2L, 6.950000047683716D, 7.0D, 0.9092974268256817D, 1.9092974268256817D, -2L, 2L
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String query =
          "SELECT"
              + " count(temperature)"
              + ", count(temperature) + avg(temperature) "
              + ", count(temperature) + max_time(hardware)"
              + ", sin(count(temperature))"
              + ", sin(count(temperature)) + 1"
              + ", -count(temperature)"
              + ", count(temperature)"
              + " FROM root.ln.wf01.wt01 WHERE time > 3";
      boolean hasResultSet = statement.execute(query);

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((long) retResults[1], resultSet.getLong(1));
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertEquals((double) retResults[3], resultSet.getDouble(3), E);
        Assert.assertEquals((double) retResults[4], resultSet.getDouble(4), E);
        Assert.assertEquals((double) retResults[5], resultSet.getDouble(5), E);
        Assert.assertEquals((long) retResults[6], resultSet.getLong(6), E);
        Assert.assertEquals((long) retResults[7], resultSet.getLong(7), E);
        Assert.assertFalse(resultSet.next());
      }

      hasResultSet = statement.execute(query + " ORDER BY TIME DESC");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((long) retResults[1], resultSet.getLong(1));
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertEquals((double) retResults[3], resultSet.getDouble(3), E);
        Assert.assertEquals((double) retResults[4], resultSet.getDouble(4), E);
        Assert.assertEquals((double) retResults[5], resultSet.getDouble(5), E);
        Assert.assertEquals((long) retResults[6], resultSet.getLong(6), E);
        Assert.assertEquals((long) retResults[7], resultSet.getLong(7), E);
        Assert.assertFalse(resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void complexExpressionsWithTimeFilterTest2() {
    Object[] retResults = {0L, 1.14112000806D, 0.28224001612D};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String query =
          "SELECT sin(count(temperature) + 1) + 1,sin(count(temperature) + 1) * 2 FROM root.ln.wf01.wt01 WHERE time > 3";
      boolean hasResultSet = statement.execute(query);

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((double) retResults[1], resultSet.getDouble(1), E);
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertFalse(resultSet.next());
      }

      hasResultSet = statement.execute(query + " ORDER BY TIME DESC");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((double) retResults[1], resultSet.getDouble(1), E);
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertFalse(resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void expressionNestExtremeTest() {
    // Issue: https://issues.apache.org/jira/browse/IOTDB-2151
    Object[] retResults = {0L, 5.5F, 0.21511998808D};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String query =
          "SELECT extreme(temperature), sin(extreme(temperature) + 1) FROM root.ln.wf01.wt01";
      boolean hasResultSet = statement.execute(query);

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((float) retResults[1], resultSet.getFloat(1), E);
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertFalse(resultSet.next());
      }

      hasResultSet = statement.execute(query + " ORDER BY TIME DESC");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((float) retResults[1], resultSet.getFloat(1), E);
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertFalse(resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Test FunctionExpression, NegativeExpression and BinaryExpression with value filter
  @Test
  public void complexExpressionsWithValueFilterTest() {
    Object[] retResults = {0L, 4L, 6.75D, 8.0D, -0.7568024953D, 5.0D, -4L, 4L};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String query =
          "SELECT"
              + " count(temperature)"
              + ", count(temperature) + avg(temperature) "
              + ", count(temperature) + max_time(hardware)"
              + ", sin(count(temperature))"
              + ", count(temperature) + 1"
              + ", -count(temperature)"
              + ", count(temperature)"
              + " FROM root.ln.wf01.wt01 WHERE temperature < 5";
      boolean hasResultSet = statement.execute(query);

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((long) retResults[1], resultSet.getLong(1));
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertEquals((double) retResults[3], resultSet.getDouble(3), E);
        Assert.assertEquals((double) retResults[4], resultSet.getDouble(4), E);
        Assert.assertEquals((double) retResults[5], resultSet.getDouble(5), E);
        Assert.assertEquals((long) retResults[6], resultSet.getLong(6), E);
        Assert.assertEquals((long) retResults[7], resultSet.getLong(7), E);
        Assert.assertFalse(resultSet.next());
      }

      hasResultSet = statement.execute(query + " ORDER BY TIME DESC");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals((long) retResults[0], resultSet.getLong(TIMESTAMP_STR));
        Assert.assertEquals((long) retResults[1], resultSet.getLong(1));
        Assert.assertEquals((double) retResults[2], resultSet.getDouble(2), E);
        Assert.assertEquals((double) retResults[3], resultSet.getDouble(3), E);
        Assert.assertEquals((double) retResults[4], resultSet.getDouble(4), E);
        Assert.assertEquals((double) retResults[5], resultSet.getDouble(5), E);
        Assert.assertEquals((long) retResults[6], resultSet.getLong(6), E);
        Assert.assertEquals((long) retResults[7], resultSet.getLong(7), E);
        Assert.assertFalse(resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Test FunctionExpression, NegativeExpression and BinaryExpression with wildcard
  @Test
  public void complexExpressionWithWildcardTest() throws Exception {
    double[] retArray =
        new double[] {
          91.83,
          151.58,
          92.83,
          152.58,
          -0.66224654127,
          0.70580058691,
          -91.83,
          -151.58,
          91.83,
          151.58,
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select sum(temperature), sum(temperature) + 1, sin(sum(temperature)), -sum(temperature), sum(temperature) from root.sg1.**");

      try (ResultSet resultSet = statement.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(0L, resultSet.getLong(TIMESTAMP_STR));
        for (int i = 1; i <= 10; i++) {
          Assert.assertEquals(
              resultSet.getMetaData().getColumnName(i), retArray[i - 1], resultSet.getDouble(i), E);
        }
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void complexExpressionWithGroupByTimeWithTimeFilterTest() {
    Object[][] retArray = {
      {0L, null, null, null, null, null, null},
      {1000L, null, null, null, null, null, null},
      {2000L, null, null, null, null, null, null},
      {3000L, null, null, null, null, null, null},
      {4000L, null, null, null, null, null, null},
      {5000L, null, null, null, null, null, null},
      {6000L, -0.32466497009092804D, 6499500.0D, 1.64995E7D, 8254.75D, -6499500.0D, 6499500.0D},
      {7000L, -0.6648173658103295D, 3874750.0D, 1.387475E7D, 10129.75D, -3874750.0D, 3874750.0D},
      {8000L, 0.8033729538397475D, 4124750.0D, 1.412475E7D, 10879.75D, -4124750.0D, 4124750.0D},
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String query =
          "SELECT sin(sum(s0)), sum(s0), sum(s0)+10000000, ((avg(s2)-1000) * 3 + 11) / 2, -sum(s0), sum(s0) "
              + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 GROUP BY([0, 9000), 1s)";
      boolean hasResultSet = statement.execute(query);

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          Assert.assertEquals((long) retArray[cnt][0], resultSet.getLong(TIMESTAMP_STR));
          for (int i = 2; i <= 7; i++) {
            if (retArray[cnt][1] == null) {
              Assert.assertNull(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
            } else {
              Assert.assertEquals(
                  resultSet.getMetaData().getColumnName(i),
                  (double) retArray[cnt][i - 1],
                  resultSet.getDouble(i),
                  E);
            }
          }
          cnt++;
        }
      }
      Assert.assertEquals(retArray.length, cnt);
      // keep the correctness of `order by time desc`
      hasResultSet = statement.execute(query + " ORDER BY TIME DESC");

      Assert.assertTrue(hasResultSet);
      cnt = retArray.length;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          cnt--;
          Assert.assertEquals((long) retArray[cnt][0], resultSet.getLong(TIMESTAMP_STR));
          for (int i = 2; i <= 7; i++) {
            if (retArray[cnt][1] == null) {
              Assert.assertNull(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
            } else {
              Assert.assertEquals(
                  resultSet.getMetaData().getColumnName(i),
                  (double) retArray[cnt][i - 1],
                  resultSet.getDouble(i),
                  E);
            }
          }
        }
      }
      Assert.assertEquals(0, cnt);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void complexExpressionWithGroupByTimeWithValueFilterTest() {
    Object[][] retArray = {
      {0L, null, null, null, null, null, null},
      {1000L, null, null, null, null, null, null},
      {2000L, null, null, null, null, null, null},
      {3000L, null, null, null, null, null, null},
      {4000L, null, null, null, null, null, null},
      {5000L, null, null, null, null, null, null},
      {6000L, -0.32466497009092804D, 6499500.0D, 1.64995E7D, 8254.75D, -6499500.0D, 6499500.0D},
      {7000L, -0.6648173658103295D, 3874750.0D, 1.387475E7D, 10129.75D, -3874750.0D, 3874750.0D},
      {8000L, 0.8033729538397475D, 4124750.0D, 1.412475E7D, 10879.75D, -4124750.0D, 4124750.0D},
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String query =
          "SELECT sin(sum(s0)), sum(s0), sum(s0)+10000000, ((avg(s2)-1000) * 3 + 11) / 2, -sum(s0), sum(s0) "
              + "FROM root.vehicle.d0 WHERE s0 >= 6000 AND s0 <= 9000 GROUP BY([0, 9000), 1s)";
      boolean hasResultSet = statement.execute(query);

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          Assert.assertEquals((long) retArray[cnt][0], resultSet.getLong(TIMESTAMP_STR));
          for (int i = 2; i <= 7; i++) {
            if (retArray[cnt][1] == null) {
              Assert.assertNull(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
            } else {
              Assert.assertEquals(
                  resultSet.getMetaData().getColumnName(i),
                  (double) retArray[cnt][i - 1],
                  resultSet.getDouble(i),
                  E);
            }
          }
          cnt++;
        }
      }
      Assert.assertEquals(retArray.length, cnt);
      // keep the correctness of `order by time desc`
      hasResultSet = statement.execute(query + " ORDER BY TIME DESC");

      Assert.assertTrue(hasResultSet);
      cnt = retArray.length;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          cnt--;
          Assert.assertEquals((long) retArray[cnt][0], resultSet.getLong(TIMESTAMP_STR));
          for (int i = 2; i <= 7; i++) {
            if (retArray[cnt][1] == null) {
              Assert.assertNull(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
            } else {
              Assert.assertEquals(
                  resultSet.getMetaData().getColumnName(i),
                  (double) retArray[cnt][i - 1],
                  resultSet.getDouble(i),
                  E);
            }
          }
        }
      }
      Assert.assertEquals(0, cnt);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      // prepare BufferWrite file
      for (int i = 5000; i < 7000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.execute("FLUSH");
      for (int i = 7500; i < 8500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.execute("FLUSH");
      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.execute("FLUSH");
      for (int i = 3000; i < 6500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.execute("merge");

      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }

      for (String sql : dataSet3) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }

      for (String sql : dataSet4) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
