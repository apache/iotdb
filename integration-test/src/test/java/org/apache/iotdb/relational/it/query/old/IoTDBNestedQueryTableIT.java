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

package org.apache.iotdb.relational.it.query.old;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBNestedQueryTableIT {

  private static final String DATABASE_NAME = "db";

  protected static final int ITERATION_TIMES = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setUdfMemoryBudgetInMB(5);

    EnvFactory.getEnv().initClusterEnvironment();
    createTable();
    generateData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTable() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "create table vehicle1(device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 TEXT MEASUREMENT, s4 STRING MEASUREMENT, s5 DATE MEASUREMENT, s6 TIMESTAMP MEASUREMENT)");

      statement.execute(
          "create table vehicle2(device_id STRING ID, s1 FLOAT MEASUREMENT, s2 DOUBLE MEASUREMENT, empty DOUBLE MEASUREMENT)");
      statement.execute(
          "create table likeTest(device_id STRING ID, s1 TEXT MEASUREMENT, s2 STRING MEASUREMENT)");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into vehicle1(time,device_id,s1,s2,s3,s4,s6) values(%d,%s,%d,%d,%s,%s,%d)",
                i, "'d1'", i, i, i, i, i));
        statement.execute(
            (String.format(
                "insert into vehicle2(time,device_id,s1,s2) values(%d,%s,%d,%d)",
                i, "'d2'", i, i)));
      }
      statement.execute("insert into vehicle1(time,device_id,s5) values(1, 'd1', '2024-01-01')");
      statement.execute("insert into vehicle1(time,device_id,s5) values(2, 'd1','2024-01-02')");
      statement.execute("insert into vehicle1(time,device_id,s5) values(3, 'd1','2024-01-03')");
      statement.execute(
          "insert into likeTest(time,device_id,s1,s2) values(1, 'd1','abcdef', '123456')");
      statement.execute(
          "insert into likeTest(time,device_id,s1,s2) values(2, 'd1','_abcdef', '123\\456')");
      statement.execute(
          "insert into likeTest(time,device_id,s1,s2) values(3, 'd1','abcdef%', '123#456')");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore
  @Test
  public void testNestedRowByRowUDFExpressions() {
    String sqlStr =
        "select time, s1, s2, sin(sin(s1) * sin(s2) + cos(s1) * cos(s1)) + sin(sin(s1 - s1 + s2) * sin(s2) + cos(s1) * cos(s1)), asin(sin(asin(sin(s1 - s2 / (-s1))))) from vehicle2";

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 4, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        ++count;

        assertEquals(count, resultSet.getLong(1));
        assertEquals(count, Double.parseDouble(resultSet.getString(2)), 0);
        assertEquals(count, Double.parseDouble(resultSet.getString(3)), 0);
        assertEquals(2 * Math.sin(1.0), Double.parseDouble(resultSet.getString(4)), 1e-5);
        assertEquals(
            Math.asin(Math.sin(Math.asin(Math.sin(count - count / (-count))))),
            Double.parseDouble(resultSet.getString(5)),
            1e-5);
      }

      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRawDataQueryWithConstants() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String query = "SELECT time, 1 + s1 FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1, rs.getInt(2), 0.01);
        }
        Assert.assertFalse(rs.next());
      }

      query = "SELECT time, (1 + 4) * 2 / 10 + s1 FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1, rs.getInt(2), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDuplicatedRawDataQueryWithConstants() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String query = "SELECT time, 1 + s1, 1 + s1 FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i + 1, rs.getInt(2), 0.01);
          Assert.assertEquals(i + 1, rs.getInt(3), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testCommutativeLaws() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String query =
          "SELECT time, s1, s1 + 1, 1 + s1, s1 * 2, 2 * s1 FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i, rs.getInt(2));
          Assert.assertEquals(i + 1, rs.getInt(3), 0.01);
          Assert.assertEquals(i + 1, rs.getInt(4), 0.01);
          Assert.assertEquals(i * 2, rs.getInt(5), 0.01);
          Assert.assertEquals(i * 2, rs.getInt(6), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testAssociativeLaws() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String query =
          "SELECT time, s1, s1 + 1 + 2, (s1 + 1) + 2, s1 + (1 + 2), s1 * 2 * 3, s1 * (2 * 3), (s1 * 2) * 3 FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i, rs.getInt(2));
          Assert.assertEquals(i + 3, rs.getInt(3), 0.01);
          Assert.assertEquals(i + 3, rs.getInt(4), 0.01);
          Assert.assertEquals(i + 3, rs.getInt(5), 0.01);
          Assert.assertEquals(i * 6, rs.getInt(6), 0.01);
          Assert.assertEquals(i * 6, rs.getInt(7), 0.01);
          Assert.assertEquals(i * 6, rs.getInt(8), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDistributiveLaw() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String query =
          "SELECT time, s1, (s1 + 1) * 2, s1 * 2 + 1 * 2, (s1 + 1) / 2, s1 / 2 + 1 / 2 FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(i, rs.getInt(2));
          Assert.assertEquals(2 * i + 2, rs.getInt(3), 0.01);
          Assert.assertEquals(2 * i + 2, rs.getInt(4), 0.01);
          Assert.assertEquals((i + 1) / 2, rs.getInt(5), 0.01);
          Assert.assertEquals(i / 2 + 1 / 2, rs.getInt(6), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testOrderOfArithmeticOperations() {
    // Priority from high to low:
    //   1. exponentiation and root extraction (not supported yet)
    //   2. multiplication and division
    //   3. addition and subtraction
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      String query =
          "SELECT time, 1 + s1 * 2 + 1, (1 + s1) * 2 + 1, (1 + s1) * (2 + 1)  FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(2 * i + 2, rs.getInt(2), 0.01);
          Assert.assertEquals(2 * i + 3, rs.getInt(3), 0.01);
          Assert.assertEquals(3 * i + 3, rs.getInt(4), 0.01);
        }
        Assert.assertFalse(rs.next());
      }

      query =
          "SELECT time, 1 - s1 / 2 + 1, (1 - s1) / 2 + 1, (1 - s1) / (2 + 1)  FROM vehicle1 where device_id='d1'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 1; i <= ITERATION_TIMES; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong(1));
          Assert.assertEquals(2 - i / 2, rs.getInt(2), 0.01);
          Assert.assertEquals((1 - i) / 2 + 1, rs.getInt(3), 0.01);
          Assert.assertEquals((1 - i) / (2 + 1), rs.getInt(4), 0.01);
        }
        Assert.assertFalse(rs.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testBetweenExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      int start = 2, end = 8;
      statement.execute("USE " + DATABASE_NAME);
      String query =
          "SELECT * FROM vehicle1 where device_id='d1' and (s1 BETWEEN "
              + start
              + " AND "
              + end
              + ")";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query =
          "SELECT * FROM vehicle1 where device_id='d1' and (s1 NOT BETWEEN " // test not between
              + (end + 1)
              + " AND "
              + ITERATION_TIMES
              + ")";
      try (ResultSet rs = statement.executeQuery(query)) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("time"));
        Assert.assertEquals("1", rs.getString("s1"));
        Assert.assertEquals("1", rs.getString("s2"));
        Assert.assertEquals("1", rs.getString("s3"));
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query =
          "SELECT * FROM vehicle1 where device_id='d1' and (time BETWEEN "
              + start
              + " AND "
              + end
              + ")";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }

      query =
          "SELECT * FROM vehicle1 where device_id='d1' and (time NOT BETWEEN " // test not between
              + (end + 1)
              + " AND "
              + ITERATION_TIMES
              + ")";
      try (ResultSet rs = statement.executeQuery(query)) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("time"));
        Assert.assertEquals("1", rs.getString("s1"));
        Assert.assertEquals("1", rs.getString("s2"));
        Assert.assertEquals("1", rs.getString("s3"));
        for (int i = start; i <= end; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(i, rs.getLong("time"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s1"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s2"));
          Assert.assertEquals(String.valueOf(i), rs.getString("s3"));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testRegularLikeInExpressions() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      String[] ans = new String[] {"abcdef"};
      String query = "SELECT s1 FROM likeTest where s1 LIKE 'abcdef'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 2; i < 3; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(ans[i - 2], rs.getString(1));
        }
        Assert.assertFalse(rs.next());
      }

      ans = new String[] {"_abcdef"};
      query = "SELECT s1 FROM likeTest where s1 LIKE '\\_%' escape '\\'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 2; i < 3; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(ans[i - 2], rs.getString(1));
        }
        Assert.assertFalse(rs.next());
      }

      ans = new String[] {"abcdef", "_abcdef", "abcdef%"};
      query = "SELECT s1 FROM likeTest where s1 LIKE '%abcde%' escape '\\'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 2; i < 5; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(ans[i - 2], rs.getString(1));
        }
        Assert.assertFalse(rs.next());
      }

      ans = new String[] {"123456"};
      query = "SELECT s2 FROM likeTest where s2 LIKE '12345_'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 2; i < 3; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(ans[i - 2], rs.getString(1));
        }
        Assert.assertFalse(rs.next());
      }

      ans = new String[] {"123\\456"};
      query = "SELECT s2 FROM likeTest where s2 LIKE '%\\\\%' escape '\\'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 2; i < 3; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(ans[i - 2], rs.getString(1));
        }
        Assert.assertFalse(rs.next());
      }

      ans = new String[] {"123#456"};
      query = "SELECT s2 FROM likeTest where s2 LIKE '123##456' escape '#'";
      try (ResultSet rs = statement.executeQuery(query)) {
        for (int i = 2; i < 3; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(ans[i - 2], rs.getString(1));
        }
        Assert.assertFalse(rs.next());
      }

    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
