/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTExplainAnalyzeIT {
  private static final String DATABASE_NAME = "testdb";

  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE IF NOT EXISTS testtb(deviceid STRING TAG, voltage FLOAT FIELD)",
        "INSERT INTO testtb VALUES(1000, 'd1', 100.0)",
        "INSERT INTO testtb VALUES(2000, 'd1', 200.0)",
        "INSERT INTO testtb VALUES(1000, 'd2', 300.0)",
      };

  private static final String dropDbSqls = "DROP DATABASE IF EXISTS " + DATABASE_NAME;

  @BeforeClass
  public static void setUpClass() {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000)
        .setMaxRowsInCteBuffer(100);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws SQLException {
    prepareData();
  }

  @After
  public void tearDown() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(dropDbSqls);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testEmptyCteQuery() {
    String sql =
        "explain analyze with cte1 as materialized (select * from testtb1) select * from testtb, cte1 where testtb.deviceid = cte1.deviceid";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("Use " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS testtb1(deviceid STRING TAG, voltage FLOAT FIELD)");
      ResultSet resultSet = statement.executeQuery(sql);
      StringBuilder sb = new StringBuilder();
      while (resultSet.next()) {
        System.out.println(resultSet.getString(1));
        sb.append(resultSet.getString(1)).append(System.lineSeparator());
      }
      resultSet.close();

      String result = sb.toString();
      Assert.assertFalse(
          "Explain Analyze should not contain ExplainAnalyze node.",
          result.contains("ExplainAnalyzeNode"));

      String[] lines = result.split(System.lineSeparator());
      Assert.assertTrue(lines.length > 3);
      Assert.assertEquals("CTE Query : 'cte1'", lines[0]);
      Assert.assertEquals("", lines[1]);
      Assert.assertEquals("Main Query", lines[2]);
      statement.execute("DROP TABLE testtb1");

    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCteQueryExceedsThreshold() {
    String sql =
        "explain analyze with cte1 as materialized (select * from testtb2) select * from testtb where testtb.deviceid in (select deviceid from cte1)";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("Use " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS testtb2(deviceid STRING TAG, voltage FLOAT FIELD)");
      for (int i = 0; i < 100; i++) {
        statement.addBatch(
            String.format("insert into testtb2(deviceid, voltage) values('d%d', %d)", i, i));
      }
      statement.executeBatch();
      ResultSet resultSet = statement.executeQuery(sql);
      StringBuilder sb = new StringBuilder();
      while (resultSet.next()) {
        sb.append(resultSet.getString(1)).append(System.lineSeparator());
      }
      resultSet.close();

      String result = sb.toString();
      Assert.assertFalse(
          "Main Query should not contain CteScan node when the CTE query's result set exceeds threshold.",
          result.contains("CteScanNode(CteScanOperator)"));
      Assert.assertTrue(
          "CTE Query should contain warning message when CTE query's result set exceeds threshold.",
          result.contains("Failed to materialize CTE"));
      Assert.assertFalse(
          "Explain Analyze should not contain ExplainAnalyze node.",
          result.contains("ExplainAnalyzeNode"));

      String[] plans = result.split("Main Query");
      for (String plan : plans) {
        String[] lines = plan.split(System.lineSeparator());
        long[] instanceCount =
            Arrays.stream(lines)
                .filter(line -> line.contains("Fragment Instances Count:"))
                .mapToLong(extractNumber("Fragment Instances Count:\\s(\\d+)"))
                .toArray();
        assertEquals(instanceCount.length, 1);

        long totalInstances =
            Arrays.stream(lines).filter(line -> line.contains("FRAGMENT-INSTANCE")).count();
        assertEquals(totalInstances, instanceCount[0]);
      }

      statement.execute("DROP TABLE testtb2");
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCteQuerySuccess() {
    String sql =
        "explain analyze with cte1 as materialized (select voltage, deviceid from testtb3) select * from testtb where testtb.deviceid in (select deviceid from cte1)";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("Use " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS testtb3(deviceid STRING TAG, voltage FLOAT FIELD)");
      for (int i = 0; i < 50; i++) {
        statement.addBatch(
            String.format("insert into testtb3(deviceid, voltage) values('d%d', %d)", i, i));
      }
      statement.executeBatch();
      ResultSet resultSet = statement.executeQuery(sql);
      StringBuilder sb = new StringBuilder();
      while (resultSet.next()) {
        sb.append(resultSet.getString(1)).append(System.lineSeparator());
      }
      resultSet.close();

      String result = sb.toString();
      Assert.assertTrue(
          "Main Query should contain CteScan node when the CTE query's result set does not exceeds threshold.",
          result.contains("CteScanNode(CteScanOperator)"));
      Assert.assertFalse(
          "Explain Analyze should not contain ExplainAnalyze node.",
          result.contains("ExplainAnalyzeNode"));

      String[] plans = result.split("Main Query");
      for (String plan : plans) {
        String[] lines = plan.split(System.lineSeparator());
        long[] instanceCount =
            Arrays.stream(lines)
                .filter(line -> line.contains("Fragment Instances Count:"))
                .mapToLong(extractNumber("Fragment Instances Count:\\s(\\d+)"))
                .toArray();
        assertEquals(instanceCount.length, 1);

        long totalInstances =
            Arrays.stream(lines).filter(line -> line.contains("FRAGMENT-INSTANCE")).count();
        assertEquals(totalInstances, instanceCount[0]);
      }

      statement.execute("DROP TABLE testtb3");
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  private static ToLongFunction<String> extractNumber(String regex) {
    return line -> {
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        try {
          return Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
          return 0;
        }
      }
      return 0;
    };
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
