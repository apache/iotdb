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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTExplainJsonFormatIT {
  private static final String DATABASE_NAME = "testdb_json";

  @BeforeClass
  public static void setUp() {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000)
        .setMaxRowsInCteBuffer(100);
    EnvFactory.getEnv().initClusterEnvironment();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS testtb(deviceid STRING TAG, voltage FLOAT FIELD)");
      // Insert data across multiple time partitions (partitionInterval=1000)
      statement.execute("INSERT INTO testtb VALUES(1000, 'd1', 100.0)");
      statement.execute("INSERT INTO testtb VALUES(2000, 'd1', 200.0)");
      statement.execute("INSERT INTO testtb VALUES(3000, 'd1', 150.0)");
      statement.execute("INSERT INTO testtb VALUES(1000, 'd2', 300.0)");
      statement.execute("INSERT INTO testtb VALUES(2000, 'd2', 250.0)");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS " + DATABASE_NAME);
    } catch (Exception e) {
      // ignore
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testExplainJsonFormat() {
    String sql = "EXPLAIN (FORMAT JSON) SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue("Should have at least one row", resultSet.next());
      String jsonStr = resultSet.getString(1);
      Assert.assertNotNull(jsonStr);

      // Verify it's valid JSON
      JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();
      Assert.assertTrue("JSON should have 'name' field", root.has("name"));
      Assert.assertTrue("JSON should have 'id' field", root.has("id"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainDefaultFormatIsNotJson() {
    String sql = "EXPLAIN SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue("Should have at least one row", resultSet.next());
      String firstLine = resultSet.getString(1);
      // Default format (GRAPHVIZ) produces box-drawing characters, not JSON
      Assert.assertFalse(
          "Default EXPLAIN should not produce JSON", firstLine.trim().startsWith("{"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainAnalyzeJsonFormat() {
    String sql = "EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      // JSON format should produce a single row with full JSON
      Assert.assertTrue("Should have at least one row", resultSet.next());
      String jsonStr = resultSet.getString(1);
      Assert.assertNotNull(jsonStr);

      // Verify it's valid JSON
      JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();
      Assert.assertTrue("JSON should have 'planStatistics' field", root.has("planStatistics"));
      Assert.assertTrue(
          "JSON should have 'fragmentInstances' field", root.has("fragmentInstances"));
      Assert.assertTrue(
          "JSON should have 'fragmentInstancesCount' field", root.has("fragmentInstancesCount"));

      // Plan statistics should contain known keys
      JsonObject planStats = root.getAsJsonObject("planStatistics");
      Assert.assertTrue(planStats.has("analyzeCostMs"));
      Assert.assertTrue(planStats.has("logicalPlanCostMs"));
      Assert.assertTrue(planStats.has("distributionPlanCostMs"));
      Assert.assertTrue(planStats.has("dispatchCostMs"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainAnalyzeVerboseJsonFormat() {
    String sql = "EXPLAIN ANALYZE VERBOSE (FORMAT JSON) SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue("Should have at least one row", resultSet.next());
      String jsonStr = resultSet.getString(1);
      Assert.assertNotNull(jsonStr);

      // Verify it's valid JSON
      JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();
      Assert.assertTrue(root.has("planStatistics"));
      Assert.assertTrue(root.has("fragmentInstances"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainAnalyzeDefaultIsText() {
    String sql = "EXPLAIN ANALYZE SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue("Should have at least one row", resultSet.next());
      String firstLine = resultSet.getString(1);
      // Default format (TEXT) should not start with '{'
      Assert.assertFalse(
          "Default EXPLAIN ANALYZE should not produce JSON", firstLine.trim().startsWith("{"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainGraphvizFormatExplicit() {
    String sql = "EXPLAIN (FORMAT GRAPHVIZ) SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue("Should have at least one row", resultSet.next());
      String firstLine = resultSet.getString(1);
      // GRAPHVIZ format produces box-drawing characters, not JSON
      Assert.assertFalse(
          "GRAPHVIZ EXPLAIN should not produce JSON", firstLine.trim().startsWith("{"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainAnalyzeTextFormatExplicit() {
    String sql = "EXPLAIN ANALYZE (FORMAT TEXT) SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue("Should have at least one row", resultSet.next());
      String firstLine = resultSet.getString(1);
      Assert.assertFalse(
          "TEXT EXPLAIN ANALYZE should not produce JSON", firstLine.trim().startsWith("{"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainInvalidFormat() {
    String sql = "EXPLAIN (FORMAT XML) SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.executeQuery(sql);
      fail("Expected SQLException for invalid format");
    } catch (SQLException e) {
      Assert.assertTrue(
          "Error message should mention the invalid format",
          e.getMessage().toUpperCase().contains("FORMAT")
              || e.getMessage().toUpperCase().contains("XML"));
    }
  }

  @Test
  public void testExplainAnalyzeJsonMultipleFragmentInstances() {
    String sql = "EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM testtb";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue(resultSet.next());
      String jsonStr = resultSet.getString(1);
      JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();

      // Verify fragmentInstancesCount matches the size of fragmentInstances array
      int declaredCount = root.get("fragmentInstancesCount").getAsInt();
      JsonArray fragmentInstances = root.getAsJsonArray("fragmentInstances");
      Assert.assertNotNull("fragmentInstances array should be present", fragmentInstances);
      Assert.assertEquals(
          "fragmentInstancesCount should match fragmentInstances array size",
          declaredCount,
          fragmentInstances.size());
      Assert.assertTrue(
          "Should have at least 2 fragment instances for multi-partition data", declaredCount >= 2);

      // Verify each fragment instance has required fields
      for (int i = 0; i < fragmentInstances.size(); i++) {
        JsonObject fi = fragmentInstances.get(i).getAsJsonObject();
        Assert.assertTrue("Fragment instance should have 'id'", fi.has("id"));
        Assert.assertTrue("Fragment instance should have 'state'", fi.has("state"));
        Assert.assertTrue("Fragment instance should have 'dataRegion'", fi.has("dataRegion"));
      }

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainJsonWithCte() {
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS cte_tb(deviceid STRING TAG, voltage FLOAT FIELD)");
      statement.execute("INSERT INTO cte_tb VALUES(1000, 'd1', 50.0)");

      String sql =
          "EXPLAIN (FORMAT JSON) WITH cte1 AS MATERIALIZED (SELECT * FROM cte_tb) "
              + "SELECT * FROM testtb WHERE testtb.deviceid IN (SELECT deviceid FROM cte1)";
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue(resultSet.next());
      String jsonStr = resultSet.getString(1);
      JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();

      // When CTEs are present, the JSON should have cteQueries and mainQuery
      Assert.assertTrue("JSON with CTE should have 'cteQueries' field", root.has("cteQueries"));
      Assert.assertTrue("JSON with CTE should have 'mainQuery' field", root.has("mainQuery"));

      JsonArray cteQueries = root.getAsJsonArray("cteQueries");
      Assert.assertEquals("Should have exactly 1 CTE query", 1, cteQueries.size());

      JsonObject cte = cteQueries.get(0).getAsJsonObject();
      Assert.assertTrue("CTE should have 'name' field", cte.has("name"));
      Assert.assertEquals("cte1", cte.get("name").getAsString());
      Assert.assertTrue("CTE should have 'plan' field", cte.has("plan"));

      // The main query plan should be a JSON object with 'name' field (plan node)
      JsonObject mainQuery = root.getAsJsonObject("mainQuery");
      Assert.assertTrue("Main query plan should have 'name' field", mainQuery.has("name"));

      statement.execute("DROP TABLE IF EXISTS cte_tb");
      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainJsonWithScalarSubquery() {
    String sql =
        "EXPLAIN (FORMAT JSON) SELECT * FROM testtb "
            + "WHERE voltage > (SELECT avg(voltage) FROM testtb)";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue(resultSet.next());
      String jsonStr = resultSet.getString(1);
      JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();

      // Verify it's a valid plan tree with children (subquery creates a more complex plan)
      Assert.assertTrue("JSON should have 'name' field", root.has("name"));
      Assert.assertTrue("JSON should have 'id' field", root.has("id"));
      Assert.assertTrue("Plan with scalar subquery should have 'children'", root.has("children"));

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExplainAnalyzeJsonWithScalarSubquery() {
    String sql =
        "EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM testtb "
            + "WHERE voltage > (SELECT avg(voltage) FROM testtb)";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sql);

      Assert.assertTrue(resultSet.next());
      String jsonStr = resultSet.getString(1);
      JsonObject root = JsonParser.parseString(jsonStr).getAsJsonObject();

      Assert.assertTrue(root.has("planStatistics"));
      Assert.assertTrue(root.has("fragmentInstances"));
      Assert.assertTrue(root.has("fragmentInstancesCount"));

      int declaredCount = root.get("fragmentInstancesCount").getAsInt();
      JsonArray fragmentInstances = root.getAsJsonArray("fragmentInstances");
      Assert.assertEquals(declaredCount, fragmentInstances.size());

      resultSet.close();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }
}
