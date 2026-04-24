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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBHintIT {
  private static final String DATABASE_NAME = "testdb";

  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS testdb",
        "USE testdb",
        "CREATE TABLE IF NOT EXISTS testtb(voltage FLOAT FIELD, manufacturer STRING FIELD, deviceid STRING TAG)",
        "INSERT INTO testtb VALUES(1000, 100.0, 'a', 'd1')",
        "INSERT INTO testtb VALUES(2000, 200.0, 'b', 'd1')",
        "INSERT INTO testtb VALUES(1000, 300.0, 'c', 'd2')",
      };

  private static final String dropDbSqls = "DROP DATABASE IF EXISTS testdb";

  @BeforeClass
  public static void setUpClass() {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() {
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
  public void testReplicaHintWithInvalidTable() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      String sql = "SELECT /*+ REPLICA(t1, 2) */ * FROM testtb";
      statement.executeQuery(sql);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testReplicaHintWithSystemTableQuery() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      String sql = "SELECT /*+ REPLICA(0) */ * FROM information_schema.tables";
      statement.executeQuery(sql);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testReplicaHintWithCTE() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      // REPLICA hint in CTE definition
      String sql1 =
          "WITH cte1 AS (SELECT /*+ REPLICA(testtb,0) */ * FROM testtb) SELECT * FROM cte1";
      statement.executeQuery(sql1);
      // REPLICA hint in materialized CTE definition
      String sql2 =
          "WITH cte1 AS materialized (SELECT /*+ REPLICA(testtb,0) */ * FROM testtb) SELECT * FROM cte1";
      statement.executeQuery(sql2);
      // REPLICA hint in main query
      String sql3 =
          "WITH cte1 AS (SELECT * FROM testtb) SELECT /*+ REPLICA(testtb, 0) */ * FROM cte1";
      statement.executeQuery(sql3);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testReplicaHintWithExplain() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      String sql = "EXPLAIN SELECT /*+ REPLICA(0) */ * FROM testtb";
      statement.executeQuery(sql);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testReplicaHintWithJoin() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      String sql =
          "SELECT /*+ REPLICA(a, 0) REPLICA(b, 1) */ * FROM testtb as a INNER JOIN testtb as b ON a.voltage = b.voltage";
      ResultSet rs = statement.executeQuery(sql);
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals("Expected 3 rows from the join query", 3, count);
    } catch (Exception e) {
      fail(e.getMessage());
    }
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
