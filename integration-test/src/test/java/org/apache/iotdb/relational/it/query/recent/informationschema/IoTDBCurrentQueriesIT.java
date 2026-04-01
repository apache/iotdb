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

package org.apache.iotdb.relational.it.query.recent.informationschema;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.END_TIME_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NUMS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.STATEMENT_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.STATE_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.USER_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.table.InformationSchema.getSchemaTables;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
// This IT will run at least 60s, so we only run it in 1C1D
public class IoTDBCurrentQueriesIT {
  private static final int CURRENT_QUERIES_COLUMN_NUM =
      getSchemaTables().get("current_queries").getColumnNum();
  private static final int QUERIES_COSTS_HISTOGRAM_COLUMN_NUM =
      getSchemaTables().get("queries_costs_histogram").getColumnNum();
  private static final String ADMIN_NAME =
      CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
  private static final String ADMIN_PWD =
      CommonDescriptor.getInstance().getConfig().getAdminPassword();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "test123123456");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCurrentQueries() {
    try {
      Assert.assertEquals(3, QUERIES_COSTS_HISTOGRAM_COLUMN_NUM);

      Connection connection =
          EnvFactory.getEnv().getConnection(ADMIN_NAME, ADMIN_PWD, BaseEnv.TABLE_SQL_DIALECT);
      Statement statement = connection.createStatement();
      statement.execute("USE information_schema");
      statement.execute("set configuration \"query_cost_stat_window\"='1'");

      // 1. query current_queries table
      String sql = "SELECT * FROM current_queries WHERE state='RUNNING'";
      ResultSet resultSet = statement.executeQuery(sql);
      ResultSetMetaData metaData = resultSet.getMetaData();
      Assert.assertEquals(CURRENT_QUERIES_COLUMN_NUM, metaData.getColumnCount());
      int rowNum = 0;
      while (resultSet.next()) {
        Assert.assertEquals(QueryState.RUNNING.name(), resultSet.getString(STATE_TABLE_MODEL));
        Assert.assertEquals(null, resultSet.getString(END_TIME_TABLE_MODEL));
        Assert.assertEquals(sql, resultSet.getString(STATEMENT_TABLE_MODEL));
        Assert.assertEquals(ADMIN_NAME, resultSet.getString(USER_TABLE_MODEL));
        rowNum++;
      }
      Assert.assertEquals(1, rowNum);
      resultSet.close();

      // 2. query queries_costs_histogram table
      sql = "SELECT * FROM queries_costs_histogram";
      resultSet = statement.executeQuery(sql);
      metaData = resultSet.getMetaData();
      Assert.assertEquals(QUERIES_COSTS_HISTOGRAM_COLUMN_NUM, metaData.getColumnCount());
      rowNum = 0;
      int queriesCount = 0;
      while (resultSet.next()) {
        int nums = resultSet.getInt(NUMS);
        if (nums > 0) {
          queriesCount++;
        }
        rowNum++;
      }
      Assert.assertEquals(1, queriesCount);
      Assert.assertEquals(61, rowNum);

      // 3. requery current_queries table
      sql = "SELECT * FROM current_queries WHERE state='FINISHED'";
      resultSet = statement.executeQuery(sql);
      metaData = resultSet.getMetaData();
      Assert.assertEquals(CURRENT_QUERIES_COLUMN_NUM, metaData.getColumnCount());
      rowNum = 0;
      int finishedQueries = 0;
      while (resultSet.next()) {
        if (QueryState.FINISHED.name().equals(resultSet.getString(STATE_TABLE_MODEL))) {
          finishedQueries++;
        }
        rowNum++;
      }
      // two rows in the result, 2 FINISHED
      Assert.assertEquals(2, rowNum);
      Assert.assertEquals(2, finishedQueries);
      resultSet.close();

      // 4. test the expired QueryInfo was evicted
      Thread.sleep(61_001);
      sql = "SELECT * FROM current_queries";
      resultSet = statement.executeQuery(sql);
      rowNum = 0;
      while (resultSet.next()) {
        rowNum++;
      }
      // one row in the result, current query
      Assert.assertEquals(1, rowNum);
      resultSet.close();

      sql = "SELECT * FROM queries_costs_histogram";
      resultSet = statement.executeQuery(sql);
      queriesCount = 0;
      while (resultSet.next()) {
        int nums = resultSet.getInt(NUMS);
        if (nums > 0) {
          queriesCount++;
        }
      }
      // the last current_queries table query was recorded, others are evicted
      Assert.assertEquals(1, queriesCount);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // 5. test privilege
    testPrivilege();

    // 6. test more configurations
    testMoreConfigurations();
  }

  private void testPrivilege() {
    // 1. test current_queries table
    try (Connection connection =
            EnvFactory.getEnv().getConnection("test", "test123123456", TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      String sql = "SELECT * FROM information_schema.current_queries";

      // another user executes a query
      try (Connection connection2 =
          EnvFactory.getEnv().getConnection(ADMIN_NAME, ADMIN_PWD, BaseEnv.TABLE_SQL_DIALECT)) {
        ResultSet resultSet = connection2.createStatement().executeQuery(sql);
        resultSet.close();
      } catch (Exception e) {
        fail(e.getMessage());
      }

      // current user query current_queries table
      ResultSet resultSet = statement.executeQuery(sql);
      int rowNum = 0;
      while (resultSet.next()) {
        rowNum++;
      }
      // only current query in the result
      Assert.assertEquals(1, rowNum);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // 2. test queries_costs_histogram table
    try (Connection connection =
            EnvFactory.getEnv().getConnection("test", "test123123456", TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.executeQuery("SELECT * FROM information_schema.queries_costs_histogram");
    } catch (SQLException e) {
      Assert.assertEquals(
          "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
          e.getMessage());
    }
  }

  private void testMoreConfigurations() {
    try {
      Connection connection =
          EnvFactory.getEnv().getConnection(ADMIN_NAME, ADMIN_PWD, BaseEnv.TABLE_SQL_DIALECT);
      Statement statement = connection.createStatement();
      statement.execute("USE information_schema");

      statement.execute("set configuration \"query_cost_stat_window\"='0'");
      Thread.sleep(1_001);

      // query_cost_stat_window = 0, history queries are cleared
      String sql = "SELECT * FROM current_queries WHERE state='FINISHED'";
      ResultSet resultSet = statement.executeQuery(sql);
      ResultSetMetaData metaData = resultSet.getMetaData();
      Assert.assertEquals(CURRENT_QUERIES_COLUMN_NUM, metaData.getColumnCount());
      int rowNum = 0;
      while (resultSet.next()) {
        rowNum++;
      }
      Assert.assertEquals(0, rowNum);
      resultSet.close();

      statement.execute("set configuration \"query_cost_stat_window\"='1040000000'");
      // make query_cost_stat_window very large but not overflow
      resultSet = statement.executeQuery(sql);
      while (resultSet.next()) {
        rowNum++;
      }
      resultSet.close();

      resultSet = statement.executeQuery(sql);
      rowNum = 0;
      while (resultSet.next()) {
        rowNum++;
      }
      // the history SQL is recorded
      Assert.assertEquals(1, rowNum);
      resultSet.close();

      // make query_cost_stat_window overflow
      try {
        statement.execute("set configuration \"query_cost_stat_window\"='10400000000'");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("java.lang.NumberFormatException: For input string: \"10400000000\""));
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
