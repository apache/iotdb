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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBExplainAnalyzePrintIT {

  private static final String[] creationSqls =
      new String[] {
        "insert into root.test.device_0(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        "insert into root.test.device_1(s11) values(11)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testIdentitySinkOperatorWhenMergedInAnalyze() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("explain analyze select * from root.test.device_0")) {
      boolean found = false;
      while (resultSet.next()) {
        if (resultSet.getString(1).contains(IdentitySinkOperator.DOWNSTREAM_PLAN_NODE_ID)) {
          found = true;
          break;
        }
      }
      assertTrue(
          "explain analyze output should contain DownStreamPlanNodeId in IdentitySinkOperator",
          found);
    }
  }

  @Test
  public void testExchangeOperatorWhenMergedInAnalyze() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "explain analyze select * from root.test.device_0, root.test.device_1")) {
      boolean found = false;
      while (resultSet.next()) {
        if (resultSet.getString(1).contains(ExchangeOperator.SIZE_IN_BYTES)) {
          found = true;
          break;
        }
      }
      assertTrue("explain analyze output should contain size_in_bytes", found);
    }
  }
}
