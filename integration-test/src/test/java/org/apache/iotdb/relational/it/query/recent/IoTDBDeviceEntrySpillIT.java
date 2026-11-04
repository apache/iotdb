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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDeviceEntrySpillIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setTableQueryDeviceEntryBatchSizeInBytes(1);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE spill_test");
      statement.execute(
          "CREATE TABLE spill_test.device_data (tag1 STRING TAG, tag2 STRING TAG, "
              + "value INT32 FIELD)");
      statement.execute(
          "INSERT INTO spill_test.device_data(tag1, tag2, time, value) "
              + "VALUES ('a', 'x', 1, 10), ('a', 'x', 2, 20), "
              + "('b', 'y', 1, 30), ('c', 'z', 1, 40)");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRawFullTableQueryWithSpill() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT tag1, tag2, value FROM spill_test.device_data")) {
      int rowCount = 0;
      while (resultSet.next()) {
        rowCount++;
      }
      assertEquals(4, rowCount);
    }
  }

  @Test
  public void testRawQueriesWithTimeFilterProjectionFilterLimitAndOrdering() throws Exception {
    String[] queries = {
      "SELECT * FROM spill_test.device_data WHERE time >= 1 AND time < 3",
      "SELECT time, value FROM spill_test.device_data WHERE time >= 1 AND time < 3",
      "SELECT tag1, tag2, value FROM spill_test.device_data "
          + "WHERE time >= 1 AND time < 3 AND value > 10",
      "SELECT * FROM spill_test.device_data WHERE time >= 1 AND time < 3 LIMIT 2",
      "SELECT * FROM spill_test.device_data WHERE time >= 1 AND time < 3 ORDER BY time ASC",
      "SELECT * FROM spill_test.device_data WHERE time >= 1 AND time < 3 " + "ORDER BY tag1, time"
    };
    for (String query : queries) {
      assertRowCount(query, query.contains("LIMIT 2") ? 2 : query.contains("value > 10") ? 3 : 4);
    }
  }

  @Test
  public void testAggregationQueryWithSpill() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT count(value) FROM spill_test.device_data")) {
      assertEquals(true, resultSet.next());
      assertEquals(4, resultSet.getLong(1));
      assertEquals(false, resultSet.next());
    }
  }

  @Test
  public void testGroupedAggregationAcrossSpillSegments() throws Exception {
    assertRowCount("SELECT tag1, count(*) FROM spill_test.device_data GROUP BY tag1", 3);
    assertRowCount(
        "SELECT tag1, tag2, count(*), sum(value) FROM spill_test.device_data "
            + "GROUP BY tag1, tag2",
        3);
    assertRowCount(
        "SELECT date_bin(1ms, time), count(*) FROM spill_test.device_data "
            + "GROUP BY date_bin(1ms, time)",
        2);
  }

  @Test
  public void testOrPredicateDoesNotDuplicateDeviceRows() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT count(*) FROM spill_test.device_data "
                    + "WHERE tag1 = 'a' OR tag2 = 'x'")) {
      assertEquals(true, resultSet.next());
      assertEquals(2, resultSet.getLong(1));
      assertEquals(false, resultSet.next());
    }
  }

  private void assertRowCount(String sql, int expectedRowCount) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      int rowCount = 0;
      while (resultSet.next()) {
        rowCount++;
      }
      assertEquals(expectedRowCount, rowCount);
    }
  }
}
