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

package com.timecho.iotdb.db.it.last;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESERIES_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class TimechoDBAliasSeriesLastCacheIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableLastCache(true);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE DATABASE root.view");

      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, temperature) VALUES (1, 11.5)");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, temperature) VALUES (2, 12.5)");
      statement.execute(
          "ALTER TIMESERIES root.sg1.d1.temperature RENAME TO root.view.d1.temperature");

      statement.execute("CREATE ALIGNED TIMESERIES root.sg1.d2(s1 INT32, s2 INT32)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s1, s2) ALIGNED VALUES (1, 1, 10)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s1, s2) ALIGNED VALUES (2, 2, 20)");
      statement.execute("ALTER TIMESERIES root.sg1.d2.s1 RENAME TO root.view.d2.s1");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testLastCacheDoesNotReviveDisabledPhysicalPathAfterClearCache() throws SQLException {
    clearCache();

    final String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESERIES_STR, VALUE_STR, DATA_TYPE_STR};
    final String[] retArray = new String[] {"2,root.view.d1.temperature,12.5,FLOAT,"};
    resultSetEqualTest("SELECT LAST temperature FROM root.view.d1", expectedHeader, retArray);

    assertNonQueryTestFail(
        "INSERT INTO root.sg1.d1(timestamp, temperature) VALUES (3, 13.5)",
        "Cannot insert data into invalid series: root.sg1.d1.temperature");

    resultSetEqualTest("SELECT LAST temperature FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testLastCacheDoesNotReviveDisabledAlignedPhysicalPathAfterClearCache()
      throws SQLException {
    clearCache();

    final String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESERIES_STR, VALUE_STR, DATA_TYPE_STR};
    final String[] retArray = new String[] {"2,root.view.d2.s1,2,INT32,"};
    resultSetEqualTest("SELECT LAST s1 FROM root.view.d2", expectedHeader, retArray);

    assertNonQueryTestFail(
        "INSERT INTO root.sg1.d2(timestamp, s1) ALIGNED VALUES (3, 3)",
        "Cannot insert data into invalid series: root.sg1.d2.s1");

    resultSetEqualTest("SELECT LAST s1 FROM root.view.d2", expectedHeader, retArray);
  }

  private void clearCache() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CLEAR CACHE");
    }
  }
}
