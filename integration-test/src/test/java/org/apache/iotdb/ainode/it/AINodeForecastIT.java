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

package org.apache.iotdb.ainode.it;

import org.apache.iotdb.ainode.utils.AINodeTestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.AIClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.BUILTIN_MODEL_MAP;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeForecastIT {

  private static final String FORECAST_TABLE_FUNCTION_SQL_TEMPLATE =
      "SELECT * FROM FORECAST("
          + "model_id=>'%s', "
          + "targets=>(SELECT time, s%d FROM db.AI WHERE time<%d ORDER BY time DESC LIMIT %d) ORDER BY time, "
          + "output_start_time=>%d, "
          + "output_length=>%d, "
          + "output_interval=>%d, "
          + "timecol=>'%s'"
          + ")";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void forecastTableFunctionTest() throws SQLException {
    for (AINodeTestUtils.FakeModelInfo modelInfo : BUILTIN_MODEL_MAP.values()) {
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        forecastTableFunctionTest(statement, modelInfo);
      }
    }
  }

  public static void forecastTableFunctionTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) throws SQLException {
    // Invoke forecast table function for specified models, there should exist result.
    for (int i = 0; i < 4; i++) {
      String forecastTableFunctionSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_SQL_TEMPLATE,
              modelInfo.getModelId(),
              i,
              5760,
              2880,
              5760,
              96,
              1,
              "time");
      try (ResultSet resultSet = statement.executeQuery(forecastTableFunctionSQL)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        // Ensure the forecast sentence return results
        Assert.assertTrue(count > 0);
      }
    }
  }

  @Test
  public void forecastTableFunctionErrorTest() throws SQLException {
    for (AINodeTestUtils.FakeModelInfo modelInfo : BUILTIN_MODEL_MAP.values()) {
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        forecastTableFunctionErrorTest(statement, modelInfo);
      }
    }
  }

  public static void forecastTableFunctionErrorTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) throws SQLException {
    // OUTPUT_START_TIME error
    String invalidOutputStartTimeSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_SQL_TEMPLATE,
            modelInfo.getModelId(),
            0,
            5760,
            2880,
            5759,
            96,
            1,
            "time");
    errorTest(
        statement,
        invalidOutputStartTimeSQL,
        "701: The OUTPUT_START_TIME should be greater than the maximum timestamp of target time series. Expected greater than [5759] but found [5759].");

    // OUTPUT_LENGTH error
    String invalidOutputLengthSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_SQL_TEMPLATE,
            modelInfo.getModelId(),
            0,
            5760,
            2880,
            5760,
            0,
            1,
            "time");
    errorTest(statement, invalidOutputLengthSQL, "701: OUTPUT_LENGTH should be greater than 0");

    // OUTPUT_INTERVAL error
    String invalidOutputIntervalSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_SQL_TEMPLATE,
            modelInfo.getModelId(),
            0,
            5760,
            2880,
            5760,
            96,
            -1,
            "time");
    errorTest(statement, invalidOutputIntervalSQL, "701: OUTPUT_INTERVAL should be greater than 0");

    // TIMECOL error
    String invalidTimecolSQL2 =
        String.format(
            FORECAST_TABLE_FUNCTION_SQL_TEMPLATE,
            modelInfo.getModelId(),
            0,
            5760,
            2880,
            5760,
            96,
            1,
            "s0");
    errorTest(
        statement, invalidTimecolSQL2, "701: The type of the column [s0] is not as expected.");
  }
}
