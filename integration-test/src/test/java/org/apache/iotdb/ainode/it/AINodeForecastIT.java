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

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeForecastIT {

  private static final String FORECAST_TABLE_FUNCTION_SQL_TEMPLATE =
      "SELECT * FROM FORECAST(model_id=>'%s', input=>(SELECT time, s%d FROM db.AI) ORDER BY time)";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE db");
      statement.execute(
          "CREATE TABLE db.AI (s0 FLOAT FIELD, s1 DOUBLE FIELD, s2 INT32 FIELD, s3 INT64 FIELD)");
      for (int i = 0; i < 2880; i++) {
        statement.execute(
            String.format(
                "INSERT INTO db.AI(time,s0,s1,s2,s3) VALUES(%d,%f,%f,%d,%d)",
                i, (float) i, (double) i, i, i));
      }
    }
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

  public void forecastTableFunctionTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) throws SQLException {
    // Invoke call inference for specified models, there should exist result.
    for (int i = 0; i < 4; i++) {
      String forecastTableFunctionSQL =
          String.format(FORECAST_TABLE_FUNCTION_SQL_TEMPLATE, modelInfo.getModelId(), i);
      try (ResultSet resultSet = statement.executeQuery(forecastTableFunctionSQL)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        // Ensure the call inference return results
        Assert.assertTrue(count > 0);
      }
    }
  }
}
