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

import org.apache.iotdb.ainode.utils.AINodeTestUtils.FakeModelInfo;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.AIClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.EXAMPLE_MODEL_PATH;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeModelManageIT {

  private static final Map<String, FakeModelInfo> BUILT_IN_MODEL_MAP =
      Stream.of(
              new AbstractMap.SimpleEntry<>(
                  "arima", new FakeModelInfo("arima", "Arima", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "holtwinters",
                  new FakeModelInfo("holtwinters", "HoltWinters", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "exponential_smoothing",
                  new FakeModelInfo(
                      "exponential_smoothing", "ExponentialSmoothing", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "naive_forecaster",
                  new FakeModelInfo("naive_forecaster", "NaiveForecaster", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "stl_forecaster",
                  new FakeModelInfo("stl_forecaster", "StlForecaster", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "gaussian_hmm",
                  new FakeModelInfo("gaussian_hmm", "GaussianHmm", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "gmm_hmm", new FakeModelInfo("gmm_hmm", "GmmHmm", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "stray", new FakeModelInfo("stray", "Stray", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "sundial", new FakeModelInfo("sundial", "Timer-Sundial", "BUILT-IN", "ACTIVE")),
              new AbstractMap.SimpleEntry<>(
                  "timer_xl", new FakeModelInfo("timer_xl", "Timer-XL", "BUILT-IN", "ACTIVE")))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void userDefinedModelManagementTestInTree() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      userDefinedModelManagementTest(statement);
    }
  }

  @Test
  public void userDefinedModelManagementTestInTable() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      userDefinedModelManagementTest(statement);
    }
  }

  private void userDefinedModelManagementTest(Statement statement)
      throws SQLException, InterruptedException {
    final String alterConfigSQL = "set configuration \"trusted_uri_pattern\"='.*'";
    final String registerSql =
        "create model operationTest using uri \"" + EXAMPLE_MODEL_PATH + "\"";
    final String showSql = "SHOW MODELS operationTest";
    final String dropSql = "DROP MODEL operationTest";

    statement.execute(alterConfigSQL);
    statement.execute(registerSql);
    boolean loading = true;
    int count = 0;
    for (int retryCnt = 0; retryCnt < 100; retryCnt++) {
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
        while (resultSet.next()) {
          String modelId = resultSet.getString(1);
          String category = resultSet.getString(3);
          String state = resultSet.getString(4);
          assertEquals("operationTest", modelId);
          assertEquals("USER-DEFINED", category);
          if (state.equals("ACTIVE")) {
            loading = false;
            count++;
          } else if (state.equals("LOADING")) {
            break;
          } else {
            fail("Unexpected status of model: " + state);
          }
        }
      }
      if (!loading) {
        break; // Model is loaded successfully
      }
      TimeUnit.SECONDS.sleep(1);
    }
    assertFalse(loading);
    assertEquals(1, count);
    statement.execute(dropSql);
    try (ResultSet resultSet = statement.executeQuery(showSql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
      count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(0, count);
    }
  }

  @Test
  public void dropBuiltInModelErrorTestInTree() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      errorTest(statement, "drop model sundial", "1501: Built-in model sundial can't be removed");
    }
  }

  @Test
  public void dropBuiltInModelErrorTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      errorTest(statement, "drop model sundial", "1501: Built-in model sundial can't be removed");
    }
  }

  @Test
  public void showBuiltInModelTestInTree() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      showBuiltInModelTest(statement);
    }
  }

  @Test
  public void showBuiltInModelTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement(); ) {
      showBuiltInModelTest(statement);
    }
  }

  private void showBuiltInModelTest(Statement statement) throws SQLException {
    int built_in_model_count = 0;
    final String showSql = "SHOW MODELS";
    try (ResultSet resultSet = statement.executeQuery(showSql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
      while (resultSet.next()) {
        built_in_model_count++;
        FakeModelInfo modelInfo =
            new FakeModelInfo(
                resultSet.getString(1),
                resultSet.getString(2),
                resultSet.getString(3),
                resultSet.getString(4));
        assertTrue(BUILT_IN_MODEL_MAP.containsKey(modelInfo.getModelId()));
        assertEquals(BUILT_IN_MODEL_MAP.get(modelInfo.getModelId()), modelInfo);
      }
    }
    assertEquals(BUILT_IN_MODEL_MAP.size(), built_in_model_count);
  }
}
