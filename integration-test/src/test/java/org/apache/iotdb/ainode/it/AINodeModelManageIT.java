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
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.ainode.it.AINodeCallInferenceIT.callInferenceTest;
import static org.apache.iotdb.ainode.it.AINodeForecastIT.forecastTableFunctionTest;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTree;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeModelManageIT {

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTree();
    prepareDataInTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void userDefinedModelManagementTestInTree() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      registerUserDefinedModel(statement);
      callInferenceTest(
          statement, new FakeModelInfo("user_chronos", "custom_t5", "user_defined", "active"));
      dropUserDefinedModel(statement);
      errorTest(
          statement,
          "create model origin_chronos using uri \"file:///data/chronos2_origin\"",
          "1505: 't5' is already used by a Transformers config, pick another name.");
      statement.execute("drop model origin_chronos");
    }
  }

  @Test
  public void userDefinedModelManagementTestInTable() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      registerUserDefinedModel(statement);
      forecastTableFunctionTest(
          statement, new FakeModelInfo("user_chronos", "custom_t5", "user_defined", "active"));
      dropUserDefinedModel(statement);
      errorTest(
          statement,
          "create model origin_chronos using uri \"file:///data/chronos2_origin\"",
          "1505: 't5' is already used by a Transformers config, pick another name.");
      statement.execute("drop model origin_chronos");
    }
  }

  private void registerUserDefinedModel(Statement statement)
      throws SQLException, InterruptedException {
    final String alterConfigSQL = "set configuration \"trusted_uri_pattern\"='.*'";
    final String registerSql = "create model user_chronos using uri \"file:///data/chronos2\"";
    final String showSql = "SHOW MODELS user_chronos";
    statement.execute(alterConfigSQL);
    statement.execute(registerSql);
    boolean loading = true;
    for (int retryCnt = 0; retryCnt < 100; retryCnt++) {
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
        while (resultSet.next()) {
          String modelId = resultSet.getString(1);
          String modelType = resultSet.getString(2);
          String category = resultSet.getString(3);
          String state = resultSet.getString(4);
          assertEquals("user_chronos", modelId);
          assertEquals("custom_t5", modelType);
          assertEquals("user_defined", category);
          if (state.equals("active")) {
            loading = false;
          } else if (state.equals("loading")) {
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
  }

  private void dropUserDefinedModel(Statement statement) throws SQLException {
    final String showSql = "SHOW MODELS user_chronos";
    final String dropSql = "DROP MODEL user_chronos";
    statement.execute(dropSql);
    try (ResultSet resultSet = statement.executeQuery(showSql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
      int count = 0;
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
      errorTest(statement, "drop model sundial", "1506: Cannot delete built-in model: sundial");
    }
  }

  @Test
  public void dropBuiltInModelErrorTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      errorTest(statement, "drop model sundial", "1506: Cannot delete built-in model: sundial");
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
        assertTrue(AINodeTestUtils.BUILTIN_MODEL_MAP.containsKey(modelInfo.getModelId()));
        assertEquals(AINodeTestUtils.BUILTIN_MODEL_MAP.get(modelInfo.getModelId()), modelInfo);
      }
    }
    assertEquals(AINodeTestUtils.BUILTIN_MODEL_MAP.size(), built_in_model_count);
  }
}
