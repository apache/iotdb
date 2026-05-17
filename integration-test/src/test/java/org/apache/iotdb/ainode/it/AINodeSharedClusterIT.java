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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.BUILTIN_MODEL_MAP;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkModelNotOnSpecifiedDevice;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkModelOnSpecifiedDevice;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTree;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Consolidates AINodeDeviceManageIT, AINodeModelManageIT, AINodeCallInferenceIT, AINodeForecastIT,
 * and AINodeInstanceManagementIT into a single class that shares one 1C1D1A cluster, avoiding 5
 * redundant cluster startups (~20 min saved).
 */
@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeSharedClusterIT {

  private static final String TARGET_DEVICES_STR = "0,1";
  private static final Set<String> TARGET_DEVICES =
      new HashSet<>(Arrays.asList(TARGET_DEVICES_STR.split(",")));

  private static final String CALL_INFERENCE_SQL_TEMPLATE =
      "CALL INFERENCE(%s, \"SELECT s%d FROM root.AI LIMIT %d\", generateTime=true, outputLength=%d)";
  private static final String CALL_INFERENCE_BY_DEFAULT_SQL_TEMPLATE =
      "CALL INFERENCE(%s, \"SELECT s%d FROM root.AI LIMIT 256\")";
  private static final int DEFAULT_INPUT_LENGTH = 256;
  private static final int DEFAULT_OUTPUT_LENGTH = 48;

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
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTree();
    prepareDataInTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ========== DeviceManage tests ==========

  @Test
  public void showAIDeviceTestInTree() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      showAIDevicesTest(statement);
    }
  }

  @Test
  public void showAIDeviceTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      showAIDevicesTest(statement);
    }
  }

  private void showAIDevicesTest(Statement statement) throws SQLException {
    final String showSql = "SHOW AI_DEVICES";
    final List<String> expectedDeviceIdList = new LinkedList<>(Arrays.asList("0", "1", "cpu"));
    final List<String> expectedDeviceTypeList =
        new LinkedList<>(Arrays.asList("cuda", "cuda", "cpu"));
    try (ResultSet resultSet = statement.executeQuery(showSql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "DeviceId,DeviceType");
      while (resultSet.next()) {
        String deviceId = resultSet.getString(1);
        String deviceType = resultSet.getString(2);
        Assert.assertEquals(expectedDeviceIdList.remove(0), deviceId);
        Assert.assertEquals(expectedDeviceTypeList.remove(0), deviceType);
      }
    }
  }

  // ========== ModelManage tests ==========

  @Test
  public void userDefinedModelManagementTestInTree() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      FakeModelInfo modelInfo =
          new FakeModelInfo("user_chronos", "custom_t5", "user_defined", "active");
      registerUserDefinedModel(statement, modelInfo, "file:///data/chronos2");
      callInferenceTest(statement, modelInfo);
      dropUserDefinedModel(statement, modelInfo.getModelId());

      modelInfo = new FakeModelInfo("user_mantis", "custom_mantis", "user_defined", "active");
      registerUserDefinedModel(statement, modelInfo, "file:///data/mantis");
      dropUserDefinedModel(statement, modelInfo.getModelId());
    }
  }

  @Test
  public void userDefinedModelManagementTestInTable() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      FakeModelInfo modelInfo =
          new FakeModelInfo("user_chronos", "custom_t5", "user_defined", "active");
      registerUserDefinedModel(statement, modelInfo, "file:///data/chronos2");
      forecastTableFunctionTest(statement, modelInfo);
      dropUserDefinedModel(statement, modelInfo.getModelId());

      modelInfo = new FakeModelInfo("user_mantis", "custom_mantis", "user_defined", "active");
      registerUserDefinedModel(statement, modelInfo, "file:///data/mantis");
      dropUserDefinedModel(statement, modelInfo.getModelId());
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
        Statement statement = connection.createStatement()) {
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
        assertTrue(BUILTIN_MODEL_MAP.containsKey(modelInfo.getModelId()));
        assertEquals(BUILTIN_MODEL_MAP.get(modelInfo.getModelId()), modelInfo);
      }
    }
    assertEquals(BUILTIN_MODEL_MAP.size(), built_in_model_count);
  }

  // ========== CallInference tests ==========

  @Test
  public void callInferenceTest() throws SQLException {
    for (AINodeTestUtils.FakeModelInfo modelInfo : BUILTIN_MODEL_MAP.values()) {
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        callInferenceTest(statement, modelInfo);
        callInferenceByDefaultTest(statement, modelInfo);
        callInferenceErrorTest(statement, modelInfo);
      }
    }
  }

  public static void callInferenceTest(Statement statement, AINodeTestUtils.FakeModelInfo modelInfo)
      throws SQLException {
    for (int i = 0; i < 4; i++) {
      String callInferenceSQL =
          String.format(
              CALL_INFERENCE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              i,
              DEFAULT_INPUT_LENGTH,
              DEFAULT_OUTPUT_LENGTH);
      try (ResultSet resultSet = statement.executeQuery(callInferenceSQL)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,output");
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertEquals(DEFAULT_OUTPUT_LENGTH, count);
      }
    }
  }

  public static void callInferenceByDefaultTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) throws SQLException {
    for (int i = 0; i < 4; i++) {
      String callInferenceSQL =
          String.format(CALL_INFERENCE_BY_DEFAULT_SQL_TEMPLATE, modelInfo.getModelId(), i);
      try (ResultSet resultSet = statement.executeQuery(callInferenceSQL)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "output");
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue(count > 0);
      }
    }
  }

  public static void callInferenceErrorTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) {
    String multiVariateSQL =
        String.format(
            "CALL INFERENCE(%s, \"SELECT s0,s1 FROM root.AI LIMIT 128\", generateTime=true, outputLength=10)",
            modelInfo.getModelId());
    errorTest(
        statement,
        multiVariateSQL,
        "701: Call inference function should not contain more than one input column, found [2] input columns.");
  }

  // ========== Forecast tests ==========

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

    String invalidOutputLengthSQLWithZero =
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
    errorTest(
        statement, invalidOutputLengthSQLWithZero, "701: OUTPUT_LENGTH should be greater than 0");

    String invalidOutputLengthSQLWithOutOfRange =
        String.format(
            FORECAST_TABLE_FUNCTION_SQL_TEMPLATE,
            modelInfo.getModelId(),
            0,
            5760,
            2880,
            5760,
            2881,
            1,
            "time");
    errorTest(
        statement,
        invalidOutputLengthSQLWithOutOfRange,
        "1599: Error occurred while executing forecast:[Attribute output_length expect value between 1 and 2880, got 2881 instead.]");

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

  // ========== InstanceManagement tests ==========

  @Test
  public void instanceBasicManagementTestInTreeModel() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      instanceBasicManagementTest(statement);
    }
  }

  @Test
  public void instanceBasicManagementTestInTableModel() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      instanceBasicManagementTest(statement);
    }
  }

  private void instanceBasicManagementTest(Statement statement)
      throws SQLException, InterruptedException {
    try (ResultSet resultSet = statement.executeQuery("SHOW AI_DEVICES")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "DeviceId,DeviceType");
      final Set<String> resultDevices = new HashSet<>();
      while (resultSet.next()) {
        resultDevices.add(resultSet.getString("DeviceId"));
      }
      Set<String> expected = new HashSet<>(TARGET_DEVICES);
      expected.add("cpu");
      Assert.assertEquals(expected, resultDevices);
    }

    statement.execute(String.format("LOAD MODEL sundial TO DEVICES '%s'", TARGET_DEVICES_STR));
    checkModelOnSpecifiedDevice(statement, "sundial", TARGET_DEVICES_STR);
    statement.execute(String.format("UNLOAD MODEL sundial FROM DEVICES '%s'", TARGET_DEVICES_STR));
    checkModelNotOnSpecifiedDevice(statement, "sundial", TARGET_DEVICES_STR);

    statement.execute(String.format("LOAD MODEL timer_xl TO DEVICES '%s'", TARGET_DEVICES_STR));
    checkModelOnSpecifiedDevice(statement, "timer_xl", TARGET_DEVICES_STR);
    statement.execute(String.format("UNLOAD MODEL timer_xl FROM DEVICES '%s'", TARGET_DEVICES_STR));
    checkModelNotOnSpecifiedDevice(statement, "timer_xl", TARGET_DEVICES_STR);
  }

  @Test
  public void instanceFailTestInTreeModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      instanceFailTest(statement);
    }
  }

  @Test
  public void instanceFailTestInTableModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      instanceFailTest(statement);
    }
  }

  private void instanceFailTest(Statement statement) {
    errorTest(
        statement,
        "LOAD MODEL unknown TO DEVICES 'cpu,0,1'",
        "1504: Model [unknown] is not registered yet. You can use 'SHOW MODELS' to retrieve the available models.");
    errorTest(
        statement,
        "LOAD MODEL sundial TO DEVICES '999'",
        "1508: AIDevice ID [999] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.");
    errorTest(
        statement,
        "UNLOAD MODEL sundial FROM DEVICES '999'",
        "1508: AIDevice ID [999] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.");
    errorTest(
        statement,
        "LOAD MODEL sundial TO DEVICES '0,0'",
        "1509: Device ID list contains duplicate entries.");
    errorTest(
        statement,
        "UNLOAD MODEL sundial FROM DEVICES '0,0'",
        "1510: Device ID list contains duplicate entries.");
  }

  // ========== Helper methods (from ModelManageIT) ==========

  private static void registerUserDefinedModel(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo, String uri)
      throws SQLException, InterruptedException {
    String modelId = modelInfo.getModelId();
    String modelType = modelInfo.getModelType();
    String category = modelInfo.getCategory();
    final String CREATE_MODEL_TEMPLATE = "create model %s using uri \"%s\"";
    final String alterConfigSQL = "set configuration \"trusted_uri_pattern\"='.*'";
    final String registerSql = String.format(CREATE_MODEL_TEMPLATE, modelId, uri);
    final String showSql = String.format("SHOW MODELS %s", modelId);
    statement.execute(alterConfigSQL);
    statement.execute(registerSql);
    boolean loading = true;
    for (int retryCnt = 0; retryCnt < 100; retryCnt++) {
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
        while (resultSet.next()) {
          String resultModelId = resultSet.getString(1);
          String resultModelType = resultSet.getString(2);
          String resultCategory = resultSet.getString(3);
          String state = resultSet.getString(4);
          assertEquals(modelId, resultModelId);
          assertEquals(modelType, resultModelType);
          assertEquals(category, resultCategory);
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
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    assertFalse(loading);
  }

  private static void dropUserDefinedModel(Statement statement, String modelId)
      throws SQLException {
    final String showSql = String.format("SHOW MODELS %s", modelId);
    final String dropSql = String.format("DROP MODEL %s", modelId);
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
}
