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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.AIClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.concurrentInference;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeConcurrentInferenceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(AINodeConcurrentInferenceIT.class);

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataForTreeModel();
    prepareDataForTableModel();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareDataForTreeModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.AI");
      statement.execute("CREATE TIMESERIES root.AI.s WITH DATATYPE=DOUBLE, ENCODING=RLE");
      for (int i = 0; i < 2880; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.AI(timestamp, s) VALUES(%d, %f)",
                i, Math.sin(i * Math.PI / 1440)));
      }
    }
  }

  private static void prepareDataForTableModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root");
      statement.execute("CREATE TABLE root.AI (s DOUBLE FIELD)");
      for (int i = 0; i < 2880; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.AI(time, s) VALUES(%d, %f)", i, Math.sin(i * Math.PI / 1440)));
      }
    }
  }

  //  @Test
  public void concurrentGPUCallInferenceTest() throws SQLException, InterruptedException {
    concurrentGPUCallInferenceTest("timer_xl");
    concurrentGPUCallInferenceTest("sundial");
  }

  private void concurrentGPUCallInferenceTest(String modelId)
      throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final int threadCnt = 10;
      final int loop = 100;
      final int predictLength = 512;
      final String devices = "0,1";
      statement.execute(String.format("LOAD MODEL %s TO DEVICES '%s'", modelId, devices));
      checkModelOnSpecifiedDevice(statement, modelId, devices);
      concurrentInference(
          statement,
          String.format(
              "CALL INFERENCE(%s, 'SELECT s FROM root.AI', predict_length=%d)",
              modelId, predictLength),
          threadCnt,
          loop,
          predictLength);
      statement.execute(String.format("UNLOAD MODEL %s FROM DEVICES '0,1'", modelId));
    }
  }

  String forecastTableFunctionSql =
      "SELECT * FROM FORECAST(model_id=>'%s', input=>(SELECT time,s FROM root.AI) ORDER BY time), predict_length=>%d";
  String forecastUDTFSql =
      "SELECT forecast(s, 'MODEL_ID'='%s', 'PREDICT_LENGTH'='%d') FROM root.AI";

  @Test
  public void concurrentGPUForecastTest() throws SQLException, InterruptedException {
    concurrentGPUForecastTest("timer_xl", forecastUDTFSql);
    concurrentGPUForecastTest("sundial", forecastUDTFSql);
    concurrentGPUForecastTest("timer_xl", forecastTableFunctionSql);
    concurrentGPUForecastTest("sundial", forecastTableFunctionSql);
  }

  public void concurrentGPUForecastTest(String modelId, String selectSql)
      throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final int threadCnt = 10;
      final int loop = 100;
      final int predictLength = 512;
      final String devices = "0,1";
      statement.execute(String.format("LOAD MODEL %s TO DEVICES '%s'", modelId, devices));
      checkModelOnSpecifiedDevice(statement, modelId, devices);
      long startTime = System.currentTimeMillis();
      concurrentInference(
          statement,
          String.format(selectSql, modelId, predictLength),
          threadCnt,
          loop,
          predictLength);
      long endTime = System.currentTimeMillis();
      LOGGER.info(
          String.format(
              "Model %s concurrent inference %d reqs (%d threads, %d loops) in GPU takes time: %dms",
              modelId, threadCnt * loop, threadCnt, loop, endTime - startTime));
      statement.execute(String.format("UNLOAD MODEL %s FROM DEVICES '0,1'", modelId));
    }
  }

  private void checkModelOnSpecifiedDevice(Statement statement, String modelId, String device)
      throws SQLException, InterruptedException {
    Set<String> targetDevices = ImmutableSet.copyOf(device.split(","));
    LOGGER.info("Checking model: {} on target devices: {}", modelId, targetDevices);
    for (int retry = 0; retry < 200; retry++) {
      Set<String> foundDevices = new HashSet<>();
      try (final ResultSet resultSet =
          statement.executeQuery(String.format("SHOW LOADED MODELS '%s'", device))) {
        while (resultSet.next()) {
          String deviceId = resultSet.getString("DeviceId");
          String loadedModelId = resultSet.getString("ModelId");
          int count = resultSet.getInt("Count(instances)");
          LOGGER.info("Model {} found in device {}, count {}", loadedModelId, deviceId, count);
          if (loadedModelId.equals(modelId) && targetDevices.contains(deviceId) && count > 0) {
            foundDevices.add(deviceId);
            LOGGER.info("Model {} is loaded to device {}", modelId, device);
          }
        }
        if (foundDevices.containsAll(targetDevices)) {
          LOGGER.info("Model {} is loaded to devices {}, start testing", modelId, targetDevices);
          return;
        }
      }
      TimeUnit.SECONDS.sleep(3);
    }
    Assert.fail("Model " + modelId + " is not loaded on device " + device);
  }
}
