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
import org.apache.iotdb.itbase.env.BaseEnv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.concurrentInference;

public class AINodeConcurrentInferenceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(AINodeConcurrentInferenceIT.class);

  private static final Map<String, String> MODEL_ID_TO_TYPE_MAP =
      ImmutableMap.of(
          "timer_xl", "Timer-XL",
          "sundial", "Timer-Sundial");

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
                "INSERT INTO root.AI(timestamp, s) VALUES(%d, %f)",
                i, Math.sin(i * Math.PI / 1440)));
      }
    }
  }

  @Test
  public void concurrentCPUCallInferenceTest() throws SQLException, InterruptedException {
    concurrentCPUCallInferenceTest("timer_xl");
    concurrentCPUCallInferenceTest("sundial");
  }

  private void concurrentCPUCallInferenceTest(String modelId)
      throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final int threadCnt = 4;
      final int loop = 10;
      final int predictLength = 96;
      statement.execute(String.format("LOAD MODEL %s TO DEVICES \"cpu\"", modelId));
      checkModelOnSpecifiedDevice(statement, MODEL_ID_TO_TYPE_MAP.get(modelId), "cpu");
      concurrentInference(
          statement,
          String.format(
              "CALL INFERENCE(%s, \"SELECT s FROM root.AI\", predict_length=%d)",
              modelId, predictLength),
          threadCnt,
          loop,
          predictLength);
      statement.execute(String.format("UNLOAD MODEL %s FROM DEVICES \"cpu\"", modelId));
    }
  }

  @Test
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
      statement.execute(String.format("LOAD MODEL %s TO DEVICES \"%s\"", modelId, devices));
      checkModelOnSpecifiedDevice(statement, MODEL_ID_TO_TYPE_MAP.get(modelId), devices);
      concurrentInference(
          statement,
          String.format(
              "CALL INFERENCE(%s, \"SELECT s FROM root.AI\", predict_length=%d)",
              modelId, predictLength),
          threadCnt,
          loop,
          predictLength);
      statement.execute(String.format("UNLOAD MODEL %s FROM DEVICES \"0,1\"", modelId));
    }
  }

  @Test
  public void concurrentCPUForecastTest() throws SQLException, InterruptedException {
    concurrentCPUForecastTest("timer_xl");
    concurrentCPUForecastTest("sundial");
  }

  private void concurrentCPUForecastTest(String modelId) throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final int threadCnt = 4;
      final int loop = 10;
      final int predictLength = 96;
      statement.execute(String.format("LOAD MODEL %s TO DEVICES \"cpu\"", modelId));
      checkModelOnSpecifiedDevice(statement, MODEL_ID_TO_TYPE_MAP.get(modelId), "cpu");
      long startTime = System.currentTimeMillis();
      concurrentInference(
          statement,
          String.format(
              "SELECT * FROM FORECAST(model_id=>'%s', input=>(SELECT time,s FROM root.AI) ORDER BY time), predict_length=>%d",
              modelId, predictLength),
          threadCnt,
          loop,
          predictLength);
      long endTime = System.currentTimeMillis();
      LOGGER.info(
          String.format(
              "Model %s concurrent inference %d reqs (%d threads, %d loops) in CPU takes time: %dms",
              modelId, threadCnt * loop, threadCnt, loop, endTime - startTime));
      statement.execute(String.format("UNLOAD MODEL %s FROM DEVICES \"cpu\"", modelId));
    }
  }

  @Test
  public void concurrentGPUForecastTest() throws SQLException, InterruptedException {
    concurrentGPUForecastTest("timer_xl");
    concurrentGPUForecastTest("sundial");
  }

  public void concurrentGPUForecastTest(String modelId) throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final int threadCnt = 10;
      final int loop = 100;
      final int predictLength = 512;
      final String devices = "0,1";
      statement.execute(String.format("LOAD MODEL %s TO DEVICES \"%s\"", modelId, devices));
      checkModelOnSpecifiedDevice(statement, MODEL_ID_TO_TYPE_MAP.get(modelId), devices);
      long startTime = System.currentTimeMillis();
      concurrentInference(
          statement,
          String.format(
              "SELECT * FROM FORECAST(model_id=>'%s', input=>(SELECT time,s FROM root.AI) ORDER BY time), predict_length=>%d",
              modelId, predictLength),
          threadCnt,
          loop,
          predictLength);
      long endTime = System.currentTimeMillis();
      LOGGER.info(
          String.format(
              "Model %s concurrent inference %d reqs (%d threads, %d loops) in GPU takes time: %dms",
              modelId, threadCnt * loop, threadCnt, loop, endTime - startTime));
      statement.execute(String.format("UNLOAD MODEL %s FROM DEVICES \"0,1\"", modelId));
    }
  }

  private void checkModelOnSpecifiedDevice(Statement statement, String modelType, String device)
      throws SQLException, InterruptedException {
    for (int retry = 0; retry < 10; retry++) {
      Set<String> targetDevices = ImmutableSet.copyOf(device.split(","));
      Set<String> foundDevices = new HashSet<>();
      try (final ResultSet resultSet =
          statement.executeQuery(String.format("SHOW LOADED MODELS %s", device))) {
        while (resultSet.next()) {
          String deviceId = resultSet.getString(1);
          String loadedModelType = resultSet.getString(2);
          int count = resultSet.getInt(3);
          if (loadedModelType.equals(modelType) && targetDevices.contains(deviceId)) {
            Assert.assertTrue(count > 1);
            foundDevices.add(deviceId);
          }
        }
        if (foundDevices.containsAll(targetDevices)) {
          return;
        }
      }
      TimeUnit.SECONDS.sleep(3);
    }
    Assert.fail("Model " + modelType + " is not loaded on device " + device);
  }
}
