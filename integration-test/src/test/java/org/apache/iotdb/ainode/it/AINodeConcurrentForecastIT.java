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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkModelNotOnSpecifiedDevice;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkModelOnSpecifiedDevice;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.concurrentInference;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeConcurrentForecastIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(AINodeConcurrentForecastIT.class);

  private static final List<AINodeTestUtils.FakeModelInfo> MODEL_LIST =
      Arrays.asList(
          new AINodeTestUtils.FakeModelInfo("sundial", "sundial", "builtin", "active"),
          new AINodeTestUtils.FakeModelInfo("timer_xl", "timer", "builtin", "active"));

  private static final String FORECAST_TABLE_FUNCTION_SQL_TEMPLATE =
      "SELECT * FROM FORECAST(model_id=>'%s', targets=>(SELECT time,s FROM root.AI) ORDER BY time, output_length=>%d)";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataForTableModel();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
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

  @Test
  public void concurrentForecastTest() throws SQLException, InterruptedException {
    for (AINodeTestUtils.FakeModelInfo modelInfo : MODEL_LIST) {
      concurrentGPUForecastTest(modelInfo, "0,1");
      // TODO: Enable cpu test after optimize memory consumption
      //      concurrentGPUForecastTest(modelInfo, "cpu");
    }
  }

  public void concurrentGPUForecastTest(AINodeTestUtils.FakeModelInfo modelInfo, String devices)
      throws SQLException, InterruptedException {
    final int forecastLength = 512;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // Single forecast request can be processed successfully
      final String forecastSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_SQL_TEMPLATE, modelInfo.getModelId(), forecastLength);
      final int threadCnt = 10;
      final int loop = 100;
      statement.execute(
          String.format("LOAD MODEL %s TO DEVICES '%s'", modelInfo.getModelId(), devices));
      checkModelOnSpecifiedDevice(statement, modelInfo.getModelId(), devices);
      long startTime = System.currentTimeMillis();
      concurrentInference(statement, forecastSQL, threadCnt, loop, forecastLength);
      long endTime = System.currentTimeMillis();
      LOGGER.info(
          String.format(
              "Model %s concurrent inference %d reqs (%d threads, %d loops) in GPU takes time: %dms",
              modelInfo.getModelId(), threadCnt * loop, threadCnt, loop, endTime - startTime));
      statement.execute(
          String.format("UNLOAD MODEL %s FROM DEVICES '%s'", modelInfo.getModelId(), devices));
      checkModelNotOnSpecifiedDevice(statement, modelInfo.getModelId(), devices);
    }
  }
}
