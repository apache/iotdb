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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.concurrentInference;

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
                "INSERT INTO root.AI(timestamp, s) VALUES(%d, %f)",
                i, Math.sin(i * Math.PI / 1440)));
      }
    }
  }

  @Test
  public void concurrentCPUCallInferenceTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("LOAD MODEL sundial TO DEVICES \"cpu\"");
      concurrentInference(statement, "CALL INFERENCE(sundial, \"SELECT s FROM root.AI\")", 4, 10);
    }
  }

  @Test
  public void concurrentGPUCallInferenceTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("LOAD MODEL sundial TO DEVICES \"0,1\"");
      concurrentInference(statement, "CALL INFERENCE(sundial, \"SELECT s FROM root.AI\")", 10, 100);
    }
  }

  @Test
  public void concurrentCPUForecastTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final int threadCnt = 4;
      final int loop = 10;
      statement.execute("LOAD MODEL sundial TO DEVICES \"cpu\"");
      long startTime = System.currentTimeMillis();
      concurrentInference(
          statement,
          "SELECT * FROM FORECAST(model_id=>'sundial', input=>(SELECT time,s FROM root.AI) ORDER BY time)",
          threadCnt,
          loop);
      long endTime = System.currentTimeMillis();
      LOGGER.info(
          String.format(
              "Timer-Sundial concurrent inference %d reqs (%d threads, %d loops) in CPU takes time: %dms",
              threadCnt * loop, threadCnt, loop, endTime - startTime));
    }
  }

  @Test
  public void concurrentGPUForecastTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final int threadCnt = 10;
      final int loop = 100;
      statement.execute("LOAD MODEL sundial TO DEVICES \"0,1\"");
      long startTime = System.currentTimeMillis();
      concurrentInference(
          statement,
          "SELECT * FROM FORECAST(model_id=>'sundial', input=>(SELECT time,s FROM root.AI) ORDER BY time)",
          threadCnt,
          loop);
      long endTime = System.currentTimeMillis();
      LOGGER.info(
          String.format(
              "Timer-Sundial concurrent inference %d reqs (%d threads, %d loops) in GPU takes time: %dms",
              threadCnt * loop, threadCnt, loop, endTime - startTime));
    }
  }
}
