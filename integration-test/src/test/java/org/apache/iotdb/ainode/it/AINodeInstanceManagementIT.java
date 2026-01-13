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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkModelNotOnSpecifiedDevice;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkModelOnSpecifiedDevice;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;

public class AINodeInstanceManagementIT {

  private static final Set<String> TARGET_DEVICES = new HashSet<>(Arrays.asList("cpu", "0", "1"));

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
  public void basicManagementTestInTreeModel() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      basicManagementTest(statement);
    }
  }

  @Test
  public void basicManagementTestInTableModel() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      basicManagementTest(statement);
    }
  }

  private void basicManagementTest(Statement statement) throws SQLException, InterruptedException {
    // Ensure resources
    try (ResultSet resultSet = statement.executeQuery("SHOW AI_DEVICES")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "DeviceID");
      final Set<String> resultDevices = new HashSet<>();
      while (resultSet.next()) {
        resultDevices.add(resultSet.getString("DeviceID"));
      }
      Assert.assertEquals(TARGET_DEVICES, resultDevices);
    }

    // Load sundial to each device
    statement.execute(String.format("LOAD MODEL sundial TO DEVICES '%s'", TARGET_DEVICES));
    checkModelOnSpecifiedDevice(statement, "sundial", TARGET_DEVICES.toString());

    // Load timer_xl to each device
    statement.execute(String.format("LOAD MODEL timer_xl TO DEVICES '%s'", TARGET_DEVICES));
    checkModelOnSpecifiedDevice(statement, "timer_xl", TARGET_DEVICES.toString());

    // Clean every device
    statement.execute(String.format("UNLOAD MODEL sundial FROM DEVICES '%s'", TARGET_DEVICES));
    statement.execute(String.format("UNLOAD MODEL timer_xl FROM DEVICES '%s'", TARGET_DEVICES));
    checkModelNotOnSpecifiedDevice(statement, "timer_xl", TARGET_DEVICES.toString());
    checkModelNotOnSpecifiedDevice(statement, "sundial", TARGET_DEVICES.toString());
  }

  private static final int LOOP_CNT = 10;

  @Test
  public void repeatLoadAndUnloadTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < LOOP_CNT; i++) {
        statement.execute("LOAD MODEL sundial TO DEVICES \"cpu,0,1\"");
        checkModelOnSpecifiedDevice(statement, "sundial", TARGET_DEVICES.toString());
        statement.execute("UNLOAD MODEL sundial FROM DEVICES \"cpu,0,1\"");
        checkModelNotOnSpecifiedDevice(statement, "sundial", TARGET_DEVICES.toString());
      }
    }
  }

  @Test
  public void concurrentLoadAndUnloadTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < LOOP_CNT; i++) {
        statement.execute("LOAD MODEL sundial TO DEVICES \"cpu,0,1\"");
        statement.execute("UNLOAD MODEL sundial FROM DEVICES \"cpu,0,1\"");
      }
      checkModelNotOnSpecifiedDevice(statement, "sundial", TARGET_DEVICES.toString());
    }
  }

  @Test
  public void failTestInTreeModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      failTest(statement);
    }
  }

  @Test
  public void failTestInTableModel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      failTest(statement);
    }
  }

  private void failTest(Statement statement) {
    errorTest(
        statement,
        "LOAD MODEL unknown TO DEVICES \"cpu,0,1\"",
        "1505: Cannot load model [unknown], because it is neither a built-in nor a fine-tuned model. You can use 'SHOW MODELS' to retrieve the available models.");
    errorTest(
        statement,
        "LOAD MODEL sundial TO DEVICES \"unknown\"",
        "1507: Device ID [unknown] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.");
    errorTest(
        statement,
        "UNLOAD MODEL sundial FROM DEVICES \"unknown\"",
        "1507: Device ID [unknown] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.");
  }
}
