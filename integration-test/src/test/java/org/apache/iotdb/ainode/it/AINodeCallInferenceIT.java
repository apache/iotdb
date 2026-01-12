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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.BUILTIN_MODEL_MAP;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTree;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeCallInferenceIT {

  private static final String CALL_INFERENCE_SQL_TEMPLATE =
      "CALL INFERENCE(%s, \"SELECT s%d FROM root.AI LIMIT %d\", generateTime=true, outputLength=%d)";
  private static final String CALL_INFERENCE_BY_DEFAULT_SQL_TEMPLATE =
      "CALL INFERENCE(%s, \"SELECT s%d FROM root.AI LIMIT 256\")";
  private static final int DEFAULT_INPUT_LENGTH = 256;
  private static final int DEFAULT_OUTPUT_LENGTH = 48;

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTree();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void callInferenceTest() throws SQLException {
    for (AINodeTestUtils.FakeModelInfo modelInfo : BUILTIN_MODEL_MAP.values()) {
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        callInferenceTest(statement, modelInfo);
        callInferenceByDefaultTest(statement, modelInfo);
      }
    }
  }

  public static void callInferenceTest(Statement statement, AINodeTestUtils.FakeModelInfo modelInfo)
      throws SQLException {
    // Invoke call inference for specified models, there should exist result.
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
        // Ensure the call inference return results
        Assert.assertEquals(DEFAULT_OUTPUT_LENGTH, count);
      }
    }
  }

  public static void callInferenceByDefaultTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) throws SQLException {
    // Invoke call inference for specified models, there should exist result.
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
        // Ensure the call inference return results
        Assert.assertTrue(count > 0);
      }
    }
  }
}
