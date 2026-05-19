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
import org.apache.iotdb.itbase.category.AINodeIT;
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

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.BUILTIN_MODEL_MAP;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Metadata-only AINode tests that don't drive inference or bind GPU devices, so they can run on a
 * plain CPU runner. Tests that do exercise CUDA paths live in {@link AINodeSharedClusterIT}.
 */
@RunWith(IoTDBTestRunner.class)
@Category({AINodeIT.class})
public class AINodeBasicIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
    int builtInModelCount = 0;
    final String showSql = "SHOW MODELS";
    try (ResultSet resultSet = statement.executeQuery(showSql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
      while (resultSet.next()) {
        builtInModelCount++;
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
    assertEquals(BUILTIN_MODEL_MAP.size(), builtInModelCount);
  }
}
