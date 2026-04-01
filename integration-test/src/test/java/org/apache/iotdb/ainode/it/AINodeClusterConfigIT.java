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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeClusterConfigIT {

  @Before
  public void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void aiNodeRegisterAndRemoveTestInTree() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      aiNodeRegisterAndRemoveTest(statement);
    }
  }

  @Test
  public void aiNodeRegisterAndRemoveTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      aiNodeRegisterAndRemoveTest(statement);
    }
  }

  private void aiNodeRegisterAndRemoveTest(Statement statement) throws SQLException {
    String show_sql = "SHOW AINODES";
    String title = "NodeID,Status,InternalAddress,InternalPort";
    try (ResultSet resultSet = statement.executeQuery(show_sql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, title);
      int count = 0;
      while (resultSet.next()) {
        assertEquals("2", resultSet.getString(1));
        assertEquals("Running", resultSet.getString(2));
        count++;
      }
      assertEquals(1, count);
    }
    String remove_sql = "REMOVE AINODE";
    statement.execute(remove_sql);
    for (int retry = 0; retry < 500; retry++) {
      try (ResultSet resultSet = statement.executeQuery(show_sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, title);
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        if (count == 0) {
          return; // Successfully removed the AI node
        }
      }
      try {
        Thread.sleep(1000); // Wait before retrying
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Assert.fail("The target AINode is not removed successfully after all retries.");
  }
}
