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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeClusterConfigIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void aiNodeRegisterAndRemoveTest() throws SQLException {
    String show_sql = "SHOW AINODES";
    String title = "NodeID,Status,InternalAddress,InternalPort";

    // Verify AINode exists via both dialects before removal
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      verifyAINodeExists(statement, show_sql, title);
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      verifyAINodeExists(statement, show_sql, title);
    }

    // Remove AINode
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("REMOVE AINODE");
      waitForAINodeRemoval(statement, show_sql, title);
    }

    // Verify removal is visible via table dialect as well
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(show_sql)) {
        checkHeader(resultSet.getMetaData(), title);
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }
    }
  }

  private static void verifyAINodeExists(Statement statement, String showSql, String title)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery(showSql)) {
      checkHeader(resultSet.getMetaData(), title);
      int count = 0;
      while (resultSet.next()) {
        assertEquals("2", resultSet.getString(1));
        assertEquals("Running", resultSet.getString(2));
        count++;
      }
      assertEquals(1, count);
    }
  }

  private static void waitForAINodeRemoval(Statement statement, String showSql, String title)
      throws SQLException {
    for (int retry = 0; retry < 500; retry++) {
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        checkHeader(resultSet.getMetaData(), title);
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        if (count == 0) {
          return;
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Assert.fail("The target AINode is not removed successfully after all retries.");
  }
}
