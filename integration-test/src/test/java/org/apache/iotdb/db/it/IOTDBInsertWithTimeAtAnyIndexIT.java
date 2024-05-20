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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IOTDBInsertWithTimeAtAnyIndexIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertTimeAtAnyIndex() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch("insert into root.db.d1(s1, s2, time) aligned values (2, 3, 1)");
      statement.addBatch("insert into root.db.d1(s1, time, s2) aligned values (20, 10, 30)");
      statement.addBatch("insert into root.db.d1(`time`, s1, s2) aligned values (100, 200, 300)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select s1 from root.db.d1")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertEquals(2, resultSet.getDouble(2), 0.00001);
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getLong(1));
        assertEquals(20, resultSet.getDouble(2), 0.00001);
        assertTrue(resultSet.next());
        assertEquals(100, resultSet.getLong(1));
        assertEquals(200, resultSet.getDouble(2), 0.00001);
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertMultiTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.addBatch(
            "insert into root.db.d1(s1, s2, time, time) aligned values (2, 3, 1, 1)");
        statement.executeBatch();
        fail();
      } catch (SQLException e) {
        // expected
      }

    } catch (SQLException e) {
      fail();
    }
  }
}
