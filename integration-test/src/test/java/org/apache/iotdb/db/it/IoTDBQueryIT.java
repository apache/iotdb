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

import org.junit.After;
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

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBQueryIT {

  private static final String DATABASE = "root.query_it";
  private static final String DEVICE = DATABASE + ".d1";
  private static final int ROW_COUNT = 100;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void cleanUp() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE DATABASE " + DATABASE);
    } catch (SQLException ignored) {
      // The database may not have been created when the case fails early.
    }
  }

  /**
   * Inserts rows directly and reads them back, covering an ordered full scan and a filtered query.
   */
  @Test
  public void testForQuery() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("CREATE TIMESERIES " + DEVICE + ".s1 WITH DATATYPE=INT64, ENCODING=PLAIN");

      for (int row = 0; row < ROW_COUNT; row++) {
        statement.execute(
            "INSERT INTO " + DEVICE + "(timestamp, s1) VALUES (" + row + ", " + row + ")");
      }

      long expected = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM " + DEVICE + " ORDER BY time")) {
        while (resultSet.next()) {
          assertEquals(expected, resultSet.getLong(DEVICE + ".s1"));
          expected++;
        }
      }
      assertEquals(ROW_COUNT, expected);

      int filtered = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM " + DEVICE + " WHERE s1 >= 90")) {
        while (resultSet.next()) {
          filtered++;
        }
      }
      assertEquals(10, filtered);
    }
  }
}
