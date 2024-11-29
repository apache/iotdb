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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDisableDeletionTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000);
    // Adjust memstable threshold size to make it flush automatically
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDeletionDisabled() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use test");
      statement.execute(
          "CREATE TABLE vehicle1(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)");

      statement.execute("insert into vehicle1(time, deviceId, s0) values (10, 'd0', 310)");
      statement.execute("insert into vehicle1(time, deviceId, s3) values (10, 'd0','text')");
      statement.execute("insert into vehicle1(time, deviceId, s4) values (10, 'd0',true)");

      try {
        statement.execute("DELETE FROM vehicle1  WHERE s0 <= 300 AND s0 > 0");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Delete statement is not supported yet.", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE s3 = 'text'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Delete statement is not supported yet.", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE s4 != true");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Delete statement is not supported yet.", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 and deviceId > 'd0'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Delete statement is not supported yet.", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 and deviceId is not null");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Delete statement is not supported yet.", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 and deviceId = null");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Delete statement is not supported yet.", e.getMessage());
      }
    }
  }
}
