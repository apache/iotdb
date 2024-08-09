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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDeviceQueryIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testQueryDevice() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use test");
      statement.execute(
          "create table table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT)");
      statement.execute(
          "create table table0(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT)");
      statement.execute(
          "insert into table0(region_id, plant_id, device_id, model, temperature, humidity) values('1', '5', '3', 'A', 37.6, 111.1)");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from table0"),
          "region_id,plant_id,device_id,model,",
          Collections.singleton("1,5,3,A,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from table0"),
          "count(devices),",
          Collections.singleton("1,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from table1"),
          "region_id,plant_id,device_id,model,",
          Collections.emptySet());
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from table1"),
          "count(devices),",
          Collections.singleton("0,"));

      try {
        statement.executeQuery("show devices from table2");
        fail("Show devices shall fail for non-exist table");
      } catch (final Exception e) {
        // Pass
      }

      try {
        statement.executeQuery("count devices from table2");
        fail("Count devices shall fail for non-exist table");
      } catch (final Exception e) {
        // Pass
      }
    }
  }
}
