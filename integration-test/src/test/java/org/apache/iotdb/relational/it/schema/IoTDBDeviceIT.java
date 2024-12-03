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
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDeviceIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultSchemaRegionGroupNumPerDatabase(2);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDevice() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      // Prepare data
      statement.execute("create database test");
      statement.execute("use test");
      statement.execute(
          "create table table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT)");
      statement.execute(
          "create table table0(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT)");
      statement.execute(
          "insert into table0(region_id, plant_id, device_id, model, temperature, humidity) values('1', '5', '3', 'A', 37.6, 111.1)");

      // Test plain show / count
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

      // Test show / count with where expression
      // Test AND
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "show devices from table0 where region_id between '0' and '2' and model = 'A'"),
          "region_id,plant_id,device_id,model,",
          Collections.singleton("1,5,3,A,"));
      // Test OR
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "count devices from table0 where region_id = '1' or plant_id like '%'"),
          "count(devices),",
          Collections.singleton("1,"));
      // Test complicated query
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "show devices from table0 where region_id < plant_id offset 0 limit 1"),
          "region_id,plant_id,device_id,model,",
          Collections.singleton("1,5,3,A,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from table0 where region_id < plant_id limit 0"),
          "region_id,plant_id,device_id,model,",
          Collections.emptySet());
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from table0 where region_id < plant_id"),
          "count(devices),",
          Collections.singleton("1,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "count devices from table0 where substring(region_id, cast((cast(plant_id as int32) - 4) as int32), 1) < plant_id"),
          "count(devices),",
          Collections.singleton("1,"));
      // Test get from cache
      statement.executeQuery(
          "select * from table0 where region_id = '1' and plant_id in ('3', '5') and device_id = '3'");
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "show devices from table0 where region_id = '1' and plant_id in ('3', '5') and device_id = '3' offset 0 limit ALL"),
          "region_id,plant_id,device_id,model,",
          Collections.singleton("1,5,3,A,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "show devices from table0 where region_id = '1' and plant_id in ('3', '5') and device_id = '3' offset 100000000000 limit 100000000000"),
          "region_id,plant_id,device_id,model,",
          Collections.emptySet());
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "count devices from table0 where region_id = '1' and plant_id in ('3', '5') and device_id = '3'"),
          "count(devices),",
          Collections.singleton("1,"));
      // Test filter
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from table0 where region_id >= '2'"),
          "count(devices),",
          Collections.singleton("0,"));
      // Test cache with complicated filter
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "show devices from table0 where region_id = '1' and plant_id in ('3', '5') and device_id = '3' and device_id between region_id and plant_id"),
          "region_id,plant_id,device_id,model,",
          Collections.singleton("1,5,3,A,"));

      try {
        statement.executeQuery("show devices from table2");
        fail("Show devices shall fail for non-exist table");
      } catch (final Exception e) {
        assertEquals("701: Table 'test.table2' does not exist.", e.getMessage());
      }

      try {
        statement.executeQuery("count devices from table2");
        fail("Count devices shall fail for non-exist table");
      } catch (final Exception e) {
        assertEquals("701: Table 'test.table2' does not exist.", e.getMessage());
      }

      try {
        statement.executeQuery("show devices from table0 where temperature = 37.6");
        fail("Show devices shall fail for measurement predicate");
      } catch (final Exception e) {
        assertEquals("701: Column 'temperature' cannot be resolved", e.getMessage());
      }

      try {
        statement.executeQuery("count devices from table0 where a = 1");
        fail("Count devices shall fail for non-exist column");
      } catch (final Exception e) {
        assertEquals("701: Column 'a' cannot be resolved", e.getMessage());
      }

      // Test fully qualified name
      statement.execute("create database test2");
      statement.execute("use test2");
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from test.table0"),
          "count(devices),",
          Collections.singleton("1,"));

      // Test update
      statement.execute("use test");
      try {
        statement.execute("update table2 set model = '1'");
        fail("Update shall fail for non-exist table");
      } catch (final Exception e) {
        assertEquals("701: Table 'test.table2' does not exist.", e.getMessage());
      }

      try {
        statement.execute("update table0 set device_id = '1'");
        fail("Update shall fail for id");
      } catch (final Exception e) {
        assertEquals("701: Update can only specify attribute columns.", e.getMessage());
      }

      try {
        statement.execute("update table0 set model = '1', model = '2'");
        fail("Update shall fail if an attribute occurs twice");
      } catch (final Exception e) {
        assertEquals("701: Update attribute shall specify a attribute only once.", e.getMessage());
      }

      try {
        statement.execute("update table0 set col = '1'");
        fail("Update shall fail for non-exist column");
      } catch (final Exception e) {
        assertEquals("701: Column 'col' cannot be resolved", e.getMessage());
      }

      try {
        statement.execute("update table0 set model = cast(device_id as int32)");
        fail("Update shall fail for non-exist column");
      } catch (final Exception e) {
        assertEquals(
            "507: Result type mismatch for attribute 'model', expected class org.apache.tsfile.utils.Binary, actual class java.lang.Integer",
            e.getMessage());
      }

      // Test null
      statement.execute("update table0 set model = null where model <> substring(device_id, 1, 1)");
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "show devices from table0 where substring(region_id, 1, 1) in ('1', '2') and 1 + 1 = 2"),
          "region_id,plant_id,device_id,model,",
          Collections.singleton("1,5,3,null,"));

      // Test common result column
      statement.execute(
          "update table0 set model = substring(device_id, 1, 1) where cast(region_id as int32) + cast(plant_id as int32) = 6 and region_id = '1'");
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "show devices from table0 where substring(region_id, 1, 1) in ('1', '2') and 1 + 1 = 2"),
          "region_id,plant_id,device_id,model,",
          Collections.singleton("1,5,3,3,"));

      // Test limit / offset from multi regions
      statement.execute(
          "insert into table0(region_id, plant_id, device_id, model, temperature, humidity) values('2', '5', '3', 'A', 37.6, 111.1)");
      TestUtils.assertResultSetSize(
          statement.executeQuery("show devices from table0 offset 1 limit 1"), 1);

      // TODO: Reopen
      if (false) {
        // Test delete devices
        statement.execute("delete devices from table0 where region_id = '1' and plant_id = '5'");
        TestUtils.assertResultSetSize(statement.executeQuery("show devices from table0"), 1);

        // Test successfully invalidate cache
        statement.execute(
            "insert into table0(region_id, plant_id, device_id, model, temperature, humidity) values('1', '5', '3', 'A', 37.6, 111.1)");
        TestUtils.assertResultSetSize(statement.executeQuery("show devices from table0"), 2);

        // Test successfully delete data
        TestUtils.assertResultSetSize(
            statement.executeQuery("select * from table0 where region_id = '1'"), 1);
      }
    }
  }
}
