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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDatabaseMaxRegionGroupNumIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setDefaultDataRegionGroupNumPerDatabase(2);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCreateAndAlterMaxRegionGroupNum() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create database test_create with(max_schema_region_group_num=3, max_data_region_group_num=4)");
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select database, max_schema_region_group_num, max_data_region_group_num "
                  + "from information_schema.databases where database = 'test_create'"),
          "database,max_schema_region_group_num,max_data_region_group_num,",
          Collections.singleton("test_create,3,4,"));

      statement.execute("create database test_alter");
      statement.execute(
          "alter database test_alter set properties max_schema_region_group_num=3, max_data_region_group_num=4");
      try (final ResultSet resultSet = statement.executeQuery("show databases details")) {
        boolean found = false;
        while (resultSet.next()) {
          if (!"test_alter".equals(resultSet.getString("Database"))) {
            continue;
          }
          found = true;
          org.junit.Assert.assertEquals(3, resultSet.getInt("MaxSchemaRegionGroupNum"));
          org.junit.Assert.assertEquals(4, resultSet.getInt("MaxDataRegionGroupNum"));
        }
        org.junit.Assert.assertTrue(found);
      }

      Assert.assertThrows(
          SQLException.class,
          () ->
              statement.execute(
                  "create database test_deprecated with(schema_region_group_num=4, data_region_group_num=5)"));
    }
  }
}
