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

package org.apache.iotdb.confignode.it.database;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Verifies the single-RegionGroup invariant of the system database. */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSystemDatabaseRegionGroupIT {

  private static final String SYSTEM_DATABASE = "root.__system";
  private static final int DEFAULT_REGION_GROUP_NUM = 3;

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSystemDatabaseRegionGroupNumUnderAutoPolicy() throws Exception {
    assertSystemDatabaseUsesSingleRegionGroup("AUTO");
  }

  @Test
  public void testSystemDatabaseRegionGroupNumUnderCustomPolicy() throws Exception {
    assertSystemDatabaseUsesSingleRegionGroup("CUSTOM");
  }

  private static void assertSystemDatabaseUsesSingleRegionGroup(String policy) throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy(policy)
        .setDataRegionGroupExtensionPolicy(policy)
        .setDefaultSchemaRegionGroupNumPerDatabase(DEFAULT_REGION_GROUP_NUM)
        .setDefaultDataRegionGroupNumPerDatabase(DEFAULT_REGION_GROUP_NUM);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO root.__system.audit.d1(timestamp, s) VALUES(1, 1)");
      assertSingleRegionGroup(statement);
    }
  }

  private static void assertSingleRegionGroup(Statement statement) throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery("SHOW DATABASES DETAILS " + SYSTEM_DATABASE)) {
      Assert.assertTrue("Expected system database to be created", resultSet.next());
      Assert.assertEquals(SYSTEM_DATABASE, resultSet.getString("Database"));
      Assert.assertEquals(1, resultSet.getInt("SchemaRegionGroupNum"));
      Assert.assertEquals(1, resultSet.getInt("MaxSchemaRegionGroupNum"));
      Assert.assertEquals(1, resultSet.getInt("DataRegionGroupNum"));
      Assert.assertEquals(1, resultSet.getInt("MaxDataRegionGroupNum"));
      Assert.assertFalse(resultSet.next());
    }
  }
}
