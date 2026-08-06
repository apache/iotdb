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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDatabaseMixedRegionGroupPolicyIT {

  private static final int DATA_REGION_PER_DATA_NODE = 4;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("AUTO")
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataReplicationFactor(1)
        .setDataRegionPerDataNode(DATA_REGION_PER_DATA_NODE);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAutoPolicyStillAdjustsWhenTheOtherPolicyIsCustom() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.mixed WITH MAX_SCHEMA_REGION_GROUP_NUM=2");

      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES DETAILS root.mixed")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2, resultSet.getInt("MaxSchemaRegionGroupNum"));
        Assert.assertEquals(DATA_REGION_PER_DATA_NODE, resultSet.getInt("MaxDataRegionGroupNum"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
