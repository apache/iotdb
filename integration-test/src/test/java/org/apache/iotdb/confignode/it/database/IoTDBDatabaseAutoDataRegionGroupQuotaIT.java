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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

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
@Category({ClusterIT.class})
public class IoTDBDatabaseAutoDataRegionGroupQuotaIT {

  private static final int CONFIG_NODE_NUM = 1;
  private static final int DATA_NODE_NUM = 3;
  private static final int SCHEMA_REPLICATION_FACTOR = 3;
  private static final int DATA_REPLICATION_FACTOR = 1;
  private static final int DATA_REGION_PER_DATA_NODE = 2;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("AUTO")
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setSchemaReplicationFactor(SCHEMA_REPLICATION_FACTOR)
        .setDataReplicationFactor(DATA_REPLICATION_FACTOR)
        .setDataRegionPerDataNode(DATA_REGION_PER_DATA_NODE);
    EnvFactory.getEnv().initClusterEnvironment(CONFIG_NODE_NUM, DATA_NODE_NUM);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMaxDataRegionGroupNumUsesDataReplicationFactor() throws SQLException {
    int expectedMaxDataRegionGroupNum =
        (int)
            Math.ceil((double) DATA_REGION_PER_DATA_NODE * DATA_NODE_NUM / DATA_REPLICATION_FACTOR);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.data_rf WITH MAX_SCHEMA_REGION_GROUP_NUM=2");

      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES DETAILS root.data_rf")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2, resultSet.getInt("MaxSchemaRegionGroupNum"));
        Assert.assertEquals(
            expectedMaxDataRegionGroupNum, resultSet.getInt("MaxDataRegionGroupNum"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testMaxDataRegionGroupNumRejectedUnderAutoPolicy() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      SQLException createException =
          Assert.assertThrows(
              SQLException.class,
              () ->
                  statement.execute(
                      "CREATE DATABASE root.auto_create WITH MAX_DATA_REGION_GROUP_NUM=4"));
      Assert.assertTrue(
          createException
              .getMessage()
              .contains(
                  "max_data_region_group_num can only be set when "
                      + "data_region_group_extension_policy is CUSTOM"));

      statement.execute("CREATE DATABASE root.auto_alter");
      SQLException alterException =
          Assert.assertThrows(
              SQLException.class,
              () ->
                  statement.execute(
                      "ALTER DATABASE root.auto_alter WITH MAX_DATA_REGION_GROUP_NUM=4"));
      Assert.assertTrue(
          alterException
              .getMessage()
              .contains(
                  "max_data_region_group_num can only be set when "
                      + "data_region_group_extension_policy is CUSTOM"));
    }
  }
}
