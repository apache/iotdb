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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBDeviceTemplateIT {

  private static final String DATABASE = "root.device_template_ha";
  private static final String TEMPLATE = "temp1";
  private static final String DEVICE = DATABASE + ".dev01";

  @Test
  public void testDeactivateTemplateWithOneDataNodeDown() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);
    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setMetadataLeaseFenceMs(20000);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);

    try {
      final DataNodeWrapper liveDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      final DataNodeWrapper victimDataNode = EnvFactory.getEnv().getDataNodeWrapper(2);
      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      liveDataNode,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TREE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DATABASE " + DATABASE);
        statement.execute("CREATE DEVICE TEMPLATE " + TEMPLATE + " (s1 INT32, s2 INT64)");
        statement.execute("SET DEVICE TEMPLATE " + TEMPLATE + " TO " + DATABASE);
        statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON " + DEVICE);
        statement.execute("INSERT INTO " + DEVICE + "(time, s1, s2) VALUES(1, 1, 1)");

        victimDataNode.stop();
        Assert.assertFalse("victim DataNode should be stopped", victimDataNode.isAlive());

        statement.execute("DEACTIVATE DEVICE TEMPLATE " + TEMPLATE + " FROM " + DEVICE);

        try (final ResultSet resultSet =
            statement.executeQuery("SHOW PATHS USING DEVICE TEMPLATE " + TEMPLATE)) {
          Assert.assertFalse("the device template should be deactivated", resultSet.next());
        }
        try (final ResultSet resultSet =
            statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE " + TEMPLATE)) {
          Assert.assertTrue("the device template should remain set", resultSet.next());
          Assert.assertEquals(DATABASE, resultSet.getString(1));
          Assert.assertFalse(resultSet.next());
        }
      }
    } finally {
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }
}
