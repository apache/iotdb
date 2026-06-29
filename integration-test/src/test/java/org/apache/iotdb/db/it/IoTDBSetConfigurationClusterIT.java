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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSetConfigurationClusterIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    EnvFactory.getEnv().initClusterEnvironment(2, 1);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testHotReloadConsistentConfigOnAllConfigNodes() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      assertSetConsistentClusterConfigurationOnSpecificNodeFailed(statement);

      statement.execute("set configuration \"read_consistency_level\"=\"weak\"");
      for (int configNodeId : getConfigNodeIds()) {
        assertAppliedConfiguration(configNodeId, "read_consistency_level", "weak");
      }
    } finally {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("set configuration \"read_consistency_level\"=\"strong\"");
      }
    }
  }

  private static List<Integer> getConfigNodeIds() throws Exception {
    List<Integer> configNodeIds = new ArrayList<>();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      for (TConfigNodeLocation configNodeLocation : client.showCluster().getConfigNodeList()) {
        configNodeIds.add(configNodeLocation.getConfigNodeId());
      }
    }
    return configNodeIds;
  }

  private static void assertAppliedConfiguration(int nodeId, String key, String value)
      throws Exception {
    try (ITableSession tableSessionConnection = EnvFactory.getEnv().getTableSessionConnection();
        SessionDataSet sessionDataSet =
            tableSessionConnection.executeQueryStatement("show configuration on " + nodeId)) {
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        if (key.equals(iterator.getString(1))) {
          Assert.assertEquals(value, iterator.isNull(2) ? null : iterator.getString(2));
          return;
        }
      }
    }
    Assert.fail("Cannot find applied configuration: " + key + " on node " + nodeId);
  }

  private static void assertSetConsistentClusterConfigurationOnSpecificNodeFailed(
      Statement statement) throws SQLException {
    try {
      statement.execute("set configuration \"read_consistency_level\"=\"weak\" on 0");
    } catch (SQLException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "must be consistent across the entire cluster and only one can be set at a time"));
      return;
    }
    Assert.fail("Set consistent cluster configuration on a specific node should fail.");
  }
}
