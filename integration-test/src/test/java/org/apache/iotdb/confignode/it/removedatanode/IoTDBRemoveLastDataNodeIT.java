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

package org.apache.iotdb.confignode.it.removedatanode;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.generateRemoveString;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.selectRemoveDataNodes;
import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

/**
 * Removing the last DataNode of a single-replica cluster must be rejected. This only needs a 1C1D
 * cluster, so it lives in the 1C1D (LocalStandaloneIT) suite, separate from the multi-DataNode
 * removal tests in {@link IoTDBRemoveDataNodeNormalIT}.
 */
@Category({LocalStandaloneIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRemoveLastDataNodeIT {

  private static final String SHOW_DATANODES = "show datanodes";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
  }

  @After
  public void tearDown() throws InterruptedException {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void failWhenRemovingLastSingleReplicaDataNodeUseSQL() throws Exception {
    // With a single replica (schema_replication_factor and data_replication_factor are both 1),
    // removing DataNodes is still supported as long as more than one DataNode remains, but the last
    // remaining DataNode cannot be removed because there is nowhere to migrate its regions to.
    // Here we set up 1C1D with single replica and try to remove the only DataNode, which must fail
    // because removing it would leave the cluster with no DataNode.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setDefaultDataRegionGroupNumPerDatabase(1);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        final ResultSet resultSet = statement.executeQuery(SHOW_DATANODES)) {
      final Set<Integer> allDataNodeId = new HashSet<>();
      while (resultSet.next()) {
        allDataNodeId.add(resultSet.getInt(ColumnHeaderConstant.NODE_ID));
      }

      final String removeDataNodeSQL =
          generateRemoveString(selectRemoveDataNodes(allDataNodeId, 1));
      try {
        statement.execute(removeDataNodeSQL);
        Assert.fail(
            "Remove DataNode should fail when it would leave no DataNode under single replica");
      } catch (final IoTDBSQLException e) {
        // The unified rejection message reports the gap and, for a single replica, appends the
        // "at least one DataNode must always remain" hint.
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("Cannot remove"));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("single replica"));
        Assert.assertFalse(
            e.getMessage(), e.getMessage().contains("Failed to remove all requested data nodes"));
      }
    }
  }
}
