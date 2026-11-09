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
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category(ClusterIT.class)
@RunWith(IoTDBTestRunner.class)
public class IoTDBRemoveDataNodeAfterDeleteIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRemoveAfterDeletingUnflushedRow() throws Exception {
    removeAfterDelete(false);
  }

  @Test
  public void testRemoveAfterDeletingUnflushedRowWithRetainedData() throws Exception {
    removeAfterDelete(true);
  }

  private void removeAfterDelete(boolean retainData) throws Exception {
    int removedNodeId = -1;
    int removedPort = -1;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.snapshot_delete");
      statement.execute("CREATE TIMESERIES root.snapshot_delete.d.s WITH DATATYPE=INT32");
      if (retainData) {
        statement.execute("INSERT INTO root.snapshot_delete.d(time,s) VALUES(1,10)");
        statement.execute("FLUSH");
      }
      statement.execute("INSERT INTO root.snapshot_delete.d(time,s) VALUES(2,20)");
      statement.execute("DELETE FROM root.snapshot_delete.d.s WHERE time = 2");
      try (ResultSet regions = statement.executeQuery("SHOW REGIONS")) {
        while (regions.next()) {
          if ("DataRegion".equals(regions.getString(ColumnHeaderConstant.TYPE))
              && "root.snapshot_delete".equals(regions.getString(ColumnHeaderConstant.DATABASE))) {
            removedNodeId = regions.getInt(ColumnHeaderConstant.DATA_NODE_ID);
          }
        }
      }
      assertTrue("The test database must have a DataRegion", removedNodeId >= 0);
      try (ResultSet nodes = statement.executeQuery("SHOW DATANODES")) {
        while (nodes.next()) {
          if (nodes.getInt(ColumnHeaderConstant.NODE_ID) == removedNodeId) {
            removedPort = nodes.getInt(ColumnHeaderConstant.RPC_PORT);
          }
        }
      }
      assertTrue(removedPort > 0);
      statement.execute("REMOVE DATANODE " + removedNodeId);
    }

    final int nodeId = removedNodeId;
    final int rpcPort = removedPort;
    DataNodeWrapper removed =
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .filter(node -> node.getPort() == rpcPort)
            .findFirst()
            .get();
    DataNodeWrapper survivor =
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .filter(node -> node.getPort() != rpcPort)
            .findFirst()
            .get();
    try (Connection connection = EnvFactory.getEnv().getConnection(survivor);
        Statement statement = connection.createStatement()) {
      await()
          .atMost(600, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try (ResultSet nodes = statement.executeQuery("SHOW DATANODES")) {
                  while (nodes.next()) {
                    assertNotEquals(nodeId, nodes.getInt(ColumnHeaderConstant.NODE_ID));
                  }
                }
                assertFalse("Removed DataNode must exit", removed.isAlive());
              });
      try (ResultSet rows = statement.executeQuery("SELECT s FROM root.snapshot_delete.d")) {
        if (retainData) {
          assertTrue(rows.next());
          assertEquals(1, rows.getLong("Time"));
          assertEquals(10, rows.getInt("root.snapshot_delete.d.s"));
        }
        assertFalse("Deleted data must not reappear after migration", rows.next());
      }
    }
  }
}
