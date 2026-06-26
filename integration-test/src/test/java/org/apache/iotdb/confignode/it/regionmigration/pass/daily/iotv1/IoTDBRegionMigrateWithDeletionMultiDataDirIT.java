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

package org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getAllDataNodes;
import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMapWithLeader;

/**
 * Tree-model coverage for IoTConsensus region migration over multiple data dirs: a deletion (mods)
 * must survive the snapshot transfer to the migrated peer. With several data dirs the snapshot
 * fragments of one TsFile can be received into different folders, so the receiver groups companion
 * files and the loader relinks them into one data dir; if that breaks, the migrated replica loses
 * the deletion. See the table-model twin {@link IoTDBRegionMigrateWithDeletionMultiDataDirTableIT}.
 */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBRegionMigrateWithDeletionMultiDataDirIT {

  private static final String MULTI_DATA_DIRS =
      "data/datanode/data/disk0,data/datanode/data/disk1,data/datanode/data/disk2";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(2)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setDnDataDirs(MULTI_DATA_DIRS);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRegionMigratePreservesDeletionWithMultiDataDirs() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.db");
      statement.execute(
          "INSERT INTO root.db.d1(timestamp, s1) VALUES (100, 100), (200, 200), (300, 300)");
      statement.execute("FLUSH");
      statement.execute("DELETE FROM root.db.d1.s1 WHERE time <= 200");
      statement.execute("FLUSH");

      Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMapWithLeader =
          getDataRegionMapWithLeader(statement);
      int dataRegionIdForTest =
          dataRegionMapWithLeader.keySet().stream().max(Integer::compareTo).orElseThrow();
      assertDeletionVisibleOnAllReplicas(dataRegionIdForTest, 1);

      Pair<Integer, Set<Integer>> leaderAndNodes = dataRegionMapWithLeader.get(dataRegionIdForTest);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int leaderId = leaderAndNodes.getLeft();
      int followerId =
          leaderAndNodes.getRight().stream().filter(id -> id != leaderId).findFirst().orElseThrow();
      int destDataNodeId =
          allDataNodes.stream()
              .filter(id -> id != leaderId && id != followerId)
              .findFirst()
              .orElseThrow();

      statement.execute(
          String.format(
              "migrate region %d from %d to %d", dataRegionIdForTest, leaderId, destDataNodeId));

      final int finalDestDataNodeId = destDataNodeId;
      Awaitility.await()
          .atMost(10, TimeUnit.MINUTES)
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try (ResultSet showRegions = statement.executeQuery("SHOW REGIONS")) {
                  boolean migrated = false;
                  while (showRegions.next()) {
                    if (showRegions.getInt("RegionId") == dataRegionIdForTest
                        && showRegions.getInt("DataNodeId") == finalDestDataNodeId) {
                      migrated = true;
                      break;
                    }
                  }
                  Assert.assertTrue(migrated);
                }
              });

      assertDeletionVisibleOnAllReplicas(dataRegionIdForTest, 1);
    }
  }

  private void assertDeletionVisibleOnAllReplicas(int dataRegionId, int expectedCount)
      throws Exception {
    Set<Integer> replicaDataNodeIds;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      replicaDataNodeIds = getReplicaDataNodeIds(statement, dataRegionId);
    }
    for (int dataNodeId : replicaDataNodeIds) {
      DataNodeWrapper dataNodeWrapper =
          EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).orElseThrow();
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(500, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(() -> assertDeletionVisibleOnReplica(dataNodeWrapper, expectedCount));
    }
  }

  private void assertDeletionVisibleOnReplica(DataNodeWrapper dataNodeWrapper, int expectedCount)
      throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(dataNodeWrapper);
        Statement dataNodeStatement = connection.createStatement()) {
      try (ResultSet countResultSet =
          dataNodeStatement.executeQuery("SELECT COUNT(s1) FROM root.db.d1")) {
        Assert.assertTrue(countResultSet.next());
        Assert.assertEquals(expectedCount, countResultSet.getLong(1));
      }
      try (ResultSet deletedRangeResultSet =
          dataNodeStatement.executeQuery("SELECT s1 FROM root.db.d1 WHERE time <= 200")) {
        Assert.assertFalse(deletedRangeResultSet.next());
      }
    }
  }

  private Set<Integer> getReplicaDataNodeIds(Statement statement, int dataRegionId)
      throws Exception {
    Set<Integer> replicaDataNodeIds = new HashSet<>();
    try (ResultSet showRegions = statement.executeQuery("SHOW REGIONS")) {
      while (showRegions.next()) {
        if ("DataRegion".equals(showRegions.getString("Type"))
            && showRegions.getInt("RegionId") == dataRegionId) {
          replicaDataNodeIds.add(showRegions.getInt("DataNodeId"));
        }
      }
    }
    Assert.assertFalse(replicaDataNodeIds.isEmpty());
    return replicaDataNodeIds;
  }
}
