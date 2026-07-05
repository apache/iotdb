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
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getAllDataNodes;
import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMapWithLeader;
import static org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1.RegionMigrateFileAssertions.MULTI_DATA_DIRS;
import static org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1.RegionMigrateFileAssertions.awaitModsVisibleOnReplicas;
import static org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1.RegionMigrateFileAssertions.awaitRegionReplicas;
import static org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1.RegionMigrateFileAssertions.awaitTsFileResourceVisibleOnReplicas;
import static org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1.RegionMigrateFileAssertions.awaitTsFileVisibleOnReplicas;
import static org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1.RegionMigrateFileAssertions.getReplicaDataNodeIds;

/**
 * Table-model twin of {@link IoTDBRegionMigrateWithDeletionMultiDataDirIT}: a deletion (mods) must
 * survive IoTConsensus region migration across multiple data dirs, asserted through the relational
 * (table) SQL dialect so the table-model cluster CI covers the same snapshot mods-transfer path.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBRegionMigrateWithDeletionMultiDataDirTableIT {

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
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute("CREATE TABLE t1 (s1 INT64 FIELD)");
      statement.execute("INSERT INTO t1 (time, s1) VALUES (100, 100), (200, 200), (300, 300)");
      statement.execute("FLUSH");

      Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMapWithLeader =
          getDataRegionMapWithLeader(statement);
      int dataRegionIdForTest =
          dataRegionMapWithLeader.keySet().stream().max(Integer::compareTo).orElseThrow();
      Set<Integer> initialReplicaDataNodeIds =
          getReplicaDataNodeIds(statement, dataRegionIdForTest);

      awaitTsFileVisibleOnReplicas("test", dataRegionIdForTest, initialReplicaDataNodeIds);
      awaitTsFileResourceVisibleOnReplicas(
          statement, "test", dataRegionIdForTest, initialReplicaDataNodeIds);

      statement.execute("DELETE FROM t1 WHERE time <= 200");
      statement.execute("FLUSH");
      awaitModsVisibleOnReplicas("test", dataRegionIdForTest, initialReplicaDataNodeIds);
      assertDeletionVisibleOnAllReplicas(statement, dataRegionIdForTest, 1);

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

      awaitRegionReplicas(statement, dataRegionIdForTest, Set.of(followerId, destDataNodeId));
      awaitModsVisibleOnReplicas("test", dataRegionIdForTest, Set.of(destDataNodeId));

      assertDeletionVisibleOnAllReplicas(statement, dataRegionIdForTest, 1);
    }
  }

  private void assertDeletionVisibleOnAllReplicas(
      Statement statement, int dataRegionId, int expectedCount) throws Exception {
    Set<Integer> replicaDataNodeIds = getReplicaDataNodeIds(statement, dataRegionId);
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
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    dataNodeWrapper,
                    SessionConfig.DEFAULT_USER,
                    SessionConfig.DEFAULT_PASSWORD,
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement dataNodeStatement = connection.createStatement()) {
      dataNodeStatement.execute("USE test");
      try (ResultSet countResultSet = dataNodeStatement.executeQuery("SELECT COUNT(s1) FROM t1")) {
        Assert.assertTrue(countResultSet.next());
        Assert.assertEquals(expectedCount, countResultSet.getLong(1));
      }
      try (ResultSet deletedRangeResultSet =
          dataNodeStatement.executeQuery("SELECT s1 FROM t1 WHERE time <= 200")) {
        Assert.assertFalse(deletedRangeResultSet.next());
      }
    }
  }
}
