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

package org.apache.iotdb.confignode.it.regionmigration.pass.commit;

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getAllDataNodes;
import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMapWithLeader;
import static org.apache.iotdb.confignode.it.regionmigration.pass.commit.RegionMigrateFileAssertions.MULTI_DATA_DIRS;
import static org.apache.iotdb.confignode.it.regionmigration.pass.commit.RegionMigrateFileAssertions.getReplicaDataNodeIds;

/**
 * Regression for IoTConsensus multi-data-dir snapshot receive of OBJECT data: receive folders that
 * only contain object/ (no sequence/unsequence) must still be loaded, and the destination peer must
 * reach Running only after the object payload is queryable.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBRegionMigrateObjectMultiDataDirIT {

  private static final byte[] OBJECT_PAYLOAD = new byte[] {(byte) 0xCA, (byte) 0xFE, 0x01, 0x02};

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
  public void testRegionMigratePreservesObjectDataWithMultiDataDirs() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute("CREATE TABLE t1 (id STRING TAG, s1 INT64 FIELD, s2 OBJECT FIELD)");
      // Spread inserts so object files are more likely to land across multiple data dirs.
      for (int i = 0; i < 30; i++) {
        statement.execute(
            String.format(
                "INSERT INTO t1 (time, id, s1, s2) VALUES (%d, 'd%d', %d, to_object(true, 0,"
                    + " X'cafe0102'))",
                100 + i, i % 5, 100 + i));
      }
      statement.execute("FLUSH");

      Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMapWithLeader =
          getDataRegionMapWithLeader(statement);
      int dataRegionIdForTest =
          dataRegionMapWithLeader.keySet().stream().max(Integer::compareTo).orElseThrow();
      assertObjectQueryableOnAllReplicas(statement, dataRegionIdForTest, 30);

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

      long objectFilesOnSourceBeforeMigrate =
          countObjectFilesForRegion(
              EnvFactory.getEnv().dataNodeIdToWrapper(leaderId).orElseThrow(), dataRegionIdForTest);
      Assert.assertTrue(
          "Source replica should already have object files before migrate",
          objectFilesOnSourceBeforeMigrate > 0);

      statement.execute(
          String.format(
              "migrate region %d from %d to %d", dataRegionIdForTest, leaderId, destDataNodeId));

      final int finalDestDataNodeId = destDataNodeId;
      AtomicReference<String> destStatus = new AtomicReference<>();
      Awaitility.await()
          .atMost(10, TimeUnit.MINUTES)
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                boolean migrated = false;
                try (ResultSet showRegions = statement.executeQuery("SHOW REGIONS")) {
                  while (showRegions.next()) {
                    if (showRegions.getInt("RegionId") == dataRegionIdForTest
                        && showRegions.getInt("DataNodeId") == finalDestDataNodeId) {
                      destStatus.set(showRegions.getString("Status"));
                      migrated = "Running".equals(destStatus.get());
                      break;
                    }
                  }
                }
                Assert.assertTrue(
                    "Destination peer should become Running after successful AddPeer, current="
                        + destStatus.get(),
                    migrated);
              });

      DataNodeWrapper destWrapper =
          EnvFactory.getEnv().dataNodeIdToWrapper(finalDestDataNodeId).orElseThrow();
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(500, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                long objectFilesOnDest =
                    countObjectFilesForRegion(destWrapper, dataRegionIdForTest);
                Assert.assertTrue(
                    "Destination must load object files from all receive folders, found="
                        + objectFilesOnDest,
                    objectFilesOnDest > 0);
                Assert.assertEquals(
                    "Object file count mismatch after multi-dir snapshot load",
                    objectFilesOnSourceBeforeMigrate,
                    objectFilesOnDest);
              });

      assertObjectQueryableOnAllReplicas(statement, dataRegionIdForTest, 30);
      assertObjectPayloadOnReplica(destWrapper);
    }
  }

  private void assertObjectQueryableOnAllReplicas(
      Statement statement, int dataRegionId, int expectedCount) throws Exception {
    Set<Integer> replicaDataNodeIds = getReplicaDataNodeIds(statement, dataRegionId);
    for (int dataNodeId : replicaDataNodeIds) {
      DataNodeWrapper dataNodeWrapper =
          EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).orElseThrow();
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(500, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(() -> assertObjectCountOnReplica(dataNodeWrapper, expectedCount));
    }
  }

  private void assertObjectCountOnReplica(DataNodeWrapper dataNodeWrapper, int expectedCount)
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
    }
  }

  private void assertObjectPayloadOnReplica(DataNodeWrapper dataNodeWrapper) throws Exception {
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    dataNodeWrapper,
                    SessionConfig.DEFAULT_USER,
                    SessionConfig.DEFAULT_PASSWORD,
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement dataNodeStatement = connection.createStatement()) {
      dataNodeStatement.execute("USE test");
      try (ResultSet resultSet =
          dataNodeStatement.executeQuery(
              "SELECT read_object(s2) FROM t1 WHERE time = 100 AND id = 'd0'")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertArrayEquals(OBJECT_PAYLOAD, resultSet.getBytes(1));
      }
    }
  }

  /**
   * Count object files for a region across all configured data disks ({@code disk0/object/...},
   * {@code disk1/object/...}, ...), matching multi-dir dn_data_dirs layout.
   */
  private static long countObjectFilesForRegion(DataNodeWrapper dataNodeWrapper, int regionId)
      throws IOException {
    Path dataRoot = Paths.get(dataNodeWrapper.getDataNodeDir(), "data");
    if (!Files.exists(dataRoot)) {
      return 0;
    }
    long total = 0;
    try (Stream<Path> disks = Files.list(dataRoot)) {
      for (Path disk : (Iterable<Path>) disks::iterator) {
        Path regionObjectDir = disk.resolve("object").resolve(String.valueOf(regionId));
        if (!Files.isDirectory(regionObjectDir)) {
          continue;
        }
        try (Stream<Path> walk = Files.walk(regionObjectDir)) {
          total += walk.filter(Files::isRegularFile).count();
        }
      }
    }
    return total;
  }
}
