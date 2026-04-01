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

import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getAllDataNodes;
import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMapWithLeader;

public class IoTDBRegionMigrateWithCompressionRatioIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(2)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testWithCompressionRatio() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute("CREATE TABLE t1 (s1 INT64 FIELD)");
      statement.execute("INSERT INTO t1 (time, s1) VALUES (100, 100)");
      statement.execute("FLUSH");

      Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMapWithLeader =
          getDataRegionMapWithLeader(statement);
      int dataRegionIdForTest =
          dataRegionMapWithLeader.keySet().stream().max(Integer::compare).get();

      Pair<Integer, Set<Integer>> leaderAndNodes = dataRegionMapWithLeader.get(dataRegionIdForTest);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int leaderId = leaderAndNodes.getLeft();
      int followerId =
          leaderAndNodes.getRight().stream().filter(i -> i != leaderId).findAny().get();
      int newLeaderId =
          allDataNodes.stream().filter(i -> i != leaderId && i != followerId).findAny().get();

      System.out.printf(
          "Old leader: %d, follower: %d, new leader: %d%n", leaderId, followerId, newLeaderId);

      double[] compressionRatioBeforeMigration = new double[] {Double.NaN};
      Awaitility.await()
          .atMost(10, TimeUnit.MINUTES)
          .pollDelay(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try (ResultSet showRegions = statement.executeQuery("SHOW REGIONS")) {
                  while (showRegions.next()) {
                    int regionId = showRegions.getInt("RegionId");
                    int dataNodeId = showRegions.getInt("DataNodeId");
                    if (regionId == dataRegionIdForTest && dataNodeId == leaderId) {
                      compressionRatioBeforeMigration[0] =
                          showRegions.getDouble("CompressionRatio");
                      break;
                    }
                  }
                }
                Assert.assertFalse(Double.isNaN(compressionRatioBeforeMigration[0]));
              });

      statement.execute(
          String.format(
              "migrate region %d from %d to %d", dataRegionIdForTest, leaderId, newLeaderId));

      double finalCompressionRatioBeforeMigration = compressionRatioBeforeMigration[0];
      Awaitility.await()
          .atMost(10, TimeUnit.MINUTES)
          .pollDelay(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                double compressionRatioAfterMigration = 0.0;
                try (ResultSet showRegions = statement.executeQuery("SHOW REGIONS")) {
                  while (showRegions.next()) {
                    int regionId = showRegions.getInt("RegionId");
                    int dataNodeId = showRegions.getInt("DataNodeId");
                    if (regionId == dataRegionIdForTest && dataNodeId == newLeaderId) {
                      compressionRatioAfterMigration = showRegions.getDouble("CompressionRatio");
                      break;
                    }
                  }
                }
                Assert.assertEquals(
                    finalCompressionRatioBeforeMigration, compressionRatioAfterMigration, 0.0001);
              });
    }
  }
}
