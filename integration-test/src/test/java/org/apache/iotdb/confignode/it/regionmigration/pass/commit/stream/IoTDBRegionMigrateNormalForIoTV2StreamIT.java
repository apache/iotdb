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

package org.apache.iotdb.confignode.it.regionmigration.pass.commit.stream;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateNormalForIoTV2StreamIT
    extends IoTDBRegionOperationReliabilityITFramework {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setIoTConsensusV2Mode(ConsensusFactory.IOT_CONSENSUS_V2_STREAM_MODE);
  }

  @Test
  public void normal1C2DTest() throws Exception {
    successTest(1, 1, 1, 2, noKillPoints(), noKillPoints(), KillNode.ALL_NODES);
  }

  @Test
  public void normal3C3DTest() throws Exception {
    successTest(2, 3, 3, 3, noKillPoints(), noKillPoints(), KillNode.ALL_NODES);
  }

  @Test
  public void migrationShouldPreserveOtherRegionFiles() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      statement.execute(INSERTION1);
      Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
      Assert.assertEquals(1, regionMap.size());
      int selectedRegion = selectRegion(regionMap);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int source = selectDataNodeContainsRegion(allDataNodes, regionMap, selectedRegion);
      int destination = selectDataNodeNotContainsRegion(allDataNodes, regionMap, selectedRegion);

      // Three replicas ensure that the unrelated database also has a replica on the source DN.
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .setDatabase(
                  new TDatabaseSchema("root.other")
                      .setDataReplicationFactor(3)
                      .setSchemaReplicationFactor(3))
              .getCode());
      statement.execute("INSERT INTO root.other.d1(timestamp,s) values(100, 1)");
      statement.execute(FLUSH_COMMAND);
      Map<Integer, Set<Integer>> allRegions = getDataRegionMap(statement);
      Set<Integer> otherRegions = new HashSet<>(allRegions.keySet());
      otherRegions.removeAll(regionMap.keySet());
      Assert.assertFalse(otherRegions.isEmpty());
      for (int regionId : otherRegions) {
        Assert.assertEquals(allDataNodes, allRegions.get(regionId));
        checkRegionFileExist(source, regionId);
      }
      checkRegionFileExist(source, selectedRegion);

      statement.execute(
          "migrate region " + selectedRegion + " from " + source + " to " + destination);
      Set<Integer> expectedDataNodes = new HashSet<>(regionMap.get(selectedRegion));
      expectedDataNodes.remove(source);
      expectedDataNodes.add(destination);
      Awaitility.await()
          .atMost(1, TimeUnit.MINUTES)
          .untilAsserted(
              () ->
                  Assert.assertEquals(
                      expectedDataNodes, getDataRegionMap(statement).get(selectedRegion)));

      checkRegionFileClear(source, selectedRegion);
      checkRegionFileExist(destination, selectedRegion);
      for (int regionId : otherRegions) {
        checkRegionFileExist(source, regionId);
      }
    }
  }
}
