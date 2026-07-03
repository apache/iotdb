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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBMigrateMultiRegionForIoTV1IT extends IoTDBRegionOperationReliabilityITFramework {
  private static final String MULTI_REGION_MIGRATE_FORMAT = "migrate region %s from %d to %d";

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBMigrateMultiRegionForIoTV1IT.class);

  /**
   * Migrate multiple regions from one source DataNode to one destination DataNode in a single
   * statement: {@code migrate region r1,r2 from src to dest}. Both regions must leave the source
   * and land on the destination.
   */
  @Test
  public void multiRegionMigrateTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1)
        // Create 6 data region groups (> 5 DataNodes) so that, with replication factor 1, at least
        // one DataNode is guaranteed by pigeonhole to host >= 2 regions - the precondition of
        // selectDataNodeHostingMultipleRegions below. Under the default AUTO policy only ~2-3
        // regions were created and a balanced spread could leave every DataNode with a single
        // region, making this test flaky ("Cannot find a DataNode hosting at least two regions").
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(6);

    EnvFactory.getEnv().initClusterEnvironment(1, 5);

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // prepare data
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      Map<Integer, Set<Integer>> regionMap = getAllRegionMap(statement);
      Set<Integer> allDataNodeId = getAllDataNodes(statement);

      // With replication factor 1 every region lives on exactly one DataNode. Pick a source
      // DataNode that hosts at least two regions, then migrate all its regions to a fresh
      // destination DataNode.
      int sourceDataNode = selectDataNodeHostingMultipleRegions(regionMap);
      List<Integer> selectedRegions = regionsOnDataNode(regionMap, sourceDataNode);
      int destDataNode =
          findDataNodeNotContainsAnyRegion(allDataNodeId, regionMap, selectedRegions);

      LOGGER.info(
          "Multi-region migrate: regions {} from DataNode {} to DataNode {}",
          selectedRegions,
          sourceDataNode,
          destDataNode);

      String command =
          String.format(
              MULTI_REGION_MIGRATE_FORMAT,
              selectedRegions.stream().map(String::valueOf).collect(Collectors.joining(",")),
              sourceDataNode,
              destDataNode);

      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  statement.execute(command);
                  return true;
                } catch (Exception e) {
                  String errorMessage = e.getMessage();
                  if (errorMessage != null
                      && errorMessage.contains("successfully submitted")
                      && errorMessage.contains("failed to submit")) {
                    LOGGER.warn("Multi-region migrate partially succeeded: {}", errorMessage);
                    return true;
                  }
                  LOGGER.warn("Multi-region migrate failed, retrying: {}", errorMessage);
                  return false;
                }
              });

      Predicate<TShowRegionResp> migratePredicate =
          tShowRegionResp -> {
            // The source replica is set to RegionStatus.Removing at the start of
            // RemoveRegionPeerProcedure and is only actually deleted at its final
            // REMOVE_REGION_LOCATION_CACHE state. getRunningRegionMap() filters out the Removing
            // replica, so checking "source absent" against the Running-only view would pass while
            // the source replica is still listed (in Removing status) in the partition table,
            // before the immediately-following getAllRegionMap() assertion can observe it gone.
            // Mirror the single-region migrate predicate (see generalTestWithAllOptions): require
            // the destination to be Running and check source absence against the all-status view.
            Map<Integer, Set<Integer>> runningRegionMap =
                getRunningRegionMap(tShowRegionResp.getRegionInfoList());
            Map<Integer, Set<Integer>> allRegionMap =
                getRegionMap(tShowRegionResp.getRegionInfoList());
            return selectedRegions.stream()
                .allMatch(
                    regionId -> {
                      Set<Integer> runningDataNodes = runningRegionMap.get(regionId);
                      Set<Integer> allDataNodes = allRegionMap.get(regionId);
                      return runningDataNodes != null
                          && runningDataNodes.contains(destDataNode)
                          && (allDataNodes == null || !allDataNodes.contains(sourceDataNode));
                    });
          };

      awaitUntilSuccess(
          client,
          selectedRegions.get(0),
          migratePredicate,
          Optional.of(destDataNode),
          Optional.of(sourceDataNode));

      regionMap = getAllRegionMap(statement);
      for (int regionId : selectedRegions) {
        Assert.assertTrue(
            "Region " + regionId + " should be on destination DataNode " + destDataNode,
            regionMap.get(regionId).contains(destDataNode));
        Assert.assertFalse(
            "Region " + regionId + " should have left source DataNode " + sourceDataNode,
            regionMap.get(regionId).contains(sourceDataNode));
      }
      LOGGER.info("Multi-region migrate test passed");
    }
  }

  private int selectDataNodeHostingMultipleRegions(Map<Integer, Set<Integer>> regionMap) {
    Map<Integer, Long> regionCountPerDataNode =
        regionMap.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.groupingBy(dataNodeId -> dataNodeId, Collectors.counting()));
    return regionCountPerDataNode.entrySet().stream()
        .filter(entry -> entry.getValue() >= 2)
        .map(Map.Entry::getKey)
        .findFirst()
        .orElseThrow(
            () -> new RuntimeException("Cannot find a DataNode hosting at least two regions"));
  }

  private List<Integer> regionsOnDataNode(Map<Integer, Set<Integer>> regionMap, int dataNodeId) {
    return regionMap.entrySet().stream()
        .filter(entry -> entry.getValue().contains(dataNodeId))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  private int findDataNodeNotContainsAnyRegion(
      Set<Integer> allDataNodeId, Map<Integer, Set<Integer>> regionMap, List<Integer> regionIds) {
    return allDataNodeId.stream()
        .filter(
            dataNodeId ->
                regionIds.stream()
                    .noneMatch(regionId -> regionMap.get(regionId).contains(dataNodeId)))
        .findFirst()
        .orElseThrow(
            () ->
                new RuntimeException(
                    "Cannot find DataNode that doesn't contain any of the regions"));
  }
}
