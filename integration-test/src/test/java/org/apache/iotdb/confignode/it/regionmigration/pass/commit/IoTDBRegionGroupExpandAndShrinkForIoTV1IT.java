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
import java.util.ArrayList;
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
public class IoTDBRegionGroupExpandAndShrinkForIoTV1IT
    extends IoTDBRegionOperationReliabilityITFramework {
  private static final String EXPAND_FORMAT = "extend region %d to %d";
  private static final String SHRINK_FORMAT = "remove region %d from %d";
  private static final String MULTI_EXPAND_FORMAT = "extend region %s to %d";
  private static final String MULTI_SHRINK_FORMAT = "remove region %s from %d";

  private static Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionGroupExpandAndShrinkForIoTV1IT.class);

  /**
   * 1. Expand: {a} -> {a,b} -> ... -> {a,b,c,d,e}
   *
   * <p>2. Check
   *
   * <p>3. Shrink: {a,b,c,d,e} -> {a,c,d,e} -> ... -> {d}
   *
   * <p>4. Check
   */
  @Test
  public void singleRegionTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);

    EnvFactory.getEnv().initClusterEnvironment(1, 5);

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // prepare data
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      // collect necessary information
      Map<Integer, Set<Integer>> regionMap = getAllRegionMap(statement);
      Set<Integer> allDataNodeId = getAllDataNodes(statement);

      // expect one data region, one schema region
      // plus one AUDIT data region, one AUDIT schema region
      Assert.assertEquals(4, regionMap.size());

      // expand
      for (int selectedRegion : regionMap.keySet()) {
        for (int i = 0; i < 4; i++) {
          int targetDataNode =
              selectDataNodeNotContainsRegion(allDataNodeId, regionMap, selectedRegion);
          regionGroupExpand(statement, client, selectedRegion, targetDataNode);
          // update regionMap every time
          regionMap = getAllRegionMap(statement);
        }
      }

      // shrink
      for (int selectedRegion : regionMap.keySet()) {
        for (int i = 0; i < 4; i++) {
          int targetDataNode =
              selectDataNodeContainsRegion(allDataNodeId, regionMap, selectedRegion);
          regionGroupShrink(statement, client, selectedRegion, targetDataNode);
          // update regionMap every time
          regionMap = getAllRegionMap(statement);
        }
      }
    }
  }

  private void regionGroupExpand(
      Statement statement,
      SyncConfigNodeIServiceClient client,
      int selectedRegion,
      int targetDataNode)
      throws Exception {
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              statement.execute(String.format(EXPAND_FORMAT, selectedRegion, targetDataNode));
              return true;
            });

    Predicate<TShowRegionResp> expandRegionPredicate =
        tShowRegionResp -> {
          Map<Integer, Set<Integer>> newRegionMap =
              getRunningRegionMap(tShowRegionResp.getRegionInfoList());
          Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
          return dataNodes.contains(targetDataNode);
        };

    awaitUntilSuccess(
        client,
        selectedRegion,
        expandRegionPredicate,
        Optional.of(targetDataNode),
        Optional.empty());

    LOGGER.info("Region {} has expanded to DataNode {}", selectedRegion, targetDataNode);
  }

  private void regionGroupShrink(
      Statement statement,
      SyncConfigNodeIServiceClient client,
      int selectedRegion,
      int targetDataNode)
      throws Exception {
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              statement.execute(String.format(SHRINK_FORMAT, selectedRegion, targetDataNode));
              return true;
            });

    Predicate<TShowRegionResp> shrinkRegionPredicate =
        tShowRegionResp -> {
          Map<Integer, Set<Integer>> newRegionMap =
              getRegionMap(tShowRegionResp.getRegionInfoList());
          Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
          return !dataNodes.contains(targetDataNode);
        };

    awaitUntilSuccess(
        client,
        selectedRegion,
        shrinkRegionPredicate,
        Optional.empty(),
        Optional.of(targetDataNode));

    LOGGER.info("Region {} has shrunk from DataNode {}", selectedRegion, targetDataNode);
  }

  /**
   * Test multi-region expand and shrink operations with normal flow: 1. Multi-expand: expand
   * multiple regions to target DataNode 2. Multi-shrink: shrink multiple regions from target
   * DataNode
   */
  @Test
  public void multiRegionNormalTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);

    EnvFactory.getEnv().initClusterEnvironment(1, 5);

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // prepare data
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      // collect necessary information
      Map<Integer, Set<Integer>> regionMap = getAllRegionMap(statement);
      Set<Integer> allDataNodeId = getAllDataNodes(statement);

      // expect one data region, one schema region
      // plus one AUDIT data region, one AUDIT schema region
      Assert.assertEquals(4, regionMap.size());

      // select multiple regions for testing
      List<Integer> selectedRegions = new ArrayList<>(regionMap.keySet());
      selectedRegions = selectedRegions.subList(0, Math.min(3, selectedRegions.size()));

      // find target DataNode that doesn't contain any of the selected regions
      int targetDataNode =
          findDataNodeNotContainsAnyRegion(allDataNodeId, regionMap, selectedRegions);

      LOGGER.info("Selected regions for multi-region test: {}", selectedRegions);
      LOGGER.info("Target DataNode: {}", targetDataNode);

      // multi-expand: expand all selected regions to target DataNode
      multiRegionGroupExpand(statement, client, selectedRegions, targetDataNode);

      // verify expand result
      regionMap = getAllRegionMap(statement);
      for (int regionId : selectedRegions) {
        Assert.assertTrue(
            "Region " + regionId + " should contain target DataNode " + targetDataNode,
            regionMap.get(regionId).contains(targetDataNode));
      }
      LOGGER.info("Multi-region expand test passed");

      // multi-shrink: shrink all selected regions from target DataNode
      multiRegionGroupShrink(statement, client, selectedRegions, targetDataNode);

      // verify shrink result
      regionMap = getAllRegionMap(statement);
      for (int regionId : selectedRegions) {
        Assert.assertFalse(
            "Region " + regionId + " should not contain target DataNode " + targetDataNode,
            regionMap.get(regionId).contains(targetDataNode));
      }
      LOGGER.info("Multi-region shrink test passed");
    }
  }

  /** Test multi-region expand with partial regions already in target DataNode */
  @Test
  public void multiRegionExpandPartialExistTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);

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

      List<Integer> allRegions = new ArrayList<>(regionMap.keySet());
      List<Integer> selectedRegions = allRegions.subList(0, Math.min(3, allRegions.size()));

      int targetDataNode =
          findDataNodeNotContainsAnyRegion(allDataNodeId, regionMap, selectedRegions);

      // first expand some regions individually
      List<Integer> preExpandRegions =
          selectedRegions.subList(0, Math.min(2, selectedRegions.size()));
      for (int regionId : preExpandRegions) {
        regionGroupExpand(statement, client, regionId, targetDataNode);
      }

      // now try to expand all regions (including already expanded ones)
      LOGGER.info(
          "Testing multi-expand with regions {} to DataNode {}, where {} already exist",
          selectedRegions,
          targetDataNode,
          preExpandRegions);

      multiRegionGroupExpand(statement, client, selectedRegions, targetDataNode);

      // verify all regions are in target DataNode
      regionMap = getAllRegionMap(statement);
      for (int regionId : selectedRegions) {
        Assert.assertTrue(
            "Region " + regionId + " should contain target DataNode " + targetDataNode,
            regionMap.get(regionId).contains(targetDataNode));
      }
      LOGGER.info("Multi-region expand partial exist test passed");
    }
  }

  /** Test multi-region shrink with partial regions not in target DataNode */
  @Test
  public void multiRegionShrinkPartialNotExistTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);

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

      List<Integer> allRegions = new ArrayList<>(regionMap.keySet());
      List<Integer> selectedRegions = allRegions.subList(0, Math.min(3, allRegions.size()));

      int targetDataNode =
          findDataNodeNotContainsAnyRegion(allDataNodeId, regionMap, selectedRegions);

      // first expand all regions to target DataNode
      multiRegionGroupExpand(statement, client, selectedRegions, targetDataNode);

      // then shrink some regions individually
      List<Integer> preShrinkRegions =
          selectedRegions.subList(0, Math.min(2, selectedRegions.size()));
      for (int regionId : preShrinkRegions) {
        regionGroupShrink(statement, client, regionId, targetDataNode);
      }

      // now try to shrink all regions (including already shrunk ones)
      LOGGER.info(
          "Testing multi-shrink with regions {} from DataNode {}, where {} already removed",
          selectedRegions,
          targetDataNode,
          preShrinkRegions);

      multiRegionGroupShrink(statement, client, selectedRegions, targetDataNode);

      // verify all regions are not in target DataNode
      regionMap = getAllRegionMap(statement);
      for (int regionId : selectedRegions) {
        Assert.assertFalse(
            "Region " + regionId + " should not contain target DataNode " + targetDataNode,
            regionMap.get(regionId).contains(targetDataNode));
      }
      LOGGER.info("Multi-region shrink partial not exist test passed");
    }
  }

  private void multiRegionGroupExpand(
      Statement statement,
      SyncConfigNodeIServiceClient client,
      List<Integer> regionIds,
      int targetDataNode)
      throws Exception {
    String command = buildMultiRegionCommand(MULTI_EXPAND_FORMAT, regionIds, targetDataNode);

    Predicate<TShowRegionResp> expandPredicate =
        tShowRegionResp -> {
          Map<Integer, Set<Integer>> newRegionMap =
              getRunningRegionMap(tShowRegionResp.getRegionInfoList());
          return regionIds.stream()
              .allMatch(
                  regionId -> {
                    Set<Integer> dataNodes = newRegionMap.get(regionId);
                    return dataNodes != null && dataNodes.contains(targetDataNode);
                  });
        };

    executeMultiRegionOperation(
        statement,
        client,
        command,
        regionIds,
        expandPredicate,
        Optional.of(targetDataNode),
        Optional.empty(),
        "expand");
  }

  private void multiRegionGroupShrink(
      Statement statement,
      SyncConfigNodeIServiceClient client,
      List<Integer> regionIds,
      int targetDataNode)
      throws Exception {
    String command = buildMultiRegionCommand(MULTI_SHRINK_FORMAT, regionIds, targetDataNode);

    Predicate<TShowRegionResp> shrinkPredicate =
        tShowRegionResp -> {
          Map<Integer, Set<Integer>> newRegionMap =
              getRegionMap(tShowRegionResp.getRegionInfoList());
          return regionIds.stream()
              .allMatch(
                  regionId -> {
                    Set<Integer> dataNodes = newRegionMap.get(regionId);
                    return dataNodes == null || !dataNodes.contains(targetDataNode);
                  });
        };

    executeMultiRegionOperation(
        statement,
        client,
        command,
        regionIds,
        shrinkPredicate,
        Optional.empty(),
        Optional.of(targetDataNode),
        "shrink");
  }

  private String buildMultiRegionCommand(
      String format, List<Integer> regionIds, int targetDataNode) {
    String regionIdStr = regionIds.stream().map(String::valueOf).collect(Collectors.joining(","));
    return String.format(format, regionIdStr, targetDataNode);
  }

  private void executeMultiRegionOperation(
      Statement statement,
      SyncConfigNodeIServiceClient client,
      String command,
      List<Integer> regionIds,
      Predicate<TShowRegionResp> predicate,
      Optional<Integer> expectedDataNode,
      Optional<Integer> notExpectedDataNode,
      String operationType) {

    LOGGER.info("Executing multi-region {} command: {}", operationType, command);

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
                // If error message contains both "successfully submitted" and "failed to submit",
                // consider it as partial success and continue
                if (errorMessage != null
                    && errorMessage.contains("successfully submitted")
                    && errorMessage.contains("failed to submit")) {
                  LOGGER.warn(
                      "Multi-region {} partially succeeded: {}", operationType, errorMessage);
                  return true;
                }
                LOGGER.warn(
                    "Multi-region {} command execution failed, retrying: {}",
                    operationType,
                    errorMessage);
                return false;
              }
            });

    // Use the first region for awaitUntilSuccess (framework limitation)
    awaitUntilSuccess(client, regionIds.get(0), predicate, expectedDataNode, notExpectedDataNode);

    String targetDescription =
        expectedDataNode.isPresent()
            ? "to DataNode " + expectedDataNode.get()
            : "from DataNode " + notExpectedDataNode.get();
    LOGGER.info(
        "Regions {} have {} {}",
        regionIds,
        operationType.equals("expand") ? "expanded" : "shrunk",
        targetDescription);
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
