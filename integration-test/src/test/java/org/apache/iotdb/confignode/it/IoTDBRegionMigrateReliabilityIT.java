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

package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class IoTDBRegionMigrateReliabilityIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionMigrateReliabilityIT.class);
  private static final String INSERTION =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 10.1, 20.7)";
  private static final String SHOW_REGIONS = "show regions";
  private static final String SHOW_DATANODES = "show datanodes";
  private static final String REGION_MIGRATE_COMMAND_FORMAT = "migrate region %d from %d to %d";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // region Normal tests

  @Test
  public void normal1C2DTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
    EnvFactory.getEnv().initClusterEnvironment(1, 2);

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(INSERTION);

      ResultSet result = statement.executeQuery(SHOW_REGIONS);
      Map<Integer, Set<Integer>> regionMap = getRegionMap(result);

      result = statement.executeQuery(SHOW_DATANODES);
      Set<Integer> dataNodeSet = new HashSet<>();
      while (result.next()) {
        dataNodeSet.add(result.getInt(ColumnHeaderConstant.NODE_ID));
      }

      final int selectedRegion = selectRegion(regionMap);
      final int originalDataNode = selectOriginalDataNode(regionMap, selectedRegion);
      final int destDataNode = selectDestDataNode(dataNodeSet, regionMap, selectedRegion);

      // set breakpoint
      HashMap<String, Runnable> keywordAction = new HashMap<>();
      Arrays.stream(RegionTransitionState.values())
          .forEach(
              state ->
                  keywordAction.put(
                      String.valueOf(state), () -> LOGGER.info(String.valueOf(state))));
      ExecutorService service = IoTDBThreadPoolFactory.newCachedThreadPool("regionMigrateIT");
      LOGGER.info("breakpoint setting...");
      service.submit(() -> logBreakpointMonitor(0, keywordAction));
      LOGGER.info("breakpoint set");

      statement.execute(regionMigrateCommand(selectedRegion, originalDataNode, destDataNode));

      awaitUntilSuccess(statement, selectedRegion, originalDataNode, destDataNode);

      checkRegionFileClear(originalDataNode);

      LOGGER.info("test pass");
    }
  }

  @Test
  public void normal3C3DTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3);
    EnvFactory.getEnv().initClusterEnvironment(3, 3);

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient configClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      statement.execute(INSERTION);

      ResultSet result = statement.executeQuery(SHOW_REGIONS);
      Map<Integer, Set<Integer>> regionMap = getRegionMap(result);

      result = statement.executeQuery(SHOW_DATANODES);
      Set<Integer> dataNodeSet = new HashSet<>();
      while (result.next()) {
        dataNodeSet.add(result.getInt(ColumnHeaderConstant.NODE_ID));
      }

      final int selectedRegion = selectRegion(regionMap);
      final int originalDataNode = selectOriginalDataNode(regionMap, selectedRegion);
      final int destDataNode = selectDestDataNode(dataNodeSet, regionMap, selectedRegion);

      // set breakpoint
      HashMap<String, Runnable> keywordAction = new HashMap<>();
      Arrays.stream(RegionTransitionState.values())
          .forEach(
              state ->
                  keywordAction.put(
                      String.valueOf(state), () -> LOGGER.info(String.valueOf(state))));
      ExecutorService service = IoTDBThreadPoolFactory.newCachedThreadPool("regionMigrateIT");
      LOGGER.info("breakpoint setting...");
      service.submit(() -> logBreakpointMonitor(0, keywordAction));
      service.submit(() -> logBreakpointMonitor(1, keywordAction));
      service.submit(() -> logBreakpointMonitor(2, keywordAction));
      LOGGER.info("breakpoint set");

      statement.execute(regionMigrateCommand(selectedRegion, originalDataNode, destDataNode));

      awaitUntilSuccess(statement, selectedRegion, originalDataNode, destDataNode);

      checkRegionFileClear(originalDataNode);

      LOGGER.info("test pass");
    }
  }

  // endregion

  // region ConfigNode crash tests
  @Test
  public void cnCrashDuringPreCheck() {}

  @Test
  public void cnCrashDuringCreatePeer() {}

  @Test
  public void cnCrashDuringAddPeer() {}

  // TODO: other cn crash test

  // endregion

  // region DataNode crash tests

  // endregion

  // region Helpers

  /**
   * Monitor the node's log and do something.
   *
   * @param nodeIndex
   * @param keywordAction Map<keyword, action>
   */
  private static void logBreakpointMonitor(int nodeIndex, HashMap<String, Runnable> keywordAction) {
    ProcessBuilder builder =
        new ProcessBuilder(
            "tail",
            "-f",
            EnvFactory.getEnv().getConfigNodeWrapper(nodeIndex).getNodePath()
                + File.separator
                + "logs"
                + File.separator
                + "log_confignode_all.log");
    builder.redirectErrorStream(true); // 将错误输出和标准输出合并

    try {
      Process process = builder.start(); // 开始执行命令
      // 读取命令的输出
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          Set<String> detected = new HashSet<>();
          String finalLine = line;
          keywordAction
              .keySet()
              .forEach(
                  k -> {
                    if (finalLine.contains(k)) {
                      detected.add(k);
                    }
                  });
          detected.forEach(
              k -> {
                keywordAction.get(k).run();
                //
                // EnvFactory.getEnv().getConfigNodeWrapper(nodeIndex).stopForcibly();
                keywordAction.remove(k);
              });
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String regionMigrateCommand(int who, int from, int to) {
    return String.format(REGION_MIGRATE_COMMAND_FORMAT, who, from, to);
  }

  private static Map<Integer, Set<Integer>> getRegionMap(ResultSet showRegionsResult)
      throws SQLException {
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    while (showRegionsResult.next()) {
      if (String.valueOf(TConsensusGroupType.DataRegion)
          .equals(showRegionsResult.getString(ColumnHeaderConstant.TYPE))) {
        int region = showRegionsResult.getInt(ColumnHeaderConstant.REGION_ID);
        int dataNode = showRegionsResult.getInt(ColumnHeaderConstant.DATA_NODE_ID);
        regionMap.putIfAbsent(region, new HashSet<>());
        regionMap.get(region).add(dataNode);
      }
    }
    return regionMap;
  }

  private static int selectRegion(Map<Integer, Set<Integer>> regionMap) {
    return regionMap.keySet().stream().findAny().orElseThrow(() -> new RuntimeException("gg"));
  }

  private static int selectOriginalDataNode(
      Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return regionMap.get(selectedRegion).stream()
        .findAny()
        .orElseThrow(() -> new RuntimeException("gg"));
  }

  private static int selectDestDataNode(
      Set<Integer> dataNodeSet, Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return dataNodeSet.stream()
        .filter(dataNodeId -> !regionMap.get(selectedRegion).contains(dataNodeId))
        .findAny()
        .orElseThrow(() -> new RuntimeException("gg"));
  }

  private static void awaitUntilSuccess(
      Statement statement, int selectedRegion, int originalDataNode, int destDataNode) {
    AtomicReference<Set<Integer>> lastTimeDataNodes = new AtomicReference<>();
    try {
      Awaitility.await()
              .atMost(1, TimeUnit.MINUTES)
              .until(
                      () -> {
                        Map<Integer, Set<Integer>> newRegionMap =
                                getRegionMap(statement.executeQuery(SHOW_REGIONS));
                        Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
                        lastTimeDataNodes.set(dataNodes);
                        return !dataNodes.contains(originalDataNode) && dataNodes.contains(destDataNode);
                      });
    } catch (ConditionTimeoutException e) {
//      Set<Integer> expectation = new Set<>(lastTimeDataNodes);
      String actualSetStr = lastTimeDataNodes.get().toString();
      lastTimeDataNodes.get().remove(originalDataNode);
      lastTimeDataNodes.get().add(destDataNode);
      String expectSetStr = lastTimeDataNodes.toString();
      LOGGER.info("DataNode Set {} is unexpected, expect {}", actualSetStr, expectSetStr);
      throw e;
    }

  }

  /** Check whether the original DataNode's region file has been deleted. */
  private static void checkRegionFileClear(int dataNode) {
    String nodePath = EnvFactory.getEnv().dataNodeIdToWrapper(dataNode).get().getNodePath();
    File originalRegionDir =
        new File(
            nodePath
                + File.separator
                + IoTDBConstant.DATA_FOLDER_NAME
                + File.separator
                + "datanode"
                + File.separator
                + IoTDBConstant.CONSENSUS_FOLDER_NAME
                + File.separator
                + "data_region");
    Assert.assertTrue(originalRegionDir.isDirectory());
    Assert.assertEquals(0, Objects.requireNonNull(originalRegionDir.listFiles()).length);
  }

  // endregion
}
