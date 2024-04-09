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

package org.apache.iotdb.confignode.it.regionmigration;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.itbase.exception.InconsistentDataException;
import org.apache.iotdb.metrics.utils.SystemType;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class IoTDBRegionMigrateReliabilityITFramework {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionMigrateReliabilityITFramework.class);
  private static final String INSERTION =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 10.1, 20.7)";
  private static final String SHOW_REGIONS = "show regions";
  private static final String SHOW_DATANODES = "show datanodes";
  private static final String REGION_MIGRATE_COMMAND_FORMAT = "migrate region %d from %d to %d";
  ExecutorService executorService = IoTDBThreadPoolFactory.newCachedThreadPool("regionMigrateIT");

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
  }

  @After
  public void tearDown() throws InterruptedException {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  public void successTest(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      KeySetView<String, Boolean> killConfigNodeKeywords,
      KeySetView<String, Boolean> killDataNodeKeywords)
      throws Exception {
    generalTestWithAllOptions(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        killConfigNodeKeywords,
        killDataNodeKeywords,
        true,
        true,
        0,
        true);
  }

  public void failTest(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      KeySetView<String, Boolean> killConfigNodeKeywords,
      KeySetView<String, Boolean> killDataNodeKeywords)
      throws Exception {
    generalTestWithAllOptions(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        killConfigNodeKeywords,
        killDataNodeKeywords,
        true,
        true,
        60,
        false);
  }

  public void generalTestWithAllOptions(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      KeySetView<String, Boolean> killConfigNodeKeywords,
      KeySetView<String, Boolean> killDataNodeKeywords,
      final boolean checkOriginalRegionDirDeleted,
      final boolean checkConfigurationFileDeleted,
      final int restartTime,
      final boolean isMigrateSuccess)
      throws Exception {
    // prepare env
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(dataReplicateFactor)
        .setSchemaReplicationFactor(schemaReplicationFactor);
    EnvFactory.getEnv().registerConfigNodeKillPoints(new ArrayList<>(killConfigNodeKeywords));
    EnvFactory.getEnv().registerDataNodeKillPoints(new ArrayList<>(killDataNodeKeywords));
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, dataNodeNum);

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(INSERTION);

      ResultSet result = statement.executeQuery(SHOW_REGIONS);
      Map<Integer, Set<Integer>> regionMap = getRegionMap(result);

      result = statement.executeQuery(SHOW_DATANODES);
      Set<Integer> allDataNodeId = new HashSet<>();
      while (result.next()) {
        allDataNodeId.add(result.getInt(ColumnHeaderConstant.NODE_ID));
      }

      final int selectedRegion = selectRegion(regionMap);
      final int originalDataNode = selectOriginalDataNode(regionMap, selectedRegion);
      final int destDataNode = selectDestDataNode(allDataNodeId, regionMap, selectedRegion);

      checkRegionFileExist(originalDataNode);
      checkPeersExist(regionMap.get(selectedRegion), originalDataNode, selectedRegion);

      // set kill points
      setConfigNodeKillPoints(killConfigNodeKeywords, restartTime);
      setDataNodeKillPoints(killDataNodeKeywords, restartTime);

      // region migration start
      statement.execute(regionMigrateCommand(selectedRegion, originalDataNode, destDataNode));

      boolean success = false;
      try {
        awaitUntilSuccess(statement, selectedRegion, originalDataNode, destDataNode);
        success = true;
      } catch (ConditionTimeoutException e) {
        LOGGER.error("Region migrate failed", e);
      }
      // Assert.assertTrue(isMigrateSuccess == success);

      // make sure all kill points have been triggered
      checkKillPointsAllTriggered(killConfigNodeKeywords);
      checkKillPointsAllTriggered(killDataNodeKeywords);

      if (!success) {
        restartAllDataNodes();
      }
      System.out.println(
          "originalDataNode: "
              + EnvFactory.getEnv().dataNodeIdToWrapper(originalDataNode).get().getNodePath());
      System.out.println(
          "destDataNode: "
              + EnvFactory.getEnv().dataNodeIdToWrapper(destDataNode).get().getNodePath());

      // check if there is anything remain
      if (checkOriginalRegionDirDeleted) {
        if (success) {
          checkRegionFileClear(originalDataNode);
          checkRegionFileExist(destDataNode);
        } else {
          checkRegionFileClear(destDataNode);
          checkRegionFileExist(originalDataNode);
        }
      }
      if (checkConfigurationFileDeleted) {
        if (success) {
          checkPeersClear(allDataNodeId, originalDataNode, selectedRegion);
        } else {
          checkPeersClear(allDataNodeId, destDataNode, selectedRegion);
        }
      }

    } catch (InconsistentDataException ignore) {

    }
    LOGGER.info("test pass");
  }

  private void restartAllDataNodes() {
    EnvFactory.getEnv()
        .getDataNodeWrapperList()
        .parallelStream()
        .forEach(
            nodeWrapper -> {
              nodeWrapper.stopForcibly();
              nodeWrapper.start();
            });
  }

  private void setConfigNodeKillPoints(
      KeySetView<String, Boolean> killConfigNodeKeywords, int nodeRestartTime) {
    EnvFactory.getEnv()
        .getConfigNodeWrapperList()
        .forEach(
            configNodeWrapper ->
                executorService.submit(
                    () ->
                        nodeLogKillPoint(
                            configNodeWrapper, killConfigNodeKeywords, nodeRestartTime)));
  }

  private void setDataNodeKillPoints(
      KeySetView<String, Boolean> killDataNodeKeywords, int nodeRestartTime) {
    EnvFactory.getEnv()
        .getDataNodeWrapperList()
        .forEach(
            dataNodeWrapper ->
                executorService.submit(
                    () ->
                        nodeLogKillPoint(dataNodeWrapper, killDataNodeKeywords, nodeRestartTime)));
  }

  /**
   * Monitor the node's log and kill it when detect specific log.
   *
   * @param nodeWrapper Easy to understand
   * @param killNodeKeywords When detect these keywords in node's log, stop the node forcibly
   */
  private static void nodeLogKillPoint(
      AbstractNodeWrapper nodeWrapper,
      KeySetView<String, Boolean> killNodeKeywords,
      int restartTime) {
    if (killNodeKeywords.isEmpty()) {
      return;
    }
    final String logFileName;
    if (nodeWrapper instanceof ConfigNodeWrapper) {
      logFileName = "log_confignode_all.log";
    } else {
      logFileName = "log_datanode_all.log";
    }
    SystemType type = SystemType.getSystemType();
    ProcessBuilder builder;
    if (type == SystemType.LINUX || type == SystemType.MAC) {
      builder =
          new ProcessBuilder(
              "tail",
              "-f",
              nodeWrapper.getNodePath() + File.separator + "logs" + File.separator + logFileName);
    } else if (type == SystemType.WINDOWS) {
      builder =
          new ProcessBuilder(
              "powershell",
              "-Command",
              "Get-Content "
                  + nodeWrapper.getNodePath()
                  + File.separator
                  + "logs"
                  + File.separator
                  + logFileName
                  + " -Wait");
    } else {
      throw new UnsupportedOperationException("Unsupported system type " + type);
    }
    builder.redirectErrorStream(true);

    try {
      Process process = builder.start();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          // if trigger more than one keyword at a same time, test code may have mistakes
          Assert.assertTrue(
              line,
              killNodeKeywords.stream()
                      .map(KillPoint::addKillPointPrefix)
                      .filter(line::contains)
                      .count()
                  <= 1);
          String finalLine = line;
          Optional<String> detectedKeyword =
              killNodeKeywords.stream()
                  .filter(keyword -> finalLine.contains(KillPoint.addKillPointPrefix(keyword)))
                  .findAny();
          if (detectedKeyword.isPresent()) {
            // each keyword only trigger once
            killNodeKeywords.remove(detectedKeyword.get());
            LOGGER.info("Kill point is triggered: {}", detectedKeyword);
            // reboot the node
            nodeWrapper.stopForcibly();
            if (restartTime > 0) {
              try {
                TimeUnit.SECONDS.sleep(restartTime);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
            nodeWrapper.start();
          }
          if (killNodeKeywords.isEmpty()) {
            break;
          }
        }
      } catch (AssertionError e) {
        LOGGER.error("gg", e);
        throw e;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void checkKillPointsAllTriggered(KeySetView<String, Boolean> killPoints) {
    if (!killPoints.isEmpty()) {
      killPoints.forEach(killPoint -> LOGGER.error("Kill point {} not triggered", killPoint));
      Assert.fail("Some kill points was not triggered");
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
        .orElseThrow(() -> new RuntimeException("cannot find original DataNode"));
  }

  private static int selectDestDataNode(
      Set<Integer> dataNodeSet, Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return dataNodeSet.stream()
        .filter(dataNodeId -> !regionMap.get(selectedRegion).contains(dataNodeId))
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find dest DataNode"));
  }

  private static void awaitUntilSuccess(
      Statement statement, int selectedRegion, int originalDataNode, int destDataNode) {
    AtomicReference<Set<Integer>> lastTimeDataNodes = new AtomicReference<>();
    AtomicReference<Exception> lastException = new AtomicReference<>();
    try {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .until(
              () -> {
                try {
                  Map<Integer, Set<Integer>> newRegionMap =
                      getRegionMap(statement.executeQuery(SHOW_REGIONS));
                  Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
                  lastTimeDataNodes.set(dataNodes);
                  return !dataNodes.contains(originalDataNode) && dataNodes.contains(destDataNode);
                } catch (Exception e) {
                  // Any exception can be ignored
                  lastException.set(e);
                  return false;
                }
              });
    } catch (ConditionTimeoutException e) {
      if (lastTimeDataNodes.get() == null) {
        LOGGER.error(
            "maybe show regions fail, lastTimeDataNodes is null, last Exception:",
            lastException.get());
        throw e;
      }
      String actualSetStr = lastTimeDataNodes.get().toString();
      lastTimeDataNodes.get().remove(originalDataNode);
      lastTimeDataNodes.get().add(destDataNode);
      String expectSetStr = lastTimeDataNodes.toString();
      LOGGER.info("DataNode Set {} is unexpected, expect {}", actualSetStr, expectSetStr);
      throw e;
    }
  }

  private static void checkRegionFileExist(int dataNode) {
    File originalRegionDir = new File(buildRegionDirPath(dataNode));
    Assert.assertTrue(originalRegionDir.isDirectory());
    Assert.assertNotEquals(0, Objects.requireNonNull(originalRegionDir.listFiles()).length);
  }

  /** Check whether the original DataNode's region file has been deleted. */
  private static void checkRegionFileClear(int dataNode) {
    File originalRegionDir = new File(buildRegionDirPath(dataNode));
    Assert.assertTrue(originalRegionDir.isDirectory());
    Assert.assertEquals(0, Objects.requireNonNull(originalRegionDir.listFiles()).length);
    LOGGER.info("Original region clear");
  }

  private static void checkPeersExist(Set<Integer> dataNodes, int originalDataNode, int regionId) {
    dataNodes.forEach(targetDataNode -> checkPeerExist(targetDataNode, originalDataNode, regionId));
  }

  private static void checkPeerExist(int checkTargetDataNode, int originalDataNode, int regionId) {
    File expectExistedFile =
        new File(buildConfigurationDataFilePath(checkTargetDataNode, originalDataNode, regionId));
    Assert.assertTrue(
        "configuration file should exist, but it didn't: " + expectExistedFile.getPath(),
        expectExistedFile.exists());
  }

  private static void checkPeersClear(Set<Integer> dataNodes, int originalDataNode, int regionId) {
    dataNodes.stream()
        .filter(dataNode -> dataNode != originalDataNode)
        .forEach(targetDataNode -> checkPeerClear(targetDataNode, originalDataNode, regionId));
    LOGGER.info("Peer clear");
  }

  private static void checkPeerClear(int checkTargetDataNode, int originalDataNode, int regionId) {
    File expectDeletedFile =
        new File(buildConfigurationDataFilePath(checkTargetDataNode, originalDataNode, regionId));
    Assert.assertFalse(
        "configuration file should be deleted, but it didn't: " + expectDeletedFile.getPath(),
        expectDeletedFile.exists());
  }

  private static String buildRegionDirPath(int dataNode) {
    String nodePath = EnvFactory.getEnv().dataNodeIdToWrapper(dataNode).get().getNodePath();
    return nodePath
        + File.separator
        + IoTDBConstant.DATA_FOLDER_NAME
        + File.separator
        + "datanode"
        + File.separator
        + IoTDBConstant.CONSENSUS_FOLDER_NAME
        + File.separator
        + IoTDBConstant.DATA_REGION_FOLDER_NAME;
  }

  private static String buildConfigurationDataFilePath(
      int localDataNodeId, int remoteDataNodeId, int regionId) {
    String configurationDatDirName =
        buildRegionDirPath(localDataNodeId) + File.separator + "1_" + regionId;
    String expectDeletedFileName =
        IoTConsensusServerImpl.generateConfigurationDatFileName(remoteDataNodeId);
    return configurationDatDirName + File.separator + expectDeletedFileName;
  }

  protected static KeySetView<String, Boolean> noKillPoints() {
    return ConcurrentHashMap.newKeySet();
  }

  @SafeVarargs
  protected static <T extends Enum<?>> KeySetView<String, Boolean> buildSet(T... keywords) {
    KeySetView<String, Boolean> result = ConcurrentHashMap.newKeySet();
    result.addAll(
        Arrays.stream(keywords).map(KillPoint::enumToString).collect(Collectors.toList()));
    return result;
  }
}
