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
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.exception.InconsistentDataException;
import org.apache.iotdb.metrics.utils.SystemType;

import org.apache.thrift.TException;
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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class IoTDBRegionMigrateReliabilityITFramework {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionMigrateReliabilityITFramework.class);
  private static final String INSERTION1 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 1, 2)";
  private static final String INSERTION2 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(101, 3, 4)";
  private static final String FLUSH_COMMAND = "flush";
  private static final String SHOW_REGIONS = "show regions";
  private static final String SHOW_DATANODES = "show datanodes";
  private static final String COUNT_TIMESERIES = "select count(*) from root.sg.**";
  private static final String REGION_MIGRATE_COMMAND_FORMAT = "migrate region %d from %d to %d";
  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  ExecutorService executorService = IoTDBThreadPoolFactory.newCachedThreadPool("regionMigrateIT");

  public static Consumer<KillPointContext> actionOfKillNode =
      context -> {
        context.getNodeWrapper().stopForcibly();
        LOGGER.info("Node {} stopped.", context.getNodeWrapper().getId());
        Assert.assertFalse(context.getNodeWrapper().isAlive());
        if (context.getNodeWrapper() instanceof ConfigNodeWrapper) {
          context.getNodeWrapper().start();
          LOGGER.info("Node {} restarted.", context.getNodeWrapper().getId());
          Assert.assertTrue(context.getNodeWrapper().isAlive());
        }
      };

  public static Consumer<KillPointContext> actionOfRestartCluster =
      context -> {
        context.getEnv().getNodeWrapperList().parallelStream()
            .forEach(AbstractNodeWrapper::stopForcibly);
        LOGGER.info("Cluster has been stopped");
        context.getEnv().getNodeWrapperList().parallelStream().forEach(AbstractNodeWrapper::start);
        LOGGER.info("Cluster has been restarted");
      };

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS_V2)
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
      KeySetView<String, Boolean> killDataNodeKeywords,
      KillNode killNode)
      throws Exception {
    generalTestWithAllOptions(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        killConfigNodeKeywords,
        killDataNodeKeywords,
        actionOfKillNode,
        true,
        killNode);
  }

  public void failTest(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      KeySetView<String, Boolean> killConfigNodeKeywords,
      KeySetView<String, Boolean> killDataNodeKeywords,
      KillNode killNode)
      throws Exception {
    generalTestWithAllOptions(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        killConfigNodeKeywords,
        killDataNodeKeywords,
        actionOfKillNode,
        false,
        killNode);
  }

  public void killClusterTest(
      KeySetView<String, Boolean> configNodeKeywords, boolean expectMigrateSuccess)
      throws Exception {
    generalTestWithAllOptions(
        2,
        3,
        3,
        3,
        configNodeKeywords,
        noKillPoints(),
        actionOfRestartCluster,
        expectMigrateSuccess,
        KillNode.ALL_NODES);
  }

  // region general test

  public void generalTestWithAllOptions(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      KeySetView<String, Boolean> configNodeKeywords,
      KeySetView<String, Boolean> dataNodeKeywords,
      Consumer<KillPointContext> actionWhenDetectKeyWords,
      final boolean expectMigrateSuccess,
      KillNode killNode)
      throws Exception {
    // prepare env
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(dataReplicateFactor)
        .setSchemaReplicationFactor(schemaReplicationFactor);
    EnvFactory.getEnv().registerConfigNodeKillPoints(new ArrayList<>(configNodeKeywords));
    EnvFactory.getEnv().registerDataNodeKillPoints(new ArrayList<>(dataNodeKeywords));
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, dataNodeNum);

    try (final Connection connection = closeQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = closeQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      statement.execute(INSERTION1);

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

      try {
        awaitUntilFlush(statement, originalDataNode);
      } catch (ConditionTimeoutException e) {
        LOGGER.error("Flush timeout:", e);
        Assert.fail();
      }

      // set kill points
      if (killNode == KillNode.ORIGINAL_DATANODE) {
        setDataNodeKillPoints(
            Collections.singletonList(
                EnvFactory.getEnv().dataNodeIdToWrapper(originalDataNode).get()),
            dataNodeKeywords,
            actionWhenDetectKeyWords);
      } else if (killNode == KillNode.DESTINATION_DATANODE) {
        setDataNodeKillPoints(
            Collections.singletonList(EnvFactory.getEnv().dataNodeIdToWrapper(destDataNode).get()),
            dataNodeKeywords,
            actionWhenDetectKeyWords);
      } else {
        setConfigNodeKillPoints(configNodeKeywords, actionWhenDetectKeyWords);
        setDataNodeKillPoints(
            EnvFactory.getEnv().getDataNodeWrapperList(),
            dataNodeKeywords,
            actionWhenDetectKeyWords);
      }

      LOGGER.info("DataNode set before migration: {}", regionMap.get(selectedRegion));

      System.out.println(
          "originalDataNode: "
              + EnvFactory.getEnv().dataNodeIdToWrapper(originalDataNode).get().getNodePath());
      System.out.println(
          "destDataNode: "
              + EnvFactory.getEnv().dataNodeIdToWrapper(destDataNode).get().getNodePath());

      // region migration start
      statement.execute(buildRegionMigrateCommand(selectedRegion, originalDataNode, destDataNode));

      boolean success = false;
      try {
        awaitUntilSuccess(client, selectedRegion, originalDataNode, destDataNode);
        success = true;
      } catch (ConditionTimeoutException e) {
        if (expectMigrateSuccess) {
          LOGGER.error("Region migrate failed", e);
          Assert.fail();
        }
      }
      if (!expectMigrateSuccess && success) {
        LOGGER.error("Region migrate succeeded unexpectedly");
        Assert.fail();
      }

      // make sure all kill points have been triggered
      checkKillPointsAllTriggered(configNodeKeywords);
      checkKillPointsAllTriggered(dataNodeKeywords);

      // check the remaining file
      if (success) {
        checkRegionFileClearIfNodeAlive(originalDataNode);
        checkRegionFileExistIfNodeAlive(destDataNode);
        checkPeersClearIfNodeAlive(allDataNodeId, originalDataNode, selectedRegion);
        checkClusterStillWritable();
      } else {
        checkRegionFileClearIfNodeAlive(destDataNode);
        checkRegionFileExistIfNodeAlive(originalDataNode);
        checkPeersClearIfNodeAlive(allDataNodeId, destDataNode, selectedRegion);
      }

      LOGGER.info("test pass");
    } catch (InconsistentDataException ignore) {

    }
  }

  private void setConfigNodeKillPoints(
      KeySetView<String, Boolean> killConfigNodeKeywords, Consumer<KillPointContext> action) {
    EnvFactory.getEnv()
        .getConfigNodeWrapperList()
        .forEach(
            configNodeWrapper ->
                executorService.submit(
                    () ->
                        doActionWhenDetectKeywords(
                            configNodeWrapper, killConfigNodeKeywords, action)));
  }

  private void setDataNodeKillPoints(
      List<DataNodeWrapper> dataNodeWrappers,
      KeySetView<String, Boolean> killDataNodeKeywords,
      Consumer<KillPointContext> action) {
    dataNodeWrappers.forEach(
        dataNodeWrapper ->
            executorService.submit(
                () -> doActionWhenDetectKeywords(dataNodeWrapper, killDataNodeKeywords, action)));
  }

  /**
   * Monitor the node's log and kill it when detect specific log.
   *
   * @param nodeWrapper Easy to understand
   * @param keywords When detect these keywords in node's log, stop the node forcibly
   */
  private static void doActionWhenDetectKeywords(
      AbstractNodeWrapper nodeWrapper,
      KeySetView<String, Boolean> keywords,
      Consumer<KillPointContext> action) {
    if (keywords.isEmpty()) {
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
              keywords.stream().map(KillPoint::addKillPointPrefix).filter(line::contains).count()
                  <= 1);
          String finalLine = line;
          Optional<String> detectedKeyword =
              keywords.stream()
                  .filter(keyword -> finalLine.contains(KillPoint.addKillPointPrefix(keyword)))
                  .findAny();
          if (detectedKeyword.isPresent()) {
            // each keyword only trigger once
            keywords.remove(detectedKeyword.get());
            action.accept(new KillPointContext(nodeWrapper, (AbstractEnv) EnvFactory.getEnv()));
            LOGGER.info("Kill point triggered: {}", detectedKeyword.get());
          }
          if (keywords.isEmpty()) {
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

  private static String buildRegionMigrateCommand(int who, int from, int to) {
    String result = String.format(REGION_MIGRATE_COMMAND_FORMAT, who, from, to);
    LOGGER.info(result);
    return result;
  }

  public static Map<Integer, Set<Integer>> getRegionMap(ResultSet showRegionsResult)
      throws SQLException {
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    while (showRegionsResult.next()) {
      if (String.valueOf(TConsensusGroupType.DataRegion)
          .equals(showRegionsResult.getString(ColumnHeaderConstant.TYPE))) {
        int regionId = showRegionsResult.getInt(ColumnHeaderConstant.REGION_ID);
        int dataNodeId = showRegionsResult.getInt(ColumnHeaderConstant.DATA_NODE_ID);
        regionMap.computeIfAbsent(regionId, id -> new HashSet<>()).add(dataNodeId);
      }
    }
    return regionMap;
  }

  private static Map<Integer, Set<Integer>> getRegionMap(List<TRegionInfo> regionInfoList) {
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    regionInfoList.forEach(
        regionInfo -> {
          int regionId = regionInfo.getConsensusGroupId().getId();
          regionMap
              .computeIfAbsent(regionId, regionId1 -> new HashSet<>())
              .add(regionInfo.getDataNodeId());
        });
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

  private static void awaitUntilFlush(Statement statement, int originalDataNode) {
    long startTime = System.currentTimeMillis();
    File sequence = new File(buildDataPath(originalDataNode, true));
    File unsequence = new File(buildDataPath(originalDataNode, false));
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .pollDelay(2, TimeUnit.SECONDS)
        .until(
            () -> {
              statement.execute(FLUSH_COMMAND);
              int fileNum = 0;
              if (sequence.exists() && sequence.listFiles() != null) {
                fileNum += Objects.requireNonNull(sequence.listFiles()).length;
              }
              if (unsequence.exists() && unsequence.listFiles() != null) {
                fileNum += Objects.requireNonNull(unsequence.listFiles()).length;
              }
              return fileNum > 0;
            });
    LOGGER.info("DataNode {} has been flushed", originalDataNode);
    LOGGER.info("Flush cost time: {}ms", System.currentTimeMillis() - startTime);
  }

  private static void awaitUntilSuccess(
      SyncConfigNodeIServiceClient client,
      int selectedRegion,
      int originalDataNode,
      int destDataNode) {
    AtomicReference<Set<Integer>> lastTimeDataNodes = new AtomicReference<>();
    AtomicReference<Exception> lastException = new AtomicReference<>();
    AtomicReference<SyncConfigNodeIServiceClient> clientRef = new AtomicReference<>(client);
    try {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(2, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  TShowRegionResp resp = clientRef.get().showRegion(new TShowRegionReq());
                  Map<Integer, Set<Integer>> newRegionMap = getRegionMap(resp.getRegionInfoList());
                  Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
                  lastTimeDataNodes.set(dataNodes);
                  return !dataNodes.contains(originalDataNode) && dataNodes.contains(destDataNode);
                } catch (TException e) {
                  clientRef.set(
                      (SyncConfigNodeIServiceClient)
                          EnvFactory.getEnv().getLeaderConfigNodeConnection());
                  lastException.set(e);
                  return false;
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
      LOGGER.error("DataNode Set {} is unexpected, expect {}", actualSetStr, expectSetStr);
      if (lastException.get() == null) {
        LOGGER.info("No exception during awaiting");
      } else {
        LOGGER.error("Last exception during awaiting:", lastException.get());
      }
      throw e;
    }
    LOGGER.info("DataNode set has been successfully changed to {}", lastTimeDataNodes.get());
  }

  private static void checkRegionFileExistIfNodeAlive(int dataNode) {
    if (EnvFactory.getEnv().dataNodeIdToWrapper(dataNode).get().isAlive()) {
      checkRegionFileExist(dataNode);
    }
  }

  private static void checkRegionFileExist(int dataNode) {
    File originalRegionDir = new File(buildRegionDirPath(dataNode));
    Assert.assertTrue(originalRegionDir.isDirectory());
    Assert.assertNotEquals(0, Objects.requireNonNull(originalRegionDir.listFiles()).length);
  }

  private static void checkRegionFileClearIfNodeAlive(int dataNode) {
    if (EnvFactory.getEnv().dataNodeIdToWrapper(dataNode).get().isAlive()) {
      checkRegionFileClear(dataNode);
    }
  }

  /** Check whether the original DataNode's region file has been deleted. */
  private static void checkRegionFileClear(int dataNode) {
    File originalRegionDir = new File(buildRegionDirPath(dataNode));
    Assert.assertTrue(originalRegionDir.isDirectory());
    try {
      Assert.assertEquals(0, Objects.requireNonNull(originalRegionDir.listFiles()).length);
    } catch (AssertionError e) {
      LOGGER.error(
          "Original DataNode {} region file not clear, these files is still remain: {}",
          dataNode,
          Arrays.toString(originalRegionDir.listFiles()));
      throw e;
    }
    LOGGER.info("Original DataNode {} region file clear", dataNode);
  }

  private static void checkPeersExistIfNodeAlive(
      Set<Integer> dataNodes, int originalDataNode, int regionId) {
    dataNodes.forEach(
        targetDataNode -> checkPeerExistIfNodeAlive(targetDataNode, originalDataNode, regionId));
  }

  private static void checkPeersExist(Set<Integer> dataNodes, int originalDataNode, int regionId) {
    dataNodes.forEach(targetDataNode -> checkPeerExist(targetDataNode, originalDataNode, regionId));
  }

  private static void checkPeerExistIfNodeAlive(
      int checkTargetDataNode, int originalDataNode, int regionId) {
    if (EnvFactory.getEnv().dataNodeIdToWrapper(checkTargetDataNode).get().isAlive()) {
      checkPeerExist(checkTargetDataNode, originalDataNode, regionId);
    }
  }

  private static void checkPeerExist(int checkTargetDataNode, int originalDataNode, int regionId) {
    File expectExistedFile =
        new File(buildConfigurationDataFilePath(checkTargetDataNode, originalDataNode, regionId));
    Assert.assertTrue(
        "configuration file should exist, but it didn't: " + expectExistedFile.getPath(),
        expectExistedFile.exists());
  }

  private static void checkPeersClearIfNodeAlive(
      Set<Integer> dataNodes, int originalDataNode, int regionId) {
    dataNodes.stream()
        .filter(dataNode -> dataNode != originalDataNode)
        .forEach(
            targetDataNode ->
                checkPeerClearIfNodeAlive(targetDataNode, originalDataNode, regionId));
  }

  private static void checkPeersClear(Set<Integer> dataNodes, int originalDataNode, int regionId) {
    dataNodes.stream()
        .filter(dataNode -> dataNode != originalDataNode)
        .forEach(targetDataNode -> checkPeerClear(targetDataNode, originalDataNode, regionId));
    LOGGER.info("Peer clear");
  }

  private static void checkPeerClearIfNodeAlive(
      int checkTargetDataNode, int originalDataNode, int regionId) {
    if (EnvFactory.getEnv().dataNodeIdToWrapper(checkTargetDataNode).get().isAlive()) {
      checkPeerClear(checkTargetDataNode, originalDataNode, regionId);
    }
  }

  private static void checkPeerClear(int checkTargetDataNode, int originalDataNode, int regionId) {
    File expectDeletedFile =
        new File(buildConfigurationDataFilePath(checkTargetDataNode, originalDataNode, regionId));
    Assert.assertFalse(
        "configuration file should be deleted, but it didn't: " + expectDeletedFile.getPath(),
        expectDeletedFile.exists());
    LOGGER.info("configuration file has been deleted: {}", expectDeletedFile.getPath());
  }

  private void checkClusterStillWritable() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // check old data
      ResultSet resultSet = statement.executeQuery(COUNT_TIMESERIES);
      resultSet.next();
      Assert.assertEquals(1, resultSet.getLong(1));
      Assert.assertEquals(1, resultSet.getLong(2));
      LOGGER.info("Old data is still remain");
      // write new data
      statement.execute(INSERTION2);
      resultSet = statement.executeQuery(COUNT_TIMESERIES);
      resultSet.next();
      Assert.assertEquals(2, resultSet.getLong(1));
      Assert.assertEquals(2, resultSet.getLong(2));
      LOGGER.info("Region group is still writable");
    } catch (SQLException e) {
      LOGGER.error("Something wrong", e);
      Assert.fail("Something wrong");
    }
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

  private static String buildDataPath(int dataNode, boolean isSequence) {
    String nodePath = EnvFactory.getEnv().dataNodeIdToWrapper(dataNode).get().getNodePath();
    return nodePath
        + File.separator
        + IoTDBConstant.DATA_FOLDER_NAME
        + File.separator
        + "datanode"
        + File.separator
        + IoTDBConstant.DATA_FOLDER_NAME
        + File.separator
        + (isSequence ? IoTDBConstant.SEQUENCE_FOLDER_NAME : IoTDBConstant.UNSEQUENCE_FOLDER_NAME);
  }

  private static String buildConfigurationDataFilePath(
      int localDataNodeId, int remoteDataNodeId, int regionId) {
    String configurationDatDirName =
        buildRegionDirPath(localDataNodeId) + File.separator + "1_" + regionId;
    String expectDeletedFileName =
        IoTConsensusServerImpl.generateConfigurationDatFileName(
            remoteDataNodeId, CONFIGURATION_FILE_NAME);
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

  public static <T> T closeQuietly(T t) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          try {
            if (method.getName().equals("close")) {
              try {
                method.invoke(t, args);
              } catch (Throwable e) {
                LOGGER.warn("Exception happens during close(): ", e);
              }
              return null;
            } else {
              return method.invoke(t, args);
            }
          } catch (InvocationTargetException e) {
            throw e.getTargetException();
          }
        };
    return (T)
        Proxy.newProxyInstance(
            t.getClass().getClassLoader(), t.getClass().getInterfaces(), handler);
  }
}
