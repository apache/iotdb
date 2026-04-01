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
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.metrics.utils.SystemType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.thrift.TException;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.utils.Pair;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

public class IoTDBRegionOperationReliabilityITFramework {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionOperationReliabilityITFramework.class);
  public static final String INSERTION1 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 1, 2)";
  private static final String INSERTION2 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(101, 3, 4)";
  public static final String FLUSH_COMMAND = "flush on cluster";
  private static final String SHOW_REGIONS = "show regions";
  private static final String SHOW_DATANODES = "show datanodes";
  private static final String COUNT_TIMESERIES = "select count(*) from root.sg.**";
  private static final String REGION_MIGRATE_COMMAND_FORMAT = "migrate region %d from %d to %d";
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

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // prepare data
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      // collect necessary information
      Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
      Set<Integer> allDataNodeId = getAllDataNodes(statement);

      // select region migration related DataNodes
      final int selectedRegion = selectRegion(regionMap);
      final int originalDataNode = selectOriginalDataNode(regionMap, selectedRegion);
      final int destDataNode =
          selectDataNodeNotContainsRegion(allDataNodeId, regionMap, selectedRegion);
      checkRegionFileExist(originalDataNode);

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
      Predicate<TShowRegionResp> migrateRegionPredicate =
          tShowRegionResp -> {
            Map<Integer, Set<Integer>> newRegionMap =
                getRegionMap(tShowRegionResp.getRegionInfoList());
            Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
            return !dataNodes.contains(originalDataNode) && dataNodes.contains(destDataNode);
          };
      try {
        awaitUntilSuccess(
            client,
            selectedRegion,
            migrateRegionPredicate,
            Optional.of(destDataNode),
            Optional.of(originalDataNode));
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
        // TODO: @YongzaoDan enable this check after the __system database is refactored!!!
        //        checkClusterStillWritable();
      } else {
        checkRegionFileClearIfNodeAlive(destDataNode);
        checkRegionFileExistIfNodeAlive(originalDataNode);
      }

      LOGGER.info("test pass");
    }
  }

  public static Set<Integer> getAllDataNodes(Statement statement) throws Exception {
    ResultSet result = statement.executeQuery(SHOW_DATANODES);
    Set<Integer> allDataNodeId = new HashSet<>();
    while (result.next()) {
      allDataNodeId.add(result.getInt(ColumnHeaderConstant.NODE_ID));
    }
    return allDataNodeId;
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

  public static Map<Integer, Set<Integer>> getDataRegionMap(Statement statement) throws Exception {
    ResultSet showRegionsResult = statement.executeQuery(SHOW_REGIONS);
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    while (showRegionsResult.next()) {
      if (String.valueOf(TConsensusGroupType.DataRegion)
              .equals(showRegionsResult.getString(ColumnHeaderConstant.TYPE))
          && !showRegionsResult
              .getString(ColumnHeaderConstant.DATABASE)
              .equals(SystemConstant.SYSTEM_DATABASE)
          && !showRegionsResult
              .getString(ColumnHeaderConstant.DATABASE)
              .equals(SystemConstant.AUDIT_DATABASE)) {
        int regionId = showRegionsResult.getInt(ColumnHeaderConstant.REGION_ID);
        int dataNodeId = showRegionsResult.getInt(ColumnHeaderConstant.DATA_NODE_ID);
        regionMap.computeIfAbsent(regionId, id -> new HashSet<>()).add(dataNodeId);
      }
    }
    return regionMap;
  }

  public static Map<Integer, Pair<Integer, Set<Integer>>> getDataRegionMapWithLeader(
      Statement statement) throws Exception {
    ResultSet showRegionsResult = statement.executeQuery(SHOW_REGIONS);
    Map<Integer, Pair<Integer, Set<Integer>>> regionMap = new HashMap<>();
    while (showRegionsResult.next()) {
      if (String.valueOf(TConsensusGroupType.DataRegion)
              .equals(showRegionsResult.getString(ColumnHeaderConstant.TYPE))
          && !showRegionsResult
              .getString(ColumnHeaderConstant.DATABASE)
              .equals(SystemConstant.SYSTEM_DATABASE)
          && !showRegionsResult
              .getString(ColumnHeaderConstant.DATABASE)
              .equals(SystemConstant.AUDIT_DATABASE)) {
        int regionId = showRegionsResult.getInt(ColumnHeaderConstant.REGION_ID);
        int dataNodeId = showRegionsResult.getInt(ColumnHeaderConstant.DATA_NODE_ID);
        Pair<Integer, Set<Integer>> leaderNodesPair =
            regionMap.computeIfAbsent(regionId, id -> new Pair<>(-1, new HashSet<>()));
        leaderNodesPair.getRight().add(dataNodeId);
        if (showRegionsResult.getString(ColumnHeaderConstant.ROLE).equals("Leader")) {
          leaderNodesPair.setLeft(dataNodeId);
        }
      }
    }
    return regionMap;
  }

  public static Map<Integer, Set<Integer>> getAllRegionMap(Statement statement) throws Exception {
    ResultSet showRegionsResult = statement.executeQuery(SHOW_REGIONS);
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    while (showRegionsResult.next()) {
      int regionId = showRegionsResult.getInt(ColumnHeaderConstant.REGION_ID);
      int dataNodeId = showRegionsResult.getInt(ColumnHeaderConstant.DATA_NODE_ID);
      regionMap.computeIfAbsent(regionId, id -> new HashSet<>()).add(dataNodeId);
    }
    return regionMap;
  }

  protected static Map<Integer, Set<Integer>> getRegionMap(List<TRegionInfo> regionInfoList) {
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

  protected static Map<Integer, Set<Integer>> getRunningRegionMap(
      List<TRegionInfo> regionInfoList) {
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    regionInfoList.stream()
        .filter(regionInfo -> RegionStatus.Running.getStatus().equals(regionInfo.getStatus()))
        .forEach(
            regionInfo -> {
              int regionId = regionInfo.getConsensusGroupId().getId();
              regionMap
                  .computeIfAbsent(regionId, regionId1 -> new HashSet<>())
                  .add(regionInfo.getDataNodeId());
            });
    return regionMap;
  }

  protected static int selectRegion(Map<Integer, Set<Integer>> regionMap) {
    return regionMap.keySet().stream().findAny().orElseThrow(() -> new RuntimeException("gg"));
  }

  private static int selectOriginalDataNode(
      Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return regionMap.get(selectedRegion).stream()
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find original DataNode"));
  }

  protected static int selectDataNodeNotContainsRegion(
      Set<Integer> dataNodeSet, Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return dataNodeSet.stream()
        .filter(dataNodeId -> !regionMap.get(selectedRegion).contains(dataNodeId))
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find dest DataNode"));
  }

  protected static int selectDataNodeContainsRegion(
      Set<Integer> dataNodeSet, Map<Integer, Set<Integer>> regionMap, int selectedRegion) {
    return dataNodeSet.stream()
        .filter(dataNodeId -> regionMap.get(selectedRegion).contains(dataNodeId))
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find dest DataNode"));
  }

  // I believe this function is not necessary, just keep it here in case it's necessary
  private static void awaitUntilFlush(Statement statement, int originalDataNode) throws Exception {
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

  protected static void awaitUntilSuccess(
      SyncConfigNodeIServiceClient client,
      int selectedRegion,
      Predicate<TShowRegionResp> predicate,
      Optional<Integer> dataNodeExpectInRegionGroup,
      Optional<Integer> dataNodeExpectNotInRegionGroup) {
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
                  lastTimeDataNodes.set(getRegionMap(resp.getRegionInfoList()).get(selectedRegion));
                  return predicate.test(resp);
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
      dataNodeExpectNotInRegionGroup.ifPresent(x -> lastTimeDataNodes.get().remove(x));
      dataNodeExpectInRegionGroup.ifPresent(x -> lastTimeDataNodes.get().add(x));
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
    File[] files = originalRegionDir.listFiles();
    try {
      int length = Objects.requireNonNull(files).length;
      // the node may still have a region of the system database
      Assert.assertTrue(length == 0 || length == 1 && files[0].getName().equals("1_1"));
    } catch (AssertionError e) {
      LOGGER.error(
          "Original DataNode {} region file not clear, these files is still remain: {}",
          dataNode,
          Arrays.toString(files));
      throw e;
    }
    LOGGER.info("Original DataNode {} region file clear", dataNode);
  }

  private void checkClusterStillWritable() {
    try (Connection connection = EnvFactory.getEnv().getAvailableConnection();
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

  protected static Map<Integer, String> getRegionStatusWithoutRunning(Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("show regions");
    final int regionIdIndex = dataSet.getColumnNames().indexOf("RegionId");
    final int regionStatusIndex = dataSet.getColumnNames().indexOf("Status");
    dataSet.setFetchSize(1024);
    Map<Integer, String> result = new TreeMap<>();
    while (dataSet.hasNext()) {
      List<Field> fields = dataSet.next().getFields();
      final int regionId = fields.get(regionIdIndex).getIntV();
      final String regionStatus = fields.get(regionStatusIndex).toString();
      if (!"Running".equals(regionStatus)) {
        result.putIfAbsent(regionId, regionStatus);
      }
    }
    return result;
  }
}
