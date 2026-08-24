/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * LICENSE_FILE_NAME); you may not use this file except in compliance
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

package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllActivationStatusResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.ClusterConstant;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;

import com.google.common.collect.ImmutableMap;
import com.timecho.iotdb.commons.commission.Bandit;
import com.timecho.iotdb.commons.commission.Lottery;
import com.timecho.iotdb.commons.commission.complete.HmacProtocol;
import com.timecho.iotdb.commons.commission.obligation.ObligationStatus;
import com.timecho.iotdb.commons.external.codec.binary.Base32;
import com.timecho.iotdb.manager.regulate.RegulateManager;
import com.timecho.iotdb.session.Session;
import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.timecho.iotdb.commons.commission.Lottery.DATANODE_NUM_LIMIT_NAME;
import static com.timecho.iotdb.commons.commission.Lottery.DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME;
import static com.timecho.iotdb.commons.commission.Lottery.LICENSE_EXPIRE_TIMESTAMP_NAME;
import static com.timecho.iotdb.commons.commission.Lottery.LICENSE_ISSUE_TIMESTAMP_NAME;
import static com.timecho.iotdb.commons.commission.obligation.ObligationStatus.ACTIVATED;
import static com.timecho.iotdb.commons.commission.obligation.ObligationStatus.ACTIVE_ACTIVATED;
import static com.timecho.iotdb.commons.commission.obligation.ObligationStatus.ACTIVE_UNACTIVATED;
import static com.timecho.iotdb.commons.commission.obligation.ObligationStatus.PASSIVE_ACTIVATED;
import static com.timecho.iotdb.commons.commission.obligation.ObligationStatus.PASSIVE_UNACTIVATED;
import static com.timecho.iotdb.commons.commission.obligation.ObligationStatus.UNACTIVATED;
import static com.timecho.iotdb.commons.commission.obligation.ObligationStatus.UNKNOWN;
import static com.timecho.iotdb.manager.regulate.RegulateManager.LICENSE_FILE_NAME;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IoTDBActivationIT {
  private static final Logger logger = IoTDBTestLogger.logger;

  private static final String ratisConsensusProtocolClass =
      "org.apache.iotdb.consensus.ratis.RatisConsensus";

  private static final int testReplicationFactor = 1;

  private static String baseLicenseContent;
  private static Properties baseLicenseProperties = new Properties();

  private static final long shortDisconnectionTimeLimit = TimeUnit.SECONDS.toMillis(10);
  private static final long mediumDisconnectionTimeLimit = TimeUnit.SECONDS.toMillis(40);

  private static final long sleepLogThreshold = TimeUnit.SECONDS.toMillis(10);
  private static final long HEARTBEAT_TIMEOUT_TIME_IN_MS =
      ConfigNodeDescriptor.getInstance().getConf().getFailureDetectorFixedThresholdInMs();

  private static final String SKIP = "SKIP";

  private static final int baseTest = 0;
  private static final int normalTest = 0;
  private static final int bigSlowTest = 0;

  @BeforeClass
  public static void setUpClass() throws IOException {
    System.setProperty("TestEnv", "Timecho");
    baseLicenseProperties = RegulateManager.buildUnlimitedLicenseProperties();
    StringWriter writer = new StringWriter();
    baseLicenseProperties.store(writer, null);
    baseLicenseContent = writer.toString();
  }

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(ratisConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(ratisConsensusProtocolClass)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor);

    // This test will use MAIN_CONFIGNODE_CLASS_NAME_FOR_ACTIVATION_IT as default main class
    ClusterConstant.MAIN_CONFIGNODE_CLASS_NAME_FOR_IT =
        ClusterConstant.MAIN_CONFIGNODE_CLASS_NAME_FOR_ACTIVATION_IT;
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // region ConfigNode Test
  @Test
  @Order(baseTest)
  public void licenseFileManageTest() throws Exception {
    System.out.println(System.getProperty("user.dir"));
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      testStatusWithRetry(leaderClient, Collections.singletonList(PASSIVE_UNACTIVATED));

      // create license file
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
      testStatusWithRetry(leaderClient, Collections.singletonList(ACTIVE_ACTIVATED));

      // add datanode
      EnvFactory.getEnv().registerNewDataNode(true);
      testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED));

      // read license file, then compare
      TSStatus status = leaderClient.getLicenseFile(LICENSE_FILE_NAME);
      Assert.assertEquals(status.message, baseLicenseContent);

      // delete license file
      leaderClient.deleteLicenseFile(LICENSE_FILE_NAME);
      testPassiveUnactivated(leaderClient, 1, 1);
    }

    allCheckPass();
  }

  @Test
  @Order(baseTest)
  public void activateCnTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 0);
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(PASSIVE_UNACTIVATED, PASSIVE_UNACTIVATED, PASSIVE_UNACTIVATED));

      // activate leader
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
      testStatusWithRetry(
          leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED));

      // register a datanode
      EnvFactory.getEnv().registerNewDataNode(true);
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, ACTIVATED));

      // deactivate leader
      leaderClient.deleteLicenseFile(LICENSE_FILE_NAME);
      testPassiveUnactivated(leaderClient, 3, 1);

      // activate a follower
      SyncConfigNodeIServiceClient followerClient =
          (SyncConfigNodeIServiceClient)
              EnvFactory.getEnv().getConfigNodeConnection(getAnyFollowerIndex());
      followerClient.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
      waitLicenseReload();
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, ACTIVATED));
      followerClient.close();
    }

    allCheckPass();
  }

  @Test
  @Order(normalTest)
  public void activateAllCnTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 0);
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // activate all 3 cn
      for (int configNodeId = 0;
          configNodeId < EnvFactory.getEnv().getConfigNodeWrapperList().size();
          configNodeId++) {
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient)
                EnvFactory.getEnv().getConfigNodeConnection(configNodeId);
        client.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
        client.close();
      }
      waitLicenseReload();
      EnvFactory.getEnv().registerNewDataNode(true);
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(ACTIVE_ACTIVATED, ACTIVE_ACTIVATED, ACTIVE_ACTIVATED, ACTIVATED));

      // deactivate all 3 cn, check status each time
      SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(0);
      client.deleteLicenseFile(LICENSE_FILE_NAME);
      client.close();
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(ACTIVE_ACTIVATED, ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, ACTIVATED));

      client = (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(1);
      client.deleteLicenseFile(LICENSE_FILE_NAME);
      client.close();
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, ACTIVATED));

      client = (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(2);
      client.deleteLicenseFile(LICENSE_FILE_NAME);
      client.close();
      testPassiveUnactivated(leaderClient, 3, 1);
    }

    allCheckPass();
  }

  @Ignore // This test has been covered by licenseModifyParallelTest
  @Test
  public void activateCnBigTest() throws Exception {
    final int configNodeNum = 5;
    final int testTimes = 20;
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, 0);
    SyncConfigNodeIServiceClient leaderClient;
    Function<Map<ObligationStatus, Integer>, List<ObligationStatus>> mapToList =
        map -> {
          List<ObligationStatus> list = new ArrayList<>();
          for (ObligationStatus activateStatus : map.keySet()) {
            for (int i = 0; i < map.get(activateStatus); i++) {
              list.add(activateStatus);
            }
          }
          return list;
        };
    leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    testPassiveUnactivated(leaderClient, configNodeNum, 0);

    // Do operation for many times.
    // In each operation, there are 2 probabilities:
    // 1. randomly choose one ConfigNode, flip its activate state
    // 2. deactivate all ConfigNode
    Random random = new Random();
    Map<ObligationStatus, Integer> initStatusCountMap = new HashMap<>();
    initStatusCountMap.put(PASSIVE_ACTIVATED, configNodeNum - 1);
    initStatusCountMap.put(ACTIVE_ACTIVATED, 0);
    Map<ObligationStatus, Integer> statusCountMap = new HashMap<>(initStatusCountMap);
    for (int i = 0; i < testTimes; i++) {
      int choice = random.nextInt(10);
      if (choice < 9) {
        int randomId = random.nextInt(configNodeNum);
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(randomId);
        TSStatus status = client.getActivateStatus();
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          continue;
        }
        ObligationStatus activateStatus = ObligationStatus.valueOf(status.getMessage());
        if (ACTIVE_ACTIVATED.equals(activateStatus)) {
          client.deleteLicenseFile(LICENSE_FILE_NAME);
          statusCountMap.computeIfPresent(ACTIVE_ACTIVATED, (key, count) -> count - 1);
          statusCountMap.computeIfPresent(PASSIVE_ACTIVATED, (key, count) -> count + 1);
          logger.info(
              String.format(
                  "configNode%d deactivate",
                  EnvFactory.getEnv().getConfigNodeWrapper(randomId).getPort()));
        } else {
          client.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
          statusCountMap.computeIfPresent(activateStatus, (key, count) -> count - 1);
          statusCountMap.computeIfPresent(ACTIVE_ACTIVATED, (key, count) -> count + 1);
          logger.info(
              String.format(
                  "configNode%d activate",
                  EnvFactory.getEnv().getConfigNodeWrapper(randomId).getPort()));
        }
        client.close();
        // check cluster activate status
        testStatusWithRetry(leaderClient, mapToList.apply(statusCountMap));
      } else if (choice == 9) {
        // delete all license file
        for (int configNodeId = 0;
            configNodeId < EnvFactory.getEnv().getConfigNodeWrapperList().size();
            configNodeId++) {
          SyncConfigNodeIServiceClient client =
              (SyncConfigNodeIServiceClient)
                  EnvFactory.getEnv().getConfigNodeConnection(configNodeId);
          client.deleteLicenseFile(LICENSE_FILE_NAME);
          client.close();
        }
        logger.info("deactivate all configNodes");
        testPassiveUnactivated(leaderClient, 5, 0);
        statusCountMap = new HashMap<>(initStatusCountMap);
      }
      if (i % 10 == 0) {
        logger.info(String.format("activateCnBigTest: %d pass", i));
      }
    }

    allCheckPass();
  }

  @Test
  @Order(normalTest)
  public void licenseUpdateTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(2, 0);
    long baseIssueTimestamp =
        Long.parseLong(baseLicenseProperties.getProperty(LICENSE_ISSUE_TIMESTAMP_NAME));
    String license0 = buildLicenseFromBase();
    String license1 =
        buildLicenseFromBase(LICENSE_ISSUE_TIMESTAMP_NAME, String.valueOf(baseIssueTimestamp + 1));
    String license2 =
        buildLicenseFromBase(LICENSE_ISSUE_TIMESTAMP_NAME, String.valueOf(baseIssueTimestamp + 2));
    SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    SyncConfigNodeIServiceClient client0 =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(0);
    SyncConfigNodeIServiceClient client1 =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(1);

    // activate both two cn
    client0.setLicenseFile(LICENSE_FILE_NAME, license0);
    client1.setLicenseFile(LICENSE_FILE_NAME, license0);
    testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVE_ACTIVATED));

    // update license of one of them
    client0.setLicenseFile(LICENSE_FILE_NAME, license1); // expect cn1 -> PASSIVE
    testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED));

    client1.setLicenseFile(LICENSE_FILE_NAME, license0);
    testStatusFalse(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVE_ACTIVATED));

    client1.setLicenseFile(LICENSE_FILE_NAME, license2); // expect cn0 -> PASSIVE, cn1 -> ACTIVE
    waitLicenseReload();
    testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED));

    client0.setLicenseFile(LICENSE_FILE_NAME, license2); // expect cn0 -> ACTIVE
    testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVE_ACTIVATED));

    allCheckPass();
  }

  @Test
  public void licenseModifyParallelTest() throws Exception {
    final int configNodeNum = 5;
    final int testTimes = 20;
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, 0);
    List<SyncConfigNodeIServiceClient> clients = new ArrayList<>();
    try {
      SyncConfigNodeIServiceClient leaderClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
      for (int i = 0; i < configNodeNum; i++) {
        clients.add((SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(i));
      }

      // Prepare test stuff
      Random random = new Random();
      List<Consumer<String>> setAll = new ArrayList<>();
      long issueTimestamp =
          Long.parseLong(baseLicenseProperties.getProperty(LICENSE_ISSUE_TIMESTAMP_NAME));
      for (SyncConfigNodeIServiceClient client : clients) {
        setAll.add(
            (licenseContent) -> {
              try {
                client.setLicenseFile(LICENSE_FILE_NAME, licenseContent);
              } catch (TException e) {
                throw new RuntimeException(e);
              }
            });
      }

      // Do the test
      String licenseContent = baseLicenseContent;
      for (int i = 0; i < testTimes; i++) {
        // In each round, there are 3 probabilities:
        // 1. 20% chance: Activate all ConfigNodes.
        // 2. 20% chance: Randomly accumulate some licenses' issue timestamp.
        // 3. 60% chance: Randomly set each ConfigNodes to activated or unactivated.
        // Besides, ConfigNodes' licenses will be set in parallel.
        int choice1 = new Random().nextInt(5);
        if (choice1 == 0) {
          logger.info("Activate all");
          runConsumerInParallel(setAll, licenseContent);
          waitLicenseReload();
          testStatusAllActiveActivate(leaderClient, configNodeNum, 0);
        } else if (choice1 == 1) {
          int dontBeZero = 0;
          while (dontBeZero == 0) {
            dontBeZero = random.nextInt(configNodeNum);
          }
          final int updateNum = dontBeZero;
          logger.info("Update {} licenses' issue time to {}", updateNum, issueTimestamp);
          issueTimestamp++;
          logger.info("issueTimestamp changed to {}", issueTimestamp);
          licenseContent =
              buildLicenseFromBase(LICENSE_ISSUE_TIMESTAMP_NAME, String.valueOf(issueTimestamp));
          List<Runnable> updateRandomly = new ArrayList<>();
          HashSet<Integer> toUpdate = getNFromM(configNodeNum, updateNum);
          for (int j = 0; j < configNodeNum; j++) {
            final int finalJ = j;
            final String finalLicenseContent = licenseContent;
            if (toUpdate.contains(j)) {
              updateRandomly.add(
                  () -> {
                    try {
                      clients.get(finalJ).setLicenseFile(LICENSE_FILE_NAME, finalLicenseContent);
                    } catch (TException e) {
                      throw new RuntimeException(e);
                    }
                  });
            }
          }
          runInParallel(updateRandomly);
          waitLicenseReload();
          List<ObligationStatus> expectation = new ArrayList<>();
          for (int j = 0; j < updateNum; j++) {
            expectation.add(ACTIVE_ACTIVATED);
          }
          for (int j = 0; j < configNodeNum - updateNum; j++) {
            expectation.add(PASSIVE_ACTIVATED);
          }
          testStatusWithRetry(leaderClient, expectation);
        } else {
          // randomly set
          List<Runnable> setRandomly = new ArrayList<>();
          final int activeActivatedNum = random.nextInt(configNodeNum + 1);
          HashSet<Integer> toActivate = getNFromM(configNodeNum, activeActivatedNum);
          for (int j = 0; j < configNodeNum; j++) {
            final int finalJ = j;
            final String finalLicenseContent = licenseContent;
            if (toActivate.contains(j)) {
              setRandomly.add(
                  () -> {
                    try {
                      clients.get(finalJ).setLicenseFile(LICENSE_FILE_NAME, finalLicenseContent);
                    } catch (TException e) {
                      throw new RuntimeException(e);
                    }
                  });
            } else {
              setRandomly.add(
                  () -> {
                    try {
                      clients.get(finalJ).deleteLicenseFile(LICENSE_FILE_NAME);
                    } catch (TException e) {
                      throw new RuntimeException(e);
                    }
                  });
            }
          }
          logger.info("Set {} of {} to activated", activeActivatedNum, configNodeNum);
          runInParallel(setRandomly);
          waitLicenseReload();
          if (activeActivatedNum == 0) {
            testPassiveUnactivated(leaderClient, configNodeNum, 0);
          } else {
            List<ObligationStatus> expectation = new ArrayList<>();
            for (int j = 0; j < activeActivatedNum; j++) {
              expectation.add(ACTIVE_ACTIVATED);
            }
            for (int j = 0; j < configNodeNum - activeActivatedNum; j++) {
              expectation.add(PASSIVE_ACTIVATED);
            }
            testStatusWithRetry(leaderClient, expectation);
          }
        }
        logger.info("loop {} pass", i);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    allCheckPass();
  }

  @Test
  @Order(normalTest)
  public void licenseExpireTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 0);
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // set license which will expire in 10 seconds
      String willExpireSoonLicense =
          buildLicenseFromBase(
              LICENSE_EXPIRE_TIMESTAMP_NAME,
              String.valueOf(System.currentTimeMillis() + 10000),
              LICENSE_ISSUE_TIMESTAMP_NAME,
              "1");
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, willExpireSoonLicense);
      testStatusWithRetry(
          leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED));

      // Wait until the license expired
      saferSleep(10000);
      // Then, cluster should go into active unactivated state
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(ACTIVE_UNACTIVATED, PASSIVE_UNACTIVATED, PASSIVE_UNACTIVATED));

      // Give valid license to a follower, cluster should be activated again
      int followerIndex = (1 + EnvFactory.getEnv().getLeaderConfigNodeIndex()) % 3;
      SyncConfigNodeIServiceClient followerClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(followerIndex);
      followerClient.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
      testStatusWithRetry(
          leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED));
    }

    allCheckPass();
  }

  @Test
  public void activeFollowerDisconnectionTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 0);
    final String specialLicenseContent =
        buildLicenseFromBase(
            DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME,
            String.valueOf(shortDisconnectionTimeLimit));
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final int followerIndex = getAnyFollowerIndex();
      final SyncConfigNodeIServiceClient followerClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(followerIndex);
      final ConfigNodeWrapper followerWrapper =
          EnvFactory.getEnv().getConfigNodeWrapper(followerIndex);

      /* ******** test active follower disconnection ******** */

      // activate a follower
      followerClient.setLicenseFile(LICENSE_FILE_NAME, specialLicenseContent);
      testStatusWithRetry(
          leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED));

      // stop this follower
      followerWrapper.stop();
      saferSleep(HEARTBEAT_TIMEOUT_TIME_IN_MS + shortDisconnectionTimeLimit);
      testStatusWithRetry(
          leaderClient, Arrays.asList(PASSIVE_UNACTIVATED, PASSIVE_UNACTIVATED, UNKNOWN));
    }

    allCheckPass();
  }

  @Test
  public void activeLeaderDisconnectionTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(5, 0);
    final String specialLicenseContent =
        buildLicenseFromBase(
            DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME,
            String.valueOf(shortDisconnectionTimeLimit));
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    ConfigNodeWrapper leaderWrapper = EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex);

    /* ******** test active leader disconnection ******** */

    // activate the leader
    leaderClient.setLicenseFile(LICENSE_FILE_NAME, specialLicenseContent);
    testStatusWithRetry(
        leaderClient,
        Arrays.asList(
            ACTIVE_ACTIVATED,
            PASSIVE_ACTIVATED,
            PASSIVE_ACTIVATED,
            PASSIVE_ACTIVATED,
            PASSIVE_ACTIVATED));

    // stop the leader
    final int oldLeaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    leaderWrapper.stop();
    saferSleep(10000);
    leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    leaderWrapper =
        EnvFactory.getEnv().getConfigNodeWrapper(EnvFactory.getEnv().getLeaderConfigNodeIndex());
    testStatusWithRetry(
        leaderClient,
        Arrays.asList(
            PASSIVE_UNACTIVATED,
            PASSIVE_UNACTIVATED,
            PASSIVE_UNACTIVATED,
            PASSIVE_UNACTIVATED,
            UNKNOWN));

    // activate a follower
    int followerIndex = 0;
    while (followerIndex == oldLeaderIndex
        || followerIndex == EnvFactory.getEnv().getLeaderConfigNodeIndex()) {
      followerIndex++;
      followerIndex %= 5;
    }
    SyncConfigNodeIServiceClient followerClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(followerIndex);
    followerClient.setLicenseFile(LICENSE_FILE_NAME, specialLicenseContent);
    testStatusWithRetry(
        leaderClient,
        Arrays.asList(
            ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, UNKNOWN));

    // stop the new leader (which is passive activated)
    leaderWrapper.stop();
    saferSleep(10000);
    leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    testStatusWithRetry(
        leaderClient,
        Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, UNKNOWN, UNKNOWN));

    allCheckPass();
  }

  @Test
  public void activeFollowerDisconnection2Test() throws Exception {
    final long sleepInterval1 = HEARTBEAT_TIMEOUT_TIME_IN_MS + mediumDisconnectionTimeLimit / 4;
    Assert.assertTrue(sleepInterval1 < mediumDisconnectionTimeLimit);
    final long sleepInterval2 = mediumDisconnectionTimeLimit - sleepInterval1;
    EnvFactory.getEnv().initClusterEnvironment(3, 0);
    final String specialLicenseContent =
        buildLicenseFromBase(
            DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME,
            String.valueOf(mediumDisconnectionTimeLimit));
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection(); ) {
      final int followerIndex = getAnyFollowerIndex();
      SyncConfigNodeIServiceClient followerClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection(followerIndex);
      followerClient.setLicenseFile(LICENSE_FILE_NAME, specialLicenseContent);
      testStatusWithRetry(
          leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED));

      // stop the follower
      ConfigNodeWrapper followerWrapper = EnvFactory.getEnv().getConfigNodeWrapper(followerIndex);
      followerWrapper.stop();
      saferSleep(sleepInterval1);
      testStatusWithRetry(
          leaderClient, Arrays.asList(PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, UNKNOWN));
      saferSleep(sleepInterval2);
      testStatusWithRetry(
          leaderClient, Arrays.asList(PASSIVE_UNACTIVATED, PASSIVE_UNACTIVATED, UNKNOWN));
      allCheckPass();
    }
  }

  @Test
  public void activeLeaderDisconnection2Test() throws Exception {
    final long sleepInterval1 = HEARTBEAT_TIMEOUT_TIME_IN_MS + mediumDisconnectionTimeLimit / 4;
    Assert.assertTrue(sleepInterval1 < mediumDisconnectionTimeLimit);
    final long sleepInterval2 = mediumDisconnectionTimeLimit - sleepInterval1;
    EnvFactory.getEnv().initClusterEnvironment(3, 0);
    final String specialLicenseContent =
        buildLicenseFromBase(
            DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME,
            String.valueOf(mediumDisconnectionTimeLimit));

    SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    leaderClient.setLicenseFile(LICENSE_FILE_NAME, specialLicenseContent);
    testStatusWithRetry(
        leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, PASSIVE_ACTIVATED));

    // stop the leader
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    ConfigNodeWrapper leaderWrapper = EnvFactory.getEnv().getConfigNodeWrapper(leaderIndex);
    leaderWrapper.stop();
    saferSleep(sleepInterval1);
    leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    testStatusWithRetry(leaderClient, Arrays.asList(PASSIVE_ACTIVATED, PASSIVE_ACTIVATED, UNKNOWN));
    saferSleep(sleepInterval2);
    testStatusWithRetry(
        leaderClient, Arrays.asList(PASSIVE_UNACTIVATED, PASSIVE_UNACTIVATED, UNKNOWN));
    allCheckPass();
  }

  // endregion

  // region DataNode Test
  @Test
  @Order(normalTest)
  public void dataNodeNumLimitSequentialTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
    String nodeNumLimit0License = buildLicenseFromBase(DATANODE_NUM_LIMIT_NAME, "0");
    String nodeNumLimit1License = buildLicenseFromBase(DATANODE_NUM_LIMIT_NAME, "1");
    String nodeNumLimit2License = buildLicenseFromBase(DATANODE_NUM_LIMIT_NAME, "2");
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      List<DataNodeWrapper> dataNodeWrappers = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        DataNodeWrapper wrapper = EnvFactory.getEnv().generateRandomDataNodeWrapper();
        dataNodeWrappers.add(wrapper);
      }

      // 1. first datanode start fail
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, nodeNumLimit0License);
      testStatusAllActiveActivate(leaderClient, 1, 0);
      dataNodeWrappers.get(0).start();
      testStatusFalse(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED));
      dataNodeWrappers.get(0).stopForcibly();

      // 2. set license, then first datanode start success
      leaderClient.deleteLicenseFile(LICENSE_FILE_NAME);
      testPassiveUnactivated(leaderClient, 1, 0);
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, nodeNumLimit1License);
      testStatusWithRetry(leaderClient, Collections.singletonList(ACTIVE_ACTIVATED));
      dataNodeWrappers.get(0).start();
      testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED));

      // 3. second datanode start fail
      dataNodeWrappers.get(1).start();
      testStatusFalse(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED, ACTIVATED));
      dataNodeWrappers.get(1).stopForcibly();

      // 4. modify license, set nodeNumLimit to 2, then second datanode start success
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, nodeNumLimit2License);
      waitLicenseReload();
      dataNodeWrappers.get(1).start();
      testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED, ACTIVATED));
    }

    allCheckPass();
  }

  @Test
  public void dataNodeNumLimitParallelTest() throws Exception {
    final int dataNodeNum = 3;
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
    String nodeNumLimit2License = buildLicenseFromBase(DATANODE_NUM_LIMIT_NAME, "2");
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      List<Runnable> startAllDataNodes = new ArrayList<>();
      for (int i = 0; i < dataNodeNum; i++) {
        DataNodeWrapper wrapper = EnvFactory.getEnv().generateRandomDataNodeWrapper();
        startAllDataNodes.add(wrapper::start);
      }

      leaderClient.setLicenseFile(LICENSE_FILE_NAME, nodeNumLimit2License);
      testStatusAllActiveActivate(leaderClient, 1, 0);
      runInParallel(startAllDataNodes);
      testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED, ACTIVATED));
      testStatusFalse(
          leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED, ACTIVATED, ACTIVATED));
    }

    allCheckPass();
  }

  @Test
  @Order(normalTest)
  public void dataNodeCpuNumLimitSequentialTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
    String cpuCoreLimit1 = buildLicenseFromBase(Lottery.DATANODE_CPU_CORE_NUM_LIMIT_NAME, "1");
    String cpuCoreLimit2 = buildLicenseFromBase(Lottery.DATANODE_CPU_CORE_NUM_LIMIT_NAME, "2");
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      List<DataNodeWrapper> dataNodeWrappers = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        DataNodeWrapper wrapper = EnvFactory.getEnv().generateRandomDataNodeWrapper();
        dataNodeWrappers.add(wrapper);
      }

      // 1. first datanode start success
      dataNodeWrappers.get(0).start();
      testStatusWithRetry(leaderClient, Arrays.asList(PASSIVE_UNACTIVATED, UNACTIVATED));

      // An unactivated cluster must still allow an already registered DataNode to restart. This
      // is required to make the CLI available for installing or replacing the license.
      dataNodeWrappers.get(0).stopForcibly();
      dataNodeWrappers.get(0).start();
      testStatusWithRetry(leaderClient, Arrays.asList(PASSIVE_UNACTIVATED, UNACTIVATED));

      // 2. set license, then second datanode failed to start
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, cpuCoreLimit1);
      testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED));
      dataNodeWrappers.get(1).start();
      testStatusFalse(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED, ACTIVATED));

      // 3. modify license, set nodeNumLimit to 2, then second datanode start success
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, cpuCoreLimit2);
      waitLicenseReload();
      dataNodeWrappers.get(1).start();
      testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, ACTIVATED, ACTIVATED));
    }

    allCheckPass();
  }

  // endregion

  // region AINode Tests

  // endregion

  // region CLI Activation Operation Test

  @Test
  public void cliOperationForTreeModelTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 2);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      showSystemInfoTest(statement);
      showActivationTest(statement);
      cliActivateFailBecauseWrongNumber(statement);
      cliActivateFailBecauseNotBigEnough(statement);
      cliActivateFailBecauseDifferentContent(statement);
      cliActivateSuccess(statement);
    }
  }

  @Test
  public void cliOperationForTableModelTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 2);
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      showSystemInfoTest(statement);
      showActivationTest(statement);
      cliActivateFailBecauseWrongNumber(statement);
      cliActivateFailBecauseNotBigEnough(statement);
      cliActivateFailBecauseDifferentContent(statement);
      cliActivateSuccess(statement);
    }
  }

  @Test
  public void cliActivateSingleV02LicenseFor3C3DTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String systemInfos = showSystemInfoTest(statement);
      String sql = "activate '" + buildV02ActivationCode(systemInfos, 3, false) + "'";
      try (Statement writeStatement = createStatementOnWriteConnection(connection);
          ResultSet ignored = writeStatement.executeQuery(sql)) {
        // The result set is checked after node activation statuses converge.
      }
      try (SyncConfigNodeIServiceClient leaderClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        testStatusAllActiveActivate(leaderClient, 3, 3);
      }
      final List<ObligationStatus> expandedClusterExpectation =
          Arrays.asList(
              ACTIVE_ACTIVATED,
              ACTIVE_ACTIVATED,
              ACTIVE_ACTIVATED,
              PASSIVE_ACTIVATED,
              PASSIVE_ACTIVATED,
              ACTIVATED,
              ACTIVATED,
              ACTIVATED);
      final List<Integer> expandedConfigNodeIds;
      try (SyncConfigNodeIServiceClient leaderClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        Map<Integer, String> beforeExpansionStatus =
            getActivationStatusMapWithRetry(leaderClient, 6);
        EnvFactory.getEnv().registerNewConfigNode(true);
        EnvFactory.getEnv().registerNewConfigNode(true);
        testStatusWithRetry(leaderClient, expandedClusterExpectation);
        Map<Integer, String> afterExpansionStatus =
            getActivationStatusMapWithRetry(leaderClient, 8);
        expandedConfigNodeIds =
            afterExpansionStatus.keySet().stream()
                .filter(nodeId -> !beforeExpansionStatus.containsKey(nodeId))
                .collect(Collectors.toList());
        Collections.sort(expandedConfigNodeIds);
        Assert.assertEquals(2, expandedConfigNodeIds.size());
        testNodeStatusWithRetry(leaderClient, expandedConfigNodeIds, PASSIVE_ACTIVATED);
      }
      ImmutableMap<String, Pair<String, String>> expectation =
          ImmutableMap.of(
              "Status",
              new Pair<>(SKIP, "-"),
              "ExpiredTime",
              new Pair<>("-", SKIP),
              "DataNodeLimit",
              new Pair<>("3", "3"),
              "AiNodeLimit",
              new Pair<>("0", "0"),
              "CpuLimit",
              new Pair<>(SKIP, "Unlimited"),
              "DeviceLimit",
              new Pair<>(SKIP, "Unlimited"),
              "TimeSeriesLimit",
              new Pair<>(SKIP, "Unlimited"));
      try (Statement writeStatement = createStatementOnWriteConnection(connection);
          ResultSet resultSet = writeStatement.executeQuery("show activation")) {
        checkShowActivationResult(resultSet, expectation);
      }
      String expandedSystemInfos = showSystemInfoTest(statement, 5);
      String expandedSql =
          "activate '" + buildV02ActivationCode(expandedSystemInfos, 3, true) + "'";
      try (Statement writeStatement = createStatementOnWriteConnection(connection);
          ResultSet ignored = writeStatement.executeQuery(expandedSql)) {
        // The result set is checked after node activation statuses converge.
      }
      try (SyncConfigNodeIServiceClient leaderClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        testStatusAllActiveActivate(leaderClient, 5, 3);
      }
      restartAllConfigNodes();
      try (SyncConfigNodeIServiceClient leaderClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        testStatusAllActiveActivate(leaderClient, 5, 3);
      }
    }
  }

  private Statement createStatementOnWriteConnection(Connection connection) throws SQLException {
    if (connection instanceof ClusterTestConnection) {
      return ((ClusterTestConnection) connection)
          .writeConnection
          .getUnderlyingConnection()
          .createStatement();
    }
    return connection.createStatement();
  }

  private String showSystemInfoTest(Statement statement) throws Exception {
    return showSystemInfoTest(statement, 3);
  }

  private String showSystemInfoTest(Statement statement, int expectedConfigNodeCount)
      throws Exception {
    ResultSet resultSet = statement.executeQuery("show system info");
    resultSet.next();
    String systemInfos = resultSet.getString("SystemInfo");
    Assert.assertEquals(expectedConfigNodeCount, systemInfos.split(",").length);
    return systemInfos;
  }

  private String buildV02ActivationCode(
      String systemInfos, int dataNodeLimit, boolean skipHardwareSystemInfoCheck) throws Exception {
    List<String> systemInfoList = Arrays.asList(systemInfos.split(","));
    byte identifier = (byte) ((1 << 0) | (1 << 3) | (skipHardwareSystemInfoCheck ? (1 << 4) : 0));
    ByteBuffer dataBuffer = ByteBuffer.allocate(skipHardwareSystemInfoCheck ? 8 : 7);
    dataBuffer.put(identifier);
    dataBuffer.putInt(20300101);
    dataBuffer.put((byte) dataNodeLimit);
    dataBuffer.put((byte) 0);
    if (skipHardwareSystemInfoCheck) {
      dataBuffer.put((byte) 1);
    }
    byte[] data = dataBuffer.array();
    String salt =
        systemInfoList.stream()
            .map(
                systemInfo ->
                    skipHardwareSystemInfoCheck ? clearHardwareSystemInfo(systemInfo) : systemInfo)
            .collect(Collectors.joining(","));
    byte[] tag = HmacProtocol.calculateTag(data, salt);
    ByteBuffer payloadBuffer = ByteBuffer.allocate(data.length + tag.length);
    payloadBuffer.put(data);
    payloadBuffer.put(tag);
    String encodedPayload = new Base32().encodeAsString(payloadBuffer.array());
    assertV02ActivationCode(
        encodedPayload, systemInfoList, dataNodeLimit, skipHardwareSystemInfoCheck);
    return "02-" + encodedPayload;
  }

  private void assertV02ActivationCode(
      String encodedPayload,
      List<String> systemInfoList,
      int dataNodeLimit,
      boolean skipHardwareSystemInfoCheck)
      throws Exception {
    Properties decodedProperties = new Properties();
    decodedProperties.load(
        new StringReader(Bandit.publicDecryptV02(encodedPayload, systemInfoList)));
    Assert.assertNotNull(decodedProperties.getProperty(LICENSE_EXPIRE_TIMESTAMP_NAME));
    Assert.assertEquals(
        String.valueOf(skipHardwareSystemInfoCheck),
        decodedProperties.getProperty(
            Lottery.SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME, Boolean.FALSE.toString()));
    Assert.assertEquals(
        String.valueOf(dataNodeLimit),
        decodedProperties.getProperty(Lottery.DATANODE_NUM_LIMIT_NAME));
    Assert.assertEquals("0", decodedProperties.getProperty(Lottery.AINODE_NUM_LIMIT_NAME));
    Assert.assertEquals(
        systemInfoList.stream()
            .map(
                systemInfo ->
                    skipHardwareSystemInfoCheck ? clearHardwareSystemInfo(systemInfo) : systemInfo)
            .collect(Collectors.joining(",")),
        decodedProperties.getProperty(Lottery.SYSTEM_INFO_HASH));
    Lottery lottery = new Lottery(() -> {});
    Assert.assertTrue(lottery.loadFromProperties(decodedProperties, false));
    Assert.assertEquals(dataNodeLimit, lottery.getDataNodeNumLimit());
    Assert.assertEquals(Integer.MAX_VALUE, lottery.getDataNodeCpuCoreNumLimit());
  }

  private String clearHardwareSystemInfo(String systemInfo) {
    int hardwareStart = systemInfo.indexOf('-') + 1;
    int hardwareEnd = systemInfo.indexOf('-', hardwareStart);
    if (hardwareStart <= 0 || hardwareEnd < 0) {
      return systemInfo;
    }
    return systemInfo.substring(0, hardwareStart) + "00000000" + systemInfo.substring(hardwareEnd);
  }

  private void showActivationTest(Statement statement) throws Exception {
    ResultSet resultSet = statement.executeQuery("show activation");
    ImmutableMap<String, Pair<String, String>> expectation =
        ImmutableMap.of(
            "Status",
            new Pair<>(UNACTIVATED.toString(), "-"),
            "ExpiredTime",
            new Pair<>("-", SKIP),
            "DataNodeLimit",
            new Pair<>("2", "0"),
            "AiNodeLimit",
            new Pair<>("0", "0"),
            "CpuLimit",
            new Pair<>(SKIP, "0"),
            "DeviceLimit",
            new Pair<>("0", "0"),
            "TimeSeriesLimit",
            new Pair<>("0", "0"));
    checkShowActivationResult(resultSet, expectation);
  }

  private void cliActivateFailBecauseWrongNumber(Statement statement) throws Exception {
    String sql =
        "activate "
            + "'"
            + "DN1=2\nL1=1711900800000\nL2=18145792000000,\n"
            + "DN1=3\nL1=1711900800000\nL2=18145792000000"
            + "'";
    Assert.assertThrows(SQLException.class, () -> statement.executeQuery(sql));
  }

  private void cliActivateFailBecauseDifferentContent(Statement statement) throws Exception {
    String sql =
        "activate "
            + "'"
            + "DN1=2\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000,\n"
            + "DN1=3\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000,\n"
            + "DN1=3\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000\n"
            + "'";
    Assert.assertThrows(SQLException.class, () -> statement.executeQuery(sql));
  }

  private void cliActivateFailBecauseNotBigEnough(Statement statement) throws Exception {
    String sql =
        "activate "
            + "'"
            + "DN1=1\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000,\n"
            + "DN1=1\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000,\n"
            + "DN1=1\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000\n"
            + "'";
    Assert.assertThrows(SQLException.class, () -> statement.executeQuery(sql));
  }

  private void cliActivateSuccess(Statement statement) throws Exception {
    String sql =
        "activate "
            + "'"
            + "DN1=2\n"
            + "ML1=1\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000,\n"
            + "DN1=2\n"
            + "ML1=1\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000,\n"
            + "DN1=2\n"
            + "ML1=1\n"
            + "L1=1711900800000\n"
            + "L2=18145792000000\n"
            + "'";
    ResultSet resultSet = statement.executeQuery(sql);
    ImmutableMap<String, Pair<String, String>> expectation =
        ImmutableMap.of(
            "Status",
            new Pair<>(ACTIVATED.toString(), "-"),
            "ExpiredTime",
            new Pair<>("-", SKIP),
            "DataNodeLimit",
            new Pair<>("2", "2"),
            "AiNodeLimit",
            new Pair<>("0", "1"),
            "CpuLimit",
            new Pair<>(SKIP, "Unlimited"),
            "DeviceLimit",
            new Pair<>("0", "Unlimited"),
            "TimeSeriesLimit",
            new Pair<>("0", "Unlimited"));
    checkShowActivationResult(resultSet, expectation);
  }

  private void checkShowActivationResult(
      ResultSet resultSet, ImmutableMap<String, Pair<String, String>> expectation)
      throws SQLException {
    int loopCount = 0;
    while (resultSet.next()) {
      loopCount++;
      logger.info("check {}", resultSet.getString(1));
      Pair<String, String> expectedUsageAndLimit = expectation.get(resultSet.getString(1));
      if (!SKIP.equals(expectedUsageAndLimit.getLeft())) {
        Assert.assertEquals(
            expectedUsageAndLimit.getLeft(), resultSet.getString(ColumnHeaderConstant.USAGE));
      }
      if (!SKIP.equals(expectedUsageAndLimit.getRight())) {
        Assert.assertEquals(
            expectedUsageAndLimit.getRight(), resultSet.getString(ColumnHeaderConstant.LIMIT));
      }
    }
    Assert.assertEquals("some expectations not checked", expectation.size(), loopCount);
  }

  // endregion

  // region Other Tests

  @Test
  @Order(baseTest)
  public void showClusterTest() throws SQLException, InterruptedException {
    ClusterConstant.MAIN_CONFIGNODE_CLASS_NAME_FOR_IT =
        ClusterConstant.MAIN_CONFIGNODE_CLASS_NAME_FOR_OTHER_IT;
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean pass = false;
      for (int retry = 0; retry < 5; retry++) {
        try {
          ResultSet resultSet = statement.executeQuery("show cluster");
          while (resultSet.next()) {
            // must contain "ActivateStatus" column, and this column must contain valid value
            ObligationStatus.valueOf(resultSet.getString(ColumnHeaderConstant.ACTIVATE_STATUS));
          }
          resultSet = statement.executeQuery("show cluster details");
          while (resultSet.next()) {
            // must contain "ActivateStatus" column, and this column must contain valid value
            ObligationStatus.valueOf(resultSet.getString(ColumnHeaderConstant.ACTIVATE_STATUS));
          }
          pass = true;
          break;
        } catch (Exception ignore) {
          TimeUnit.SECONDS.sleep(1);
        }
      }
      Assert.assertTrue(pass);
    }

    allCheckPass();
  }

  @Test
  public void unactivatedTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // new ConfigNode can join cluster
      EnvFactory.getEnv().registerNewConfigNode(true);
      testPassiveUnactivated(leaderClient, 2, 0);

      leaderClient.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
      testStatusWithRetry(leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED));
      EnvFactory.getEnv().registerNewDataNode(true);
      testStatusWithRetry(
          leaderClient, Arrays.asList(ACTIVE_ACTIVATED, PASSIVE_ACTIVATED, ACTIVATED));
      leaderClient.deleteLicenseFile(LICENSE_FILE_NAME);

      Connection connection = EnvFactory.getEnv().getConnection();

      // old DataNode can restart
      testPassiveUnactivated(leaderClient, 2, 1);
      EnvFactory.getEnv().getDataNodeWrapper(0).stopForcibly();
      EnvFactory.getEnv().getDataNodeWrapper(0).start();
      testPassiveUnactivated(leaderClient, 2, 1);

      // new DataNode can join
      EnvFactory.getEnv().registerNewDataNode(false);
      testStatusWithRetry(
          leaderClient,
          Arrays.asList(PASSIVE_UNACTIVATED, PASSIVE_UNACTIVATED, UNACTIVATED, UNACTIVATED));

      // not allow manually set system status to Running
      Statement statement = connection.createStatement();
      boolean pass = false;
      try {
        statement.executeQuery("set system to running");
      } catch (SQLException e) {
        pass = true;
      }
      Assert.assertTrue(pass);
    }

    allCheckPass();
  }

  @Test
  public void timeHackTest() throws Exception {
    final String specialLicenseContent =
        buildLicenseFromBase(LICENSE_ISSUE_TIMESTAMP_NAME, "2701243842000");
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, specialLicenseContent);
      testStatusFalse(leaderClient, Collections.singletonList(ACTIVE_ACTIVATED));
    }
  }

  @Test
  public void getLicenseInfoTest()
      throws IoTDBConnectionException,
          StatementExecutionException,
          ClientManagerException,
          IOException,
          InterruptedException,
          TException,
          LicenseException {
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
    try (SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      leaderClient.setLicenseFile(LICENSE_FILE_NAME, baseLicenseContent);
      waitLicenseReload();
      EnvFactory.getEnv().registerNewDataNode(true);
      Session session = (Session) EnvFactory.getEnv().getSessionConnection();
      LicenseInfoResp resp = session.getLicenseInfo();
      Lottery lottery = new Lottery(() -> {});
      RegulateManager.tryLoadLicenseFromString(lottery, baseLicenseContent);
      Assert.assertEquals(lottery.toTLicense().toString(), resp.license.toString());
      allCheckPass();
    }
  }

  // endregion

  // region Helpers
  private static void waitLicenseReload() {
    saferSleep(RegulateManager.FILE_MONITOR_INTERVAL * 2);
  }

  private void restartAllConfigNodes() {
    for (ConfigNodeWrapper wrapper : EnvFactory.getEnv().getConfigNodeWrapperList()) {
      wrapper.stopForcibly();
    }
    for (ConfigNodeWrapper wrapper : EnvFactory.getEnv().getConfigNodeWrapperList()) {
      wrapper.start();
    }
  }

  private <K, V> String mapToString(Map<K, V> map) {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    for (Map.Entry<K, V> entry : map.entrySet()) {
      builder.append(entry.getKey()).append(":").append(entry.getValue().toString()).append(", ");
    }
    builder.append("}");
    return builder.toString();
  }

  private String listToString(List<ObligationStatus> list) {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    for (ObligationStatus status : list) {
      builder.append(status).append(", ");
    }
    builder.append("}");
    return builder.toString();
  }

  /**
   * When there is no active node in cluster, still need to wait a moment before goes into
   * unactivated state
   */
  private void testGoIntoUnactivated(
      SyncConfigNodeIServiceClient leaderClient, int configNodeNum, int dataNodeNum)
      throws Exception {
    saferSleep(10000);
    testPassiveUnactivated(leaderClient, configNodeNum, dataNodeNum);
  }

  private void testStatusWithRetry(
      SyncConfigNodeIServiceClient leaderClient, List<ObligationStatus> expectation)
      throws Exception {
    testStatusWithRetry(leaderClient, expectation, 60000);
  }

  private void testAllNodesActivatedWithRetry(
      SyncConfigNodeIServiceClient client, int expectedNodeCount) throws Exception {
    long startTime = System.currentTimeMillis();
    while (true) {
      TGetAllActivationStatusResp resp = null;
      try {
        resp = client.getAllActivationStatus();
      } catch (TException ignored) {

      }
      if (resp != null
          && TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.status.getCode()
          && resp.getActivationStatusMap().size() == expectedNodeCount
          && resp.getActivationStatusMap().values().stream()
              .map(ObligationStatus::valueOf)
              .allMatch(ObligationStatus::isActivated)) {
        logger.info("check pass");
        return;
      }
      if (System.currentTimeMillis() - startTime > 60000) {
        StringBuilder errBuilder = new StringBuilder();
        errBuilder
            .append("Test fail because all nodes are not activated after 60 seconds retry.\n")
            .append("Expected node count is ")
            .append(expectedNodeCount)
            .append("\n");
        if (resp != null) {
          errBuilder.append("Actual state is ").append(mapToString(resp.getActivationStatusMap()));
        } else {
          errBuilder.append("Actual state is unknown, because last resp is null");
        }
        String errMsg = errBuilder.toString();
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
      saferSleep(1000);
    }
  }

  private Map<Integer, String> getActivationStatusMapWithRetry(
      SyncConfigNodeIServiceClient client, int expectedNodeCount) throws Exception {
    long startTime = System.currentTimeMillis();
    while (true) {
      TGetAllActivationStatusResp resp = null;
      try {
        resp = client.getAllActivationStatus();
      } catch (TException ignored) {

      }
      if (resp != null
          && TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.status.getCode()
          && resp.getActivationStatusMap().size() == expectedNodeCount) {
        return new HashMap<>(resp.getActivationStatusMap());
      }
      if (System.currentTimeMillis() - startTime > 60000) {
        StringBuilder errBuilder = new StringBuilder();
        errBuilder
            .append("Test fail because activation status map size is not ")
            .append(expectedNodeCount)
            .append(" after 60 seconds retry.\n");
        if (resp != null) {
          errBuilder.append("Actual state is ").append(mapToString(resp.getActivationStatusMap()));
        } else {
          errBuilder.append("Actual state is unknown, because last resp is null");
        }
        String errMsg = errBuilder.toString();
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
      saferSleep(1000);
    }
  }

  private void testNodeStatusWithRetry(
      SyncConfigNodeIServiceClient client, List<Integer> nodeIds, ObligationStatus expectedStatus)
      throws Exception {
    long startTime = System.currentTimeMillis();
    while (true) {
      TGetAllActivationStatusResp resp = null;
      try {
        resp = client.getAllActivationStatus();
      } catch (TException ignored) {

      }
      if (resp != null && TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.status.getCode()) {
        Map<Integer, String> activationStatusMap = resp.getActivationStatusMap();
        if (nodeIds.stream()
            .allMatch(
                nodeId -> expectedStatus.toString().equals(activationStatusMap.get(nodeId)))) {
          logger.info("check pass");
          return;
        }
      }
      if (System.currentTimeMillis() - startTime > 60000) {
        StringBuilder errBuilder = new StringBuilder();
        errBuilder
            .append("Test fail because nodes ")
            .append(nodeIds)
            .append(" are not ")
            .append(expectedStatus)
            .append(" after 60 seconds retry.\n");
        if (resp != null) {
          errBuilder.append("Actual state is ").append(mapToString(resp.getActivationStatusMap()));
        } else {
          errBuilder.append("Actual state is unknown, because last resp is null");
        }
        String errMsg = errBuilder.toString();
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
      saferSleep(1000);
    }
  }

  private void testStatusFalse(
      SyncConfigNodeIServiceClient leaderClient, List<ObligationStatus> expectation)
      throws Exception {
    testStatusFalseWithFailMessage(leaderClient, expectation, "");
  }

  private void testStatusFalseWithFailMessage(
      SyncConfigNodeIServiceClient leaderClient,
      List<ObligationStatus> expectation,
      String failMessage)
      throws Exception {
    long startTime = System.currentTimeMillis();
    while (true) {
      TGetAllActivationStatusResp resp = null;
      try {
        resp = leaderClient.getAllActivationStatus();
      } catch (TException ignored) {

      }
      if (resp != null
          && TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.status.getCode()
          && compareLists(
              expectation,
              resp.getActivationStatusMap().values().stream()
                  .map(ObligationStatus::valueOf)
                  .collect(Collectors.toList()))) {
        String errMsg =
            "Test fail because cluster goes into the undesirable state: "
                + mapToString(resp.getActivationStatusMap())
                + ".\n"
                + failMessage;
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
      if (System.currentTimeMillis() - startTime > 5000) {
        logger.info("check pass");
        return;
      }
      saferSleep(1000);
    }
  }

  private void testStatusWithRetry(
      SyncConfigNodeIServiceClient client, List<ObligationStatus> expectation, long timeLimit)
      throws Exception {
    logger.info("expect {}", listToString(expectation));
    long startTime = System.currentTimeMillis();
    while (true) {
      TGetAllActivationStatusResp resp = null;
      try {
        resp = client.getAllActivationStatus();
      } catch (TException ignored) {

      }
      if (resp != null
          && TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.status.getCode()
          && compareLists(
              expectation,
              resp.getActivationStatusMap().values().stream()
                  .map(ObligationStatus::valueOf)
                  .collect(Collectors.toList()))) {
        logger.info("check pass");
        break;
      }
      if (System.currentTimeMillis() - startTime > timeLimit) {
        StringBuilder errBuilder = new StringBuilder();
        errBuilder
            .append("Test fail because cluster not goes into desirable state after ")
            .append(timeLimit / 1000)
            .append(" seconds retry.\n")
            .append("Desirable state is ")
            .append(listToString(expectation))
            .append("\n");

        if (resp != null) {
          errBuilder.append("Actual state is ").append(mapToString(resp.getActivationStatusMap()));
        } else {
          errBuilder.append("Actual state is unknown, because last resp is null");
        }
        String errMsg = errBuilder.toString();
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
      saferSleep(1000);
    }
  }

  private void testPassiveActivated(
      SyncConfigNodeIServiceClient leaderClient, int configNodeNum, int dataNodeNum)
      throws Exception {
    List<ObligationStatus> statusList = new ArrayList<>();
    for (int i = 0; i < configNodeNum; i++) {
      statusList.add(PASSIVE_ACTIVATED);
    }
    for (int i = 0; i < dataNodeNum; i++) {
      statusList.add(ACTIVATED);
    }
    testStatusWithRetry(leaderClient, statusList);
  }

  private void testStatusAllActiveActivate(
      SyncConfigNodeIServiceClient leaderClient, int configNodeNum, int dataNodeNum)
      throws Exception {
    List<ObligationStatus> statusList = new ArrayList<>();
    for (int i = 0; i < configNodeNum; i++) {
      statusList.add(ACTIVE_ACTIVATED);
    }
    for (int i = 0; i < dataNodeNum; i++) {
      statusList.add(ACTIVATED);
    }
    testStatusWithRetry(leaderClient, statusList);
  }

  private void testPassiveUnactivated(
      SyncConfigNodeIServiceClient leaderClient, int configNodeNum, int dataNodeNum)
      throws Exception {
    List<ObligationStatus> statusList = new ArrayList<>();
    for (int i = 0; i < configNodeNum; i++) {
      statusList.add(PASSIVE_UNACTIVATED);
    }
    for (int i = 0; i < dataNodeNum; i++) {
      statusList.add(UNACTIVATED);
    }
    testStatusWithRetry(leaderClient, statusList);
  }

  public static <T> boolean compareLists(List<T> list1, List<T> list2) {
    if (list1.size() != list2.size()) {
      return false;
    }
    Map<T, Integer> countMap = new HashMap<>();
    for (T element : list1) {
      countMap.put(element, countMap.getOrDefault(element, 0) + 1);
    }
    for (T element : list2) {
      Integer count = countMap.get(element);
      if (count == null || count <= 0) {
        return false;
      }
      countMap.put(element, count - 1);
    }
    return true;
  }

  private static String propertiesToString(Properties properties) throws IOException {
    StringWriter writer = new StringWriter();
    properties.store(writer, "");
    return writer.toString();
  }

  private static String buildLicenseFromBase(String... contents) throws IOException {
    if (contents.length % 2 != 0) {
      throw new RuntimeException("contents.length % 2 needs to be 0!");
    }
    Properties newProperties = new Properties();
    newProperties.load(new StringReader(baseLicenseContent));
    for (int i = 0; i < contents.length; i += 2) {
      newProperties.setProperty(contents[i], contents[i + 1]);
    }
    return propertiesToString(newProperties);
  }

  private static void setLicense(SyncConfigNodeIServiceClient client, String licenseFileName)
      throws IOException, TException {
    Path licensePath = Paths.get(".license/" + licenseFileName);
    String licenseContent = new String(Files.readAllBytes(licensePath));
    client.setLicenseFile(LICENSE_FILE_NAME, licenseContent);
  }

  private static void runInParallel(List<Runnable> runnableList) {
    List<CompletableFuture<?>> tasks = new ArrayList<>();
    for (Runnable runnable : runnableList) {
      tasks.add(CompletableFuture.runAsync(runnable));
    }
    tasks.forEach(CompletableFuture::join);
  }

  private static <T> void runConsumerInParallel(List<Consumer<T>> consumerList, T parameter) {
    List<Runnable> runnableList = new ArrayList<>();
    for (Consumer<T> consumer : consumerList) {
      Runnable runnable = () -> consumer.accept(parameter);
      runnableList.add(runnable);
    }
    runInParallel(runnableList);
  }

  /**
   * Fisher-Yates Shuffle algorithm, randomly get n stuff from m stuff in an efficient way. For
   * example, getNFromM(5, 2) will return {1,3} {3,4} {2,0} in same opportunity.
   */
  private static HashSet<Integer> getNFromM(int m, int n) {
    int[] numbers = new int[m];
    for (int i = 0; i < m; i++) {
      numbers[i] = i;
    }
    Random random = new Random();
    for (int i = 0; i < n; i++) {
      int j = i + random.nextInt(m - i);
      int temp = numbers[j];
      numbers[j] = numbers[i];
      numbers[i] = temp;
    }
    HashSet<Integer> result = new HashSet<>();
    for (int i = 0; i < n; i++) {
      result.add(numbers[i]);
    }
    return result;
  }

  private static int getAnyFollowerIndex() throws IOException, InterruptedException {
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    return IntStream.range(0, EnvFactory.getEnv().getConfigNodeWrapperList().size())
        .filter(x -> x != leaderIndex)
        .findAny()
        .getAsInt();
  }

  private static void allCheckPass() {
    logger.info("all check pass");
  }

  private static void saferSleep(final long sleepMs) {
    final boolean needToLog = sleepMs > sleepLogThreshold;
    if (needToLog) {
      logger.info("Sleeping for {} ms...", sleepMs);
    }
    long sleepRemain = sleepMs;
    while (sleepRemain > 0) {
      long whenLastSleepStarted = System.currentTimeMillis();
      try {
        Thread.sleep(sleepRemain);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        long interval = System.currentTimeMillis() - whenLastSleepStarted;
        sleepRemain -= interval;
        if (needToLog) {
          logger.warn(
              "Sleeping was interrupted at {} ms, will sleep again for {} ms...",
              interval,
              sleepRemain);
        }
        continue;
      }
      break;
    }
    if (needToLog) {
      logger.info("Slept for {} ms.", sleepMs);
    }
  }

  // endregion
}
