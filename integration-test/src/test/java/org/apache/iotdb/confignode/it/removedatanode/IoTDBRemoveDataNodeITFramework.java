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

package org.apache.iotdb.confignode.it.removedatanode;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.exception.InconsistentDataException;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMap;
import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

public class IoTDBRemoveDataNodeITFramework {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRemoveDataNodeITFramework.class);
  private static final String TREE_MODEL_INSERTION =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 1, 2)";

  private static final String SHOW_REGIONS = "show regions";
  private static final String SHOW_DATANODES = "show datanodes";

  private static final String defaultSchemaRegionGroupExtensionPolicy = "CUSTOM";
  private static final String defaultDataRegionGroupExtensionPolicy = "CUSTOM";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionGroupExtensionPolicy(defaultSchemaRegionGroupExtensionPolicy)
        .setDataRegionGroupExtensionPolicy(defaultDataRegionGroupExtensionPolicy);
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
      final int removeDataNodeNum,
      final int dataRegionPerDataNode,
      final boolean rejoinRemovedDataNode,
      final SQLModel model)
      throws Exception {
    testRemoveDataNode(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        removeDataNodeNum,
        dataRegionPerDataNode,
        true,
        rejoinRemovedDataNode,
        model);
  }

  public void failTest(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      final int removeDataNodeNum,
      final int dataRegionPerDataNode,
      final boolean rejoinRemovedDataNode,
      final SQLModel model)
      throws Exception {
    testRemoveDataNode(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        removeDataNodeNum,
        dataRegionPerDataNode,
        false,
        rejoinRemovedDataNode,
        model);
  }

  public void testRemoveDataNode(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      final int removeDataNodeNum,
      final int dataRegionPerDataNode,
      final boolean expectRemoveSuccess,
      final boolean rejoinRemovedDataNode,
      final SQLModel model)
      throws Exception {
    // Set up the environment
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaReplicationFactor(schemaReplicationFactor)
        .setDataReplicationFactor(dataReplicateFactor)
        .setDefaultDataRegionGroupNumPerDatabase(
            dataRegionPerDataNode * dataNodeNum / dataReplicateFactor);
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, dataNodeNum);

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Insert data in tree model
      statement.execute(TREE_MODEL_INSERTION);

      Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
      regionMap.forEach(
          (key, valueSet) -> {
            LOGGER.info("Key: {}, Value: {}", key, valueSet);
            if (valueSet.size() != dataReplicateFactor) {
              Assert.fail();
            }
          });

      // Get all data nodes
      ResultSet result = statement.executeQuery(SHOW_DATANODES);
      Set<Integer> allDataNodeId = new HashSet<>();
      while (result.next()) {
        allDataNodeId.add(result.getInt(ColumnHeaderConstant.NODE_ID));
      }

      // Select data nodes to remove
      final Set<Integer> removeDataNodes =
          selectRemoveDataNodes(allDataNodeId, regionMap, removeDataNodeNum);

      List<DataNodeWrapper> removeDataNodeWrappers =
          removeDataNodes.stream()
              .map(dataNodeId -> EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).get())
              .collect(Collectors.toList());

      AtomicReference<SyncConfigNodeIServiceClient> clientRef = new AtomicReference<>(client);
      List<TDataNodeLocation> removeDataNodeLocations =
          clientRef
              .get()
              .getDataNodeConfiguration(-1)
              .getDataNodeConfigurationMap()
              .values()
              .stream()
              .map(TDataNodeConfiguration::getLocation)
              .filter(location -> removeDataNodes.contains(location.getDataNodeId()))
              .collect(Collectors.toList());
      if (SQLModel.NOT_USE_SQL.equals(model)) {
        TDataNodeRemoveReq removeReq = new TDataNodeRemoveReq(removeDataNodeLocations);

        // Remove data nodes
        TDataNodeRemoveResp removeResp = clientRef.get().removeDataNode(removeReq);
        LOGGER.info("Submit Remove DataNodes result {} ", removeResp);
        if (removeResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          if (expectRemoveSuccess) {
            LOGGER.error("Submit Remove DataNodes fail");
            Assert.fail();
          } else {
            LOGGER.info("Submit Remove DataNodes fail, as expected.");
            return;
          }
        }
        LOGGER.info("Submit Remove DataNodes request: {}", removeReq);

      } else {
        String removeDataNodeSQL = generateRemoveString(removeDataNodes);
        LOGGER.info("Remove DataNodes SQL: {}", removeDataNodeSQL);
        try {
          statement.execute(removeDataNodeSQL);
        } catch (IoTDBSQLException e) {
          if (expectRemoveSuccess) {
            LOGGER.error("Remove DataNodes SQL execute fail: {}", e.getMessage());
            Assert.fail();
          } else {
            LOGGER.info("Submit Remove DataNodes fail, as expected");
            return;
          }
        }
        LOGGER.info("Remove DataNodes SQL submit successfully.");
      }

      // Wait until success
      boolean removeSuccess = false;
      try {
        awaitUntilSuccess(clientRef, removeDataNodeLocations);
        removeSuccess = true;
      } catch (ConditionTimeoutException e) {
        if (expectRemoveSuccess) {
          LOGGER.error("Remove DataNodes timeout in 2 minutes");
          Assert.fail();
        }
      }

      if (!expectRemoveSuccess && removeSuccess) {
        LOGGER.error("Remove DataNodes success, but expect fail");
        Assert.fail();
      }

      LOGGER.info("Remove DataNodes success");

      if (rejoinRemovedDataNode) {
        try {
          // Use sleep and restart to ensure that removeDataNodes restarts successfully
          Thread.sleep(30000);
          restartDataNodes(removeDataNodeWrappers);
          LOGGER.info("RemoveDataNodes:{} rejoined successfully.", removeDataNodes);
        } catch (Exception e) {
          LOGGER.error("RemoveDataNodes rejoin failed.");
          Assert.fail();
        }
      }
    } catch (InconsistentDataException e) {
      LOGGER.error("Unexpected error:", e);
    }

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement())) {

      // Check the data region distribution after removing data nodes
      Map<Integer, Set<Integer>> afterRegionMap = getDataRegionMap(statement);
      afterRegionMap.forEach(
          (key, valueSet) -> {
            LOGGER.info("Key: {}, Value: {}", key, valueSet);
            if (valueSet.size() != dataReplicateFactor) {
              Assert.fail();
            }
          });

      if (rejoinRemovedDataNode) {
        ResultSet result = statement.executeQuery(SHOW_DATANODES);
        Set<Integer> allDataNodeId = new HashSet<>();
        while (result.next()) {
          allDataNodeId.add(result.getInt(ColumnHeaderConstant.NODE_ID));
        }
        Assert.assertEquals(allDataNodeId.size(), dataNodeNum);
      }
    } catch (InconsistentDataException e) {
      LOGGER.error("Unexpected error:", e);
    }
  }

  private static Set<Integer> selectRemoveDataNodes(
      Set<Integer> allDataNodeId, Map<Integer, Set<Integer>> regionMap, int removeDataNodeNum) {
    Set<Integer> removeDataNodeIds = new HashSet<>();
    for (int i = 0; i < removeDataNodeNum; i++) {
      int removeDataNodeId = allDataNodeId.iterator().next();
      removeDataNodeIds.add(removeDataNodeId);
      allDataNodeId.remove(removeDataNodeId);
    }
    return removeDataNodeIds;
  }

  private static void awaitUntilSuccess(
      AtomicReference<SyncConfigNodeIServiceClient> clientRef,
      List<TDataNodeLocation> removeDataNodeLocations) {
    AtomicReference<List<TDataNodeLocation>> lastTimeDataNodeLocations = new AtomicReference<>();
    AtomicReference<Exception> lastException = new AtomicReference<>();

    try {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(2, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  List<TDataNodeLocation> remainingDataNodes =
                      clientRef
                          .get()
                          .getDataNodeConfiguration(-1)
                          .getDataNodeConfigurationMap()
                          .values()
                          .stream()
                          .map(TDataNodeConfiguration::getLocation)
                          .collect(Collectors.toList());
                  lastTimeDataNodeLocations.set(remainingDataNodes);
                  for (TDataNodeLocation location : removeDataNodeLocations) {
                    if (remainingDataNodes.contains(location)) {
                      return false;
                    }
                  }
                  return true;
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
      if (lastTimeDataNodeLocations.get() == null) {
        LOGGER.error(
            "Maybe getDataNodeConfiguration fail, lastTimeDataNodeLocations is null, last Exception:",
            lastException.get());
        throw e;
      }
      String actualSetStr = lastTimeDataNodeLocations.get().toString();
      lastTimeDataNodeLocations.get().removeAll(removeDataNodeLocations);
      String expectedSetStr = lastTimeDataNodeLocations.get().toString();
      LOGGER.error(
          "Remove DataNodes timeout in 2 minutes, expected set: {}, actual set: {}",
          expectedSetStr,
          actualSetStr);
      if (lastException.get() == null) {
        LOGGER.info("No exception during awaiting");
      } else {
        LOGGER.error("Last exception during awaiting:", lastException.get());
      }
      throw e;
    }

    LOGGER.info("DataNodes has been successfully changed to {}", lastTimeDataNodeLocations.get());
  }

  public void restartDataNodes(List<DataNodeWrapper> dataNodeWrappers) {
    dataNodeWrappers.parallelStream()
        .forEach(
            nodeWrapper -> {
              nodeWrapper.stopForcibly();
              Awaitility.await()
                  .atMost(1, TimeUnit.MINUTES)
                  .pollDelay(2, TimeUnit.SECONDS)
                  .until(() -> !nodeWrapper.isAlive());
              LOGGER.info("Node {} stopped.", nodeWrapper.getId());
              nodeWrapper.start();
              Awaitility.await()
                  .atMost(1, TimeUnit.MINUTES)
                  .pollDelay(2, TimeUnit.SECONDS)
                  .until(nodeWrapper::isAlive);
              try {
                TimeUnit.SECONDS.sleep(10);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              LOGGER.info("Node {} restarted.", nodeWrapper.getId());
            });
  }

  public static String generateRemoveString(Set<Integer> dataNodes) {
    StringBuilder sb = new StringBuilder("remove datanode ");

    for (Integer node : dataNodes) {
      sb.append(node).append(", ");
    }

    sb.setLength(sb.length() - 2);

    return sb.toString();
  }
}
