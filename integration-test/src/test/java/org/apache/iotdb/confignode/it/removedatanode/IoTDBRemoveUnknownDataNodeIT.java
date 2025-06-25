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
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.exception.InconsistentDataException;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.relational.it.query.old.aligned.TableUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMap;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.awaitUntilSuccess;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.generateRemoveString;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.getConnectionWithSQLType;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.selectRemoveDataNodes;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.stopDataNodes;
import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRemoveUnknownDataNodeIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBRemoveUnknownDataNodeIT.class);

  private static final String SHOW_DATANODES = "show datanodes";

  private static final String DEFAULT_SCHEMA_REGION_GROUP_EXTENSION_POLICY = "CUSTOM";
  private static final String DEFAULT_DATA_REGION_GROUP_EXTENSION_POLICY = "CUSTOM";

  @Before
  public void setUp() throws Exception {
    // Setup common environmental configuration
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionGroupExtensionPolicy(DEFAULT_SCHEMA_REGION_GROUP_EXTENSION_POLICY)
        .setDataRegionGroupExtensionPolicy(DEFAULT_DATA_REGION_GROUP_EXTENSION_POLICY);
  }

  @After
  public void tearDown() throws InterruptedException {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  //  @Test
  //  public void success1C4DIoTTest() throws Exception {
  //    // Setup 1C4D, and remove 1D, this test should success
  //    successTest(2, 3, 1, 4, 1, 2, SQLModel.NOT_USE_SQL, ConsensusFactory.IOT_CONSENSUS);
  //  }
  //
  //  @Test
  //  public void success1C4DIoTV2Test() throws Exception {
  //    // Setup 1C4D, and remove 1D, this test should success
  //    successTest(2, 3, 1, 4, 1, 2, SQLModel.NOT_USE_SQL, ConsensusFactory.IOT_CONSENSUS_V2);
  //  }

  //  @Test
  //  public void fail1C3DIoTTest() throws Exception {
  //    // Setup 1C3D with schema replication factor = 3, and remove 1D, this test should fail due
  // to
  //    // insufficient DN for holding schema
  //    failTest(2, 3, 1, 3, 1, 2, SQLModel.NOT_USE_SQL, ConsensusFactory.IOT_CONSENSUS);
  //  }
  //
  //  @Test
  //  public void fail1C3DIoTV2Test() throws Exception {
  //    // Setup 1C3D with schema replication factor = 3, and remove 1D, this test should fail due
  // to
  //    // insufficient DN for holding schema
  //    failTest(2, 3, 1, 3, 1, 2, SQLModel.NOT_USE_SQL, ConsensusFactory.IOT_CONSENSUS_V2);
  //  }

  //  @Test
  //  public void success1C4DIoTTestUseSQL() throws Exception {
  //    // Setup 1C4D, and remove 1D, this test should success
  //    successTest(2, 3, 1, 4, 1, 2, SQLModel.TREE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS);
  //  }
  //
  //  @Test
  //  public void success1C4DIoTV2TestUseSQL() throws Exception {
  //    // Setup 1C4D, and remove 1D, this test should success
  //    successTest(2, 3, 1, 4, 1, 2, SQLModel.TREE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS_V2);
  //  }

  @Test
  public void fail1C3DTestIoTUseSQL() throws Exception {
    // Setup 1C3D with schema replication factor = 3, and remove 1D, this test should fail due to
    // insufficient DN for holding schema
    failTest(2, 3, 1, 3, 1, 2, SQLModel.TREE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS);
  }

  //  @Test
  public void fail1C3DTestIoTV2UseSQL() throws Exception {
    // Setup 1C3D with schema replication factor = 3, and remove 1D, this test should fail due to
    // insufficient DN for holding schema
    failTest(2, 3, 1, 3, 1, 2, SQLModel.TREE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS_V2);
  }

  @Test
  public void success1C4DIoTTestUseTableSQL() throws Exception {
    // Setup 1C4D, and remove 1D, this test should success
    successTest(2, 3, 1, 4, 1, 2, SQLModel.TABLE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS);
  }

  //  @Test
  public void success1C4DIoTV2TestUseTableSQL() throws Exception {
    // Setup 1C4D, and remove 1D, this test should success
    successTest(2, 3, 1, 4, 1, 2, SQLModel.TABLE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS_V2);
  }

  //  @Test
  //  public void fail1C3DIoTTestUseTableSQL() throws Exception {
  //    // Setup 1C3D with schema replication factor = 3, and remove 1D, this test should fail due
  // to
  //    // insufficient DN for holding schema
  //    failTest(2, 3, 1, 3, 1, 2, SQLModel.TABLE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS);
  //  }
  //
  //  @Test
  //  public void fail1C3DIoTV2TestUseTableSQL() throws Exception {
  //    // Setup 1C3D with schema replication factor = 3, and remove 1D, this test should fail due
  // to
  //    // insufficient DN for holding schema
  //    failTest(2, 3, 1, 3, 1, 2, SQLModel.TABLE_MODEL_SQL, ConsensusFactory.IOT_CONSENSUS_V2);
  //  }

  private void successTest(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      final int removeDataNodeNum,
      final int dataRegionPerDataNode,
      final SQLModel model,
      final String dataRegionGroupConsensusProtocol)
      throws Exception {
    testRemoveDataNode(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        removeDataNodeNum,
        dataRegionPerDataNode,
        true,
        model,
        dataRegionGroupConsensusProtocol);
  }

  private void failTest(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      final int removeDataNodeNum,
      final int dataRegionPerDataNode,
      final SQLModel model,
      final String dataRegionGroupConsensusProtocol)
      throws Exception {
    testRemoveDataNode(
        dataReplicateFactor,
        schemaReplicationFactor,
        configNodeNum,
        dataNodeNum,
        removeDataNodeNum,
        dataRegionPerDataNode,
        false,
        model,
        dataRegionGroupConsensusProtocol);
  }

  public void testRemoveDataNode(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      final int removeDataNodeNum,
      final int dataRegionPerDataNode,
      final boolean expectRemoveSuccess,
      final SQLModel model,
      final String dataRegionGroupConsensusProtocol)
      throws Exception {
    // Set up specific environment
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(dataRegionGroupConsensusProtocol)
        .setSchemaReplicationFactor(schemaReplicationFactor)
        .setDataReplicationFactor(dataReplicateFactor)
        .setDefaultDataRegionGroupNumPerDatabase(
            dataRegionPerDataNode * dataNodeNum / dataReplicateFactor);
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, dataNodeNum);

    final Set<Integer> removeDataNodes = new HashSet<>();
    final List<TDataNodeLocation> removeDataNodeLocations = new ArrayList<>();

    try (final Connection connection = makeItCloseQuietly(getConnectionWithSQLType(model));
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      if (SQLModel.TABLE_MODEL_SQL.equals(model)) {
        // Insert data in table model
        TableUtils.insertData();
      } else {
        // Insert data in tree model
        ConfigNodeTestUtils.insertTreeModelData(statement);
      }

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

      // Randomly select data nodes to remove
      removeDataNodes.addAll(selectRemoveDataNodes(allDataNodeId, removeDataNodeNum));
      List<DataNodeWrapper> removeDataNodeWrappers =
          removeDataNodes.stream()
              .map(dataNodeId -> EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).get())
              .collect(Collectors.toList());
      AtomicReference<SyncConfigNodeIServiceClient> clientRef = new AtomicReference<>(client);
      removeDataNodeLocations.addAll(
          clientRef
              .get()
              .getDataNodeConfiguration(-1)
              .getDataNodeConfigurationMap()
              .values()
              .stream()
              .map(TDataNodeConfiguration::getLocation)
              .filter(location -> removeDataNodes.contains(location.getDataNodeId()))
              .collect(Collectors.toList()));
      // Stop DataNodes before removing them
      stopDataNodes(removeDataNodeWrappers);
      LOGGER.info("RemoveDataNodes: {} are stopped.", removeDataNodes);
    } catch (InconsistentDataException e) {
      LOGGER.error("Unexpected error:", e);
    }

    // Establish a new connection after stopping data nodes
    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      AtomicReference<SyncConfigNodeIServiceClient> clientRef = new AtomicReference<>(client);
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

      // Check the data region distribution after removing data nodes
      Map<Integer, Set<Integer>> afterRegionMap = getDataRegionMap(statement);
      afterRegionMap.forEach(
          (key, valueSet) -> {
            LOGGER.info("Key: {}, Value: {}", key, valueSet);
            if (valueSet.size() != dataReplicateFactor) {
              Assert.fail();
            }
          });
    } catch (InconsistentDataException e) {
      LOGGER.error("Unexpected error:", e);
    }
  }
}
