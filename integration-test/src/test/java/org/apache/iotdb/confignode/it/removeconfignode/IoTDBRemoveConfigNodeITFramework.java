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

package org.apache.iotdb.confignode.it.removeconfignode;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.it.removedatanode.SQLModel;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.exception.InconsistentDataException;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.relational.it.query.old.aligned.TableUtils;

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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMap;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.getConnectionWithSQLType;
import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

public class IoTDBRemoveConfigNodeITFramework {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRemoveConfigNodeITFramework.class);
  private static final String TREE_MODEL_INSERTION =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 1, 2)";

  private static final String SHOW_CONFIGNODES = "show confignodes";

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

  public void testRemoveConfigNode(
      final int dataReplicateFactor,
      final int schemaReplicationFactor,
      final int configNodeNum,
      final int dataNodeNum,
      final int dataRegionPerDataNode,
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

    try (final Connection connection = makeItCloseQuietly(getConnectionWithSQLType(model));
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      if (SQLModel.TABLE_MODEL_SQL.equals(model)) {
        // Insert data in table model
        TableUtils.insertData();
      } else {
        // Insert data in tree model
        statement.execute(TREE_MODEL_INSERTION);
      }

      Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
      regionMap.forEach(
          (key, valueSet) -> {
            LOGGER.info("Key: {}, Value: {}", key, valueSet);
            if (valueSet.size() != dataReplicateFactor) {
              Assert.fail();
            }
          });

      // Get all config nodes
      ResultSet result = statement.executeQuery(SHOW_CONFIGNODES);
      Set<Integer> allConfigNodeId = new HashSet<>();
      while (result.next()) {
        allConfigNodeId.add(result.getInt(ColumnHeaderConstant.NODE_ID));
      }

      AtomicReference<SyncConfigNodeIServiceClient> clientRef = new AtomicReference<>(client);

      int removeConfigNodeId = allConfigNodeId.iterator().next();
      String removeConfigNodeSQL = generateRemoveString(removeConfigNodeId);
      LOGGER.info("Remove ConfigNodes SQL: {}", removeConfigNodeSQL);
      try {
        statement.execute(removeConfigNodeSQL);
      } catch (IoTDBSQLException e) {
        LOGGER.error("Remove ConfigNodes SQL execute fail: {}", e.getMessage());
        Assert.fail();
      }
      LOGGER.info("Remove ConfigNodes SQL submit successfully.");

      // Wait until success
      try {
        awaitUntilSuccess(statement, removeConfigNodeId);
      } catch (ConditionTimeoutException e) {
        LOGGER.error("Remove ConfigNodes timeout in 2 minutes");
        Assert.fail();
      }

      LOGGER.info("Remove ConfigNodes success");
    } catch (InconsistentDataException e) {
      LOGGER.error("Unexpected error:", e);
    }
  }

  private static void awaitUntilSuccess(Statement statement, int removeConfigNodeId) {
    AtomicReference<Set<Integer>> lastTimeConfigNodes = new AtomicReference<>();
    AtomicReference<Exception> lastException = new AtomicReference<>();

    try {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(2, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  // Get all config nodes
                  ResultSet result = statement.executeQuery(SHOW_CONFIGNODES);
                  Set<Integer> allConfigNodeId = new HashSet<>();
                  while (result.next()) {
                    allConfigNodeId.add(result.getInt(ColumnHeaderConstant.NODE_ID));
                  }
                  lastTimeConfigNodes.set(allConfigNodeId);
                  return !allConfigNodeId.contains(removeConfigNodeId);
                } catch (Exception e) {
                  // Any exception can be ignored
                  lastException.set(e);
                  return false;
                }
              });
    } catch (ConditionTimeoutException e) {
      if (lastTimeConfigNodes.get() == null) {
        LOGGER.error(
            "Maybe show confignodes fail, lastTimeConfigNodes is null, last Exception:",
            lastException.get());
        throw e;
      }
      String actualSetStr = lastTimeConfigNodes.get().toString();
      lastTimeConfigNodes.get().remove(removeConfigNodeId);
      String expectedSetStr = lastTimeConfigNodes.get().toString();
      LOGGER.error(
          "Remove ConfigNode timeout in 2 minutes, expected set: {}, actual set: {}",
          expectedSetStr,
          actualSetStr);
      if (lastException.get() == null) {
        LOGGER.info("No exception during awaiting");
      } else {
        LOGGER.error("Last exception during awaiting:", lastException.get());
      }
      throw e;
    }
  }

  public static String generateRemoveString(Integer configNodeId) {
    return "remove confignode " + configNodeId;
  }
}
