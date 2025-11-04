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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;
import static org.junit.Assert.fail;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionReconstructForIoTV1IT extends IoTDBRegionOperationReliabilityITFramework {
  private static final String RECONSTRUCT_FORMAT = "reconstruct region %d on %d";
  private static Logger LOGGER = LoggerFactory.getLogger(IoTDBRegionReconstructForIoTV1IT.class);

  private boolean deleteTsFiles(File file) {
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          if (!deleteTsFiles(f)) {
            return false;
          } else if (f.getName().endsWith(".tsfile")) {
            LOGGER.info("{} is removed", f);
          }
        }
      }
    } else if (file.getName().endsWith(".tsfile")) {
      return file.delete();
    }
    return true;
  }

  @Test
  public void normal1C3DTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3);
    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");

    EnvFactory.getEnv().initClusterEnvironment(1, 3);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // prepare data
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      // collect necessary information
      Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap =
          getDataRegionMapWithLeader(statement);
      Set<Integer> allDataNodeId = getAllDataNodes(statement);

      // select datanode
      final int selectedRegion = 3;
      Assert.assertTrue(dataRegionMap.containsKey(selectedRegion));
      Pair<Integer, Set<Integer>> leaderAndNodeIds = dataRegionMap.get(selectedRegion);
      Assert.assertEquals(2, leaderAndNodeIds.right.size());
      // reconstruct from the leader to ensure no data is lost
      final int dataNodeToBeClosed = leaderAndNodeIds.left;
      final int dataNodeToBeReconstructed =
          leaderAndNodeIds.right.stream().filter(x -> x != dataNodeToBeClosed).findAny().get();
      final int dataNodeAlwaysGood =
          allDataNodeId.stream()
              .filter(x -> x != dataNodeToBeReconstructed && x != dataNodeToBeClosed)
              .findAny()
              .get();
      final DataNodeWrapper dataNodeWrapper =
          EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeAlwaysGood).get();
      Session session =
          new Session.Builder()
              .host(dataNodeWrapper.getIp())
              .port(dataNodeWrapper.getPort())
              .build();
      session.open();

      LOGGER.info(
          "IoTDBRegionReconstructForIoTV1IT: Node {} is to be closed",
          EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeClosed).get().getIpAndPortString());
      LOGGER.info(
          "IoTDBRegionReconstructForIoTV1IT: Node {} is to be reconstructed",
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(dataNodeToBeReconstructed)
              .get()
              .getIpAndPortString());

      // delete one DataNode's data dir, stop another DataNode
      File dataDirToBeReconstructed =
          new File(
              EnvFactory.getEnv()
                  .dataNodeIdToWrapper(dataNodeToBeReconstructed)
                  .get()
                  .getDataPath());

      EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeClosed).get().stopForcibly();

      while (true) {
        try (Connection flushConn =
                EnvFactory.getEnv()
                    .getConnection(
                        EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeReconstructed).get(),
                        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
                        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
                        BaseEnv.TREE_SQL_DIALECT);
            Statement flushStatement = flushConn.createStatement()) {
          flushStatement.execute("FLUSH ON LOCAL");
        }
        deleteTsFiles(dataDirToBeReconstructed);

        // now, the query should throw exception
        try {
          session.executeQueryStatement("select * from root.sg.**");
        } catch (StatementExecutionException e) {
          break;
        }
      }

      // start DataNode, reconstruct the delete one
      EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeClosed).get().start();
      EnvFactory.getAbstractEnv().checkNodeInStatus(dataNodeToBeClosed, NodeStatus.Running);
      session.executeNonQueryStatement(
          String.format(RECONSTRUCT_FORMAT, selectedRegion, dataNodeToBeReconstructed));
      try {
        Awaitility.await()
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(10, TimeUnit.MINUTES)
            .until(
                () ->
                    getRegionStatusWithoutRunning(session).isEmpty()
                        && dataDirToBeReconstructed.getAbsoluteFile().exists());
      } catch (Exception e) {
        LOGGER.error(
            "Two factor: {} && {}",
            getRegionStatusWithoutRunning(session),
            dataDirToBeReconstructed.getAbsoluteFile().exists());
        fail();
      }
      EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeClosed).get().stopForcibly();

      // now, the query should work fine, but the update of region status may have some delay
      long start = System.currentTimeMillis();
      while (true) {
        SessionDataSet resultSet;
        try {
          resultSet = session.executeQueryStatement("select * from root.sg.**");
        } catch (StatementExecutionException e) {
          if (System.currentTimeMillis() - start > 60_000L) {
            fail("Cannot execute query within 60s");
          }
          TimeUnit.SECONDS.sleep(1);
          continue;
        }
        if (resultSet.hasNext()) {
          RowRecord rowRecord = resultSet.next();
          Assert.assertEquals("2.0", rowRecord.getField(0).getStringValue());
          Assert.assertEquals("1.0", rowRecord.getField(1).getStringValue());
          break;
        }
      }
    }
  }
}
