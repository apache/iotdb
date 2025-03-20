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
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.read.common.RowRecord;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionReconstructForIoTV1IT extends IoTDBRegionOperationReliabilityITFramework {
  private static final String RECONSTRUCT_FORMAT = "reconstruct region %d on %d";
  private static Logger LOGGER = LoggerFactory.getLogger(IoTDBRegionReconstructForIoTV1IT.class);

  @Test
  public void normal1C3DTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3);

    EnvFactory.getEnv().initClusterEnvironment(1, 3);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // prepare data
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      // collect necessary information
      Map<Integer, Set<Integer>> dataRegionMap = getDataRegionMap(statement);
      Set<Integer> allDataNodeId = getAllDataNodes(statement);

      // select datanode
      final int selectedRegion = 1;
      Assert.assertTrue(dataRegionMap.containsKey(selectedRegion));
      Assert.assertEquals(2, dataRegionMap.get(selectedRegion).size());
      Iterator<Integer> iterator = dataRegionMap.get(selectedRegion).iterator();
      final int dataNodeToBeClosed = iterator.next();
      final int dataNodeToBeReconstructed = iterator.next();
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

      // delete one DataNode's data dir, stop another DataNode
      File dataDirToBeReconstructed =
          new File(
              EnvFactory.getEnv()
                  .dataNodeIdToWrapper(dataNodeToBeReconstructed)
                  .get()
                  .getDataPath());
      FileUtils.deleteDirectory(dataDirToBeReconstructed);
      EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeClosed).get().stopForcibly();

      // now, the query should throw exception
      Assert.assertThrows(
          StatementExecutionException.class,
          () -> session.executeQueryStatement("select * from root.**"));

      // start DataNode, reconstruct the delete one
      EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeClosed).get().start();
      EnvFactory.getAbstractEnv().checkNodeInStatus(dataNodeToBeClosed, NodeStatus.Running);
      session.executeNonQueryStatement(
          String.format(RECONSTRUCT_FORMAT, selectedRegion, dataNodeToBeReconstructed));
      try {
        Awaitility.await()
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(1, TimeUnit.MINUTES)
            .until(
                () ->
                    getRegionStatusWithoutRunning(session).isEmpty()
                        && dataDirToBeReconstructed.getAbsoluteFile().exists());
      } catch (Exception e) {
        LOGGER.error(
            "Two factor: {} && {}",
            getRegionStatusWithoutRunning(session).isEmpty(),
            dataDirToBeReconstructed.getAbsoluteFile().exists());
        Assert.fail();
      }
      EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeToBeClosed).get().stopForcibly();

      // now, the query should work fine
      SessionDataSet resultSet = session.executeQueryStatement("select * from root.**");
      RowRecord rowRecord = resultSet.next();
      Assert.assertEquals("2.0", rowRecord.getFields().get(0).getStringValue());
      Assert.assertEquals("1.0", rowRecord.getFields().get(1).getStringValue());
    }
  }
}
