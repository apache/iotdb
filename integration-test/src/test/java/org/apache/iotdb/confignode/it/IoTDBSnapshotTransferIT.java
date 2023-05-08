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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Optional;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
@Ignore
public class IoTDBSnapshotTransferIT {

  private final long snapshotMagic = 99;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(2)
        .setDataRatisTriggerSnapshotThreshold(snapshotMagic)
        .setConfigNodeRatisSnapshotTriggerThreshold(1); // must trigger snapshot

    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSnapshotTransfer() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient configClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      statement.execute("CREATE DATABASE root.emma");
      statement.execute(
          "CREATE TIMESERIES root.emma.william.ethereal WITH DATATYPE=INT32,ENCODING=PLAIN");

      // insert large amounts of data to trigger a snapshot
      for (int i = 0; i < snapshotMagic + 1; i++) {
        final String insert =
            String.format(
                "INSERT INTO root.emma.william(timestamp, ethereal) values(%d, %d)", i, i);
        statement.execute(insert);
      }

      final TShowRegionResp result = configClient.showRegion(new TShowRegionReq());
      Assert.assertNotNull(result.getRegionInfoList());

      final Optional<TRegionInfo> dataRegionInfo =
          result.getRegionInfoList().stream()
              .filter(
                  info ->
                      info.getConsensusGroupId().getType().equals(TConsensusGroupType.DataRegion))
              .findAny();
      Assert.assertTrue(dataRegionInfo.isPresent());

      final int dataNodeId = dataRegionInfo.get().getDataNodeId();

      final TDataNodeConfigurationResp locationResp =
          configClient.getDataNodeConfiguration(dataNodeId);
      Assert.assertNotNull(locationResp.getDataNodeConfigurationMap());

      // remove this datanode and the regions will be transferred to the other one
      final TDataNodeRemoveReq removeReq = new TDataNodeRemoveReq();
      final TDataNodeLocation location =
          locationResp.getDataNodeConfigurationMap().get(dataNodeId).getLocation();

      removeReq.setDataNodeLocations(Collections.singletonList(location));
      final TDataNodeRemoveResp removeResp = configClient.removeDataNode(removeReq);
      Assert.assertEquals(
          removeResp.getStatus().getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());

      // insert new data to ensure that the data regions are properly functioning
      for (int i = 0; i < snapshotMagic + 1; i++) {
        final String insert =
            String.format(
                "INSERT INTO root.emma.william(timestamp, ethereal) values (%d, %d)",
                i + snapshotMagic * 2, i + snapshotMagic * 2);
        statement.execute(insert);
      }

      final ResultSet readResult = statement.executeQuery("SELECT count(*) from root.emma.william");
      readResult.next();
      Assert.assertEquals(
          2 * (snapshotMagic + 1), readResult.getInt("count(root.emma.william.ethereal)"));

      EnvFactory.getEnv().registerNewConfigNode(true);
      final TShowRegionResp registerResult = configClient.showRegion(new TShowRegionReq());
      Assert.assertNotNull(result.getRegionInfoList());

      final long configNodeGroupCount =
          registerResult.getRegionInfoList().stream()
              .filter(
                  info ->
                      info.getConsensusGroupId().getType().equals(TConsensusGroupType.ConfigRegion))
              .count();
      Assert.assertEquals(4, configNodeGroupCount);
    }
  }
}
