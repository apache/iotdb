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
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionGroupExpandAndShrinkForIoTV1IT
    extends IoTDBRegionOperationReliabilityITFramework {
  private static final String EXPAND_FORMAT = "extend region %d to %d";
  private static final String SHRINK_FORMAT = "remove region %d from %d";

  private static Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionGroupExpandAndShrinkForIoTV1IT.class);

  /**
   * 1. Expand: {a} -> {a,b} -> ... -> {a,b,c,d,e}
   *
   * <p>2. Check
   *
   * <p>3. Shrink: {a,b,c,d,e} -> {a,c,d,e} -> ... -> {d}
   *
   * <p>4. Check
   */
  @Test
  public void normal1C5DTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);

    EnvFactory.getEnv().initClusterEnvironment(1, 5);

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // prepare data
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      // collect necessary information
      Map<Integer, Set<Integer>> regionMap = getAllRegionMap(statement);
      Set<Integer> allDataNodeId = getAllDataNodes(statement);

      // expect one data region, one schema region
      Assert.assertEquals(2, regionMap.size());

      // expand
      for (int selectedRegion : regionMap.keySet()) {
        for (int i = 0; i < 4; i++) {
          int targetDataNode =
              selectDataNodeNotContainsRegion(allDataNodeId, regionMap, selectedRegion);
          regionGroupExpand(statement, client, selectedRegion, targetDataNode);
          // update regionMap every time
          regionMap = getAllRegionMap(statement);
        }
      }

      // shrink
      for (int selectedRegion : regionMap.keySet()) {
        for (int i = 0; i < 4; i++) {
          int targetDataNode =
              selectDataNodeContainsRegion(allDataNodeId, regionMap, selectedRegion);
          regionGroupShrink(statement, client, selectedRegion, targetDataNode);
          // update regionMap every time
          regionMap = getAllRegionMap(statement);
        }
      }
    }
  }

  private void regionGroupExpand(
      Statement statement,
      SyncConfigNodeIServiceClient client,
      int selectedRegion,
      int targetDataNode)
      throws Exception {
    statement.execute(String.format(EXPAND_FORMAT, selectedRegion, targetDataNode));

    Predicate<TShowRegionResp> expandRegionPredicate =
        tShowRegionResp -> {
          Map<Integer, Set<Integer>> newRegionMap =
              getRunningRegionMap(tShowRegionResp.getRegionInfoList());
          Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
          return dataNodes.contains(targetDataNode);
        };

    awaitUntilSuccess(client, expandRegionPredicate, Optional.of(targetDataNode), Optional.empty());

    LOGGER.info("Region {} has expanded to DataNode {}", selectedRegion, targetDataNode);
  }

  private void regionGroupShrink(
      Statement statement,
      SyncConfigNodeIServiceClient client,
      int selectedRegion,
      int targetDataNode)
      throws Exception {
    statement.execute(String.format(SHRINK_FORMAT, selectedRegion, targetDataNode));

    Predicate<TShowRegionResp> shrinkRegionPredicate =
        tShowRegionResp -> {
          Map<Integer, Set<Integer>> newRegionMap =
              getRegionMap(tShowRegionResp.getRegionInfoList());
          Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
          return !dataNodes.contains(targetDataNode);
        };

    awaitUntilSuccess(client, shrinkRegionPredicate, Optional.empty(), Optional.of(targetDataNode));

    LOGGER.info("Region {} has shrunk from DataNode {}", selectedRegion, targetDataNode);
  }
}
