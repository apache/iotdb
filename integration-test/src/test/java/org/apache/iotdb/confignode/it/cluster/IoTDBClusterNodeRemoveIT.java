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

package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterNodeRemoveIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClusterNodeRemoveIT.class);

  private static final String sbinPath = EnvFactory.getEnv().getSbinPath();
  private static final String libPath = EnvFactory.getEnv().getLibPath();

  private static final String testRegionGroupExtensionPolicy = "AUTO";
  private static final String testSchemaRegionConsensusProtocolClass =
      ConsensusFactory.RATIS_CONSENSUS;
  private static final String testDataRegionConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;
  private static final int testReplicationFactor = 3;

  private static final double testSchemaRegionPerDataNode = 2.0;
  private static final double testDataRegionPerDataNode = 2.0;

  private static final String database = "root.db";
  private static final long testTimePartitionInterval = 604800000;
  private static final int testMinSchemaRegionGroupNum = 2;
  private static final int testMinDataRegionGroupNum = 2;
  private static final int testDataNodeNum = 3;

  private static final int removeDataNodeId = 2;
  private static final int correctScriptExitValue = 1;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(testSchemaRegionConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(testDataRegionConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor)
        .setSchemaRegionPerDataNode(testSchemaRegionPerDataNode)
        .setDataRegionPerDataNode(testDataRegionPerDataNode)
        .setSchemaRegionGroupExtensionPolicy(testRegionGroupExtensionPolicy)
        .setDataRegionGroupExtensionPolicy(testRegionGroupExtensionPolicy)
        .setTimePartitionInterval(testTimePartitionInterval);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testClusterNodeRemove()
      throws ClientManagerException, IOException, InterruptedException, TException,
          IllegalPathException {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status =
          client.setDatabase(
              new TDatabaseSchema(database)
                  .setMinSchemaRegionGroupNum(testMinSchemaRegionGroupNum)
                  .setMinDataRegionGroupNum(testMinDataRegionGroupNum));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Create 2 SchemaPartitions to extend 2 SchemaRegionGroups
      for (int i = 0; i < testMinSchemaRegionGroupNum; i++) {
        String device = database + ".d" + i + ".s";
        TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq();
        TSchemaPartitionTableResp schemaPartitionTableResp;
        Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;
        ByteBuffer buffer = ConfigNodeTestUtils.generatePatternTreeBuffer(new String[] {device});
        schemaPartitionReq.setPathPatternTree(buffer);
        schemaPartitionTableResp = client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            schemaPartitionTableResp.getStatus().getCode());
        Assert.assertEquals(1, schemaPartitionTableResp.getSchemaPartitionTableSize());
        schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
        Assert.assertEquals(1, schemaPartitionTable.get(database).size());
      }

      // Create 2 DataPartitions to extend 2 DataRegionGroups
      for (int i = 0; i < testMinDataRegionGroupNum; i++) {
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
            ConfigNodeTestUtils.constructPartitionSlotsMap(
                database, i, i + 1, i, i + 1, testTimePartitionInterval);
        TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
        TDataPartitionTableResp dataPartitionTableResp = null;
        for (int retry = 0; retry < 5; retry++) {
          // Build new Client since it's unstable in Win8 environment
          try (SyncConfigNodeIServiceClient configNodeClient =
              (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
            dataPartitionTableResp =
                configNodeClient.getOrCreateDataPartitionTable(dataPartitionReq);
            if (dataPartitionTableResp != null) {
              break;
            }
          } catch (Exception e) {
            // Retry sometimes in order to avoid request timeout
            LOGGER.error(e.getMessage());
            TimeUnit.SECONDS.sleep(1);
          }
        }
        Assert.assertNotNull(dataPartitionTableResp);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
        Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
        ConfigNodeTestUtils.checkDataPartitionTable(
            database,
            i,
            i + 1,
            i,
            i + 1,
            testTimePartitionInterval,
            dataPartitionTableResp.getDataPartitionTable());
      }

      // Register 1 DataNode
      EnvFactory.getEnv().registerNewDataNode(true);

      // Remove 1 DataNode
      String os = System.getProperty("os.name").toLowerCase();
      if (os.startsWith("windows")) {
        removeOnWindows();
      } else {
        removeOnUnix();
      }

      // The number of DataNodes should be 3
      boolean removeSuccess = false;
      for (int retry = 0; retry < 10; retry++) {
        TShowClusterResp showClusterResp = client.showCluster();
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), showClusterResp.getStatus().getCode());
        if (showClusterResp.getDataNodeListSize() == 3) {
          removeSuccess = true;
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }
      Assert.assertTrue(removeSuccess);

      // The Regions should be redistributed
      Map<Integer, AtomicInteger> schemaRegionCounter = new HashMap<>();
      Map<Integer, AtomicInteger> dataRegionCounter = new HashMap<>();
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                Assert.assertEquals(RegionStatus.Running.getStatus(), regionInfo.getStatus());
                switch (regionInfo.getConsensusGroupId().getType()) {
                  case SchemaRegion:
                    schemaRegionCounter
                        .computeIfAbsent(regionInfo.getDataNodeId(), empty -> new AtomicInteger(0))
                        .incrementAndGet();
                    break;
                  case DataRegion:
                    dataRegionCounter
                        .computeIfAbsent(regionInfo.getDataNodeId(), empty -> new AtomicInteger(0))
                        .incrementAndGet();
                    break;
                }
              });
      schemaRegionCounter.forEach(
          (dataNodeId, counter) -> Assert.assertEquals(testMinSchemaRegionGroupNum, counter.get()));
      dataRegionCounter.forEach(
          (dataNodeId, counter) -> Assert.assertEquals(testMinDataRegionGroupNum, counter.get()));
    }
  }

  private void removeOnWindows() throws IOException, InterruptedException {
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "remove-datanode.bat",
            removeDataNodeId + "");
    builder.environment().put("CLASSPATH", libPath);
    monitorRemoveProcess(builder.start());
  }

  private void removeOnUnix() throws IOException, InterruptedException {
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh", sbinPath + File.separator + "remove-datanode.sh", removeDataNodeId + "");
    builder.environment().put("CLASSPATH", libPath);
    monitorRemoveProcess(builder.start());
  }

  private void monitorRemoveProcess(Process p) throws IOException, InterruptedException {
    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    while ((line = br.readLine()) != null) {
      LOGGER.info(line);
    }
    br.close();
    p.destroy();

    while (p.isAlive()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    assertEquals(correctScriptExitValue, p.exitValue());
  }
}
