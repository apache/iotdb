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
package org.apache.iotdb.confignode;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBConfigNodeSnapshotIT {

  protected static String originalConfigNodeConsensusProtocolClass;
  private static final String testConfigNodeConsensusProtocolClass =
      "org.apache.iotdb.consensus.ratis.RatisConsensus";

  protected static int originalRatisSnapshotTriggerThreshold;
  private static final int testRatisSnapshotTriggerThreshold = 100;

  protected static long originalTimePartitionInterval;
  private static final long testTimePartitionInterval = 86400;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass(testConfigNodeConsensusProtocolClass);

    originalRatisSnapshotTriggerThreshold =
        ConfigFactory.getConfig().getRatisSnapshotTriggerThreshold();
    ConfigFactory.getConfig().setRatisSnapshotTriggerThreshold(testRatisSnapshotTriggerThreshold);

    originalTimePartitionInterval = ConfigFactory.getConfig().getTimePartitionInterval();
    ConfigFactory.getConfig().setTimePartitionIntervalForRouting(testTimePartitionInterval);

    // Init 3C3D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();

    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setRatisSnapshotTriggerThreshold(originalRatisSnapshotTriggerThreshold);
    ConfigFactory.getConfig().setTimePartitionIntervalForRouting(originalTimePartitionInterval);
  }

  private ByteBuffer generatePatternTreeBuffer(String path)
      throws IllegalPathException, IOException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath(path));
    patternTree.constructTree();

    PublicBAOS baos = new PublicBAOS();
    patternTree.serialize(baos);
    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Test
  public void testPartitionInfoSnapshot() throws IOException, IllegalPathException, TException {
    final String sg = "root.sg";
    final int storageGroupNum = 10;
    final int seriesPartitionSlotsNum = 100;
    final int timePartitionSlotsNum = 10;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {

      for (int i = 0; i < storageGroupNum; i++) {
        String storageGroup = sg + i;
        TSetStorageGroupReq setStorageGroupReq =
            new TSetStorageGroupReq(new TStorageGroupSchema(storageGroup));
        TSStatus status = client.setStorageGroup(setStorageGroupReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        for (int j = 0; j < seriesPartitionSlotsNum; j++) {
          TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(j);

          // Create SchemaPartition
          ByteBuffer patternTree = generatePatternTreeBuffer(storageGroup + ".d" + j + ".s");
          TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq(patternTree);
          TSchemaPartitionTableResp schemaPartitionTableResp =
              client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
          // All requests should success if snapshot success
          Assert.assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              schemaPartitionTableResp.getStatus().getCode());
          Assert.assertNotNull(schemaPartitionTableResp.getSchemaPartitionTable());
          Assert.assertEquals(1, schemaPartitionTableResp.getSchemaPartitionTableSize());
          Assert.assertNotNull(
              schemaPartitionTableResp.getSchemaPartitionTable().get(storageGroup));
          Assert.assertEquals(
              1, schemaPartitionTableResp.getSchemaPartitionTable().get(storageGroup).size());

          for (int k = 0; k < timePartitionSlotsNum; k++) {
            TTimePartitionSlot timePartitionSlot =
                new TTimePartitionSlot(testTimePartitionInterval * k);

            // Create DataPartition
            Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
                new HashMap<>();
            partitionSlotsMap.put(storageGroup, new HashMap<>());
            partitionSlotsMap
                .get(storageGroup)
                .put(seriesPartitionSlot, Collections.singletonList(timePartitionSlot));
            TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
            TDataPartitionTableResp dataPartitionTableResp =
                client.getOrCreateDataPartitionTable(dataPartitionReq);
            // All requests should success if snapshot success
            Assert.assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                dataPartitionTableResp.getStatus().getCode());
            Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
            Assert.assertEquals(1, dataPartitionTableResp.getDataPartitionTableSize());
            Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable().get(storageGroup));
            Assert.assertEquals(
                1, dataPartitionTableResp.getDataPartitionTable().get(storageGroup).size());
            Assert.assertNotNull(
                dataPartitionTableResp
                    .getDataPartitionTable()
                    .get(storageGroup)
                    .get(seriesPartitionSlot));
            Assert.assertEquals(
                1,
                dataPartitionTableResp
                    .getDataPartitionTable()
                    .get(storageGroup)
                    .get(seriesPartitionSlot)
                    .size());
          }
        }
      }
    }
  }
}
