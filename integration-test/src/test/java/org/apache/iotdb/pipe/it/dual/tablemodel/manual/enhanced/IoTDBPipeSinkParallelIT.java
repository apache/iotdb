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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.PipeTestUtils.RetryFlushHandler;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeSinkParallelIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    // Retain multiple Regions for the three parallel tasks without generating many tiny files
    // from CPU-based defaults or exhausting query memory on concurrent schema analysis.
    for (final BaseEnv env : new BaseEnv[] {senderEnv, receiverEnv}) {
      env.getConfig().getCommonConfig().setDataRegionPerDataNode(2);
      env.getConfig().getDataNodeConfig().setLoadTsFileAnalyzeSchemaMemorySizeInBytes(256 * 1024L);
    }
  }

  // Three parallel batched tasks must deliver ten historical and ten realtime table rows plus four
  // tree rows, with per-Region serialization disabled and double-living mode enabled.
  @Test
  public void testIoTConnectorParallel() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    TableModelUtils.insertData("test", "test", 0, 10, senderEnv);
    final Set<String> expectedResSet = new HashSet<>();
    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "true");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.serialize-by-region", "false");
      connectorAttributes.put("connector.parallel.tasks", "3");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.sg1.d1(time, s1) values (0, 1)",
              "insert into root.sg1.d1(time, s1) values (1, 2)",
              "insert into root.sg1.d1(time, s1) values (2, 3)",
              "insert into root.sg1.d1(time, s1) values (3, 4)",
              "flush"),
          null);
      TableModelUtils.insertData("test", "test", 10, 20, senderEnv);
      expectedResSet.add("0,1.0,");
      expectedResSet.add("1,2.0,");
      expectedResSet.add("2,3.0,");
      expectedResSet.add("3,4.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.sg1.**",
          "Time,root.sg1.d1.s1,",
          expectedResSet,
          handleFailure);

      TableModelUtils.assertCountData("test", "test", 20, receiverEnv, handleFailure);
    }
  }
}
