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

package org.apache.iotdb.pipe.it.autocreate;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeTsFileParseOOMIT extends AbstractPipeDualAutoIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getCommonConfig().setPipeMemoryManagementEnabled(true);
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(0);
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(0);
  }

  @Test
  public void testPipeKeepsRunningWhenTsFileProcessingTemporarilyOutOfMemory() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList("insert into root.temporary_oom.d0(time,s1) values (0,1)", "flush"),
        null);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", "root.temporary_oom");
      extractorAttributes.put("extractor.realtime.enable", "false");

      processorAttributes.put("processor", "throwing-exception-processor");
      processorAttributes.put("stages", "process-tsfile-insertion-event-with-temporary-oom");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("temporary_oom_pipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      assertPipeRunningFor(client, "temporary_oom_pipe", 35_000L);
    }
  }

  private void assertPipeRunningFor(
      final SyncConfigNodeIServiceClient client, final String pipeName, final long durationMs)
      throws Exception {
    final long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < durationMs) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      final TShowPipeInfo pipeInfo =
          showPipeResult.stream().filter(info -> info.id.equals(pipeName)).findFirst().orElse(null);
      Assert.assertNotNull(pipeInfo);
      Assert.assertEquals("RUNNING", pipeInfo.state);
      Thread.sleep(1000);
    }
  }
}
