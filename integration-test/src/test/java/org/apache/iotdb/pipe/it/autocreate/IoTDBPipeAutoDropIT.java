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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeAutoDropIT extends AbstractPipeDualAutoIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getCommonConfig().setPipeAutoSplitFullEnabled(true);
    receiverEnv.getConfig().getCommonConfig().setPipeAutoSplitFullEnabled(true);
    senderEnv.getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");
  }

  @Test
  public void testAutoDropInHistoricalTransfer() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    TestUtils.executeNonQuery(
        senderEnv,
        String.format(
            "create pipe a2b with sink ('node-urls'='%s')", receiverDataNode.getIpAndPortString()),
        null);

    TestUtils.executeNonQuery(senderEnv, "insert into root.db.d1(time,s1) values (1,1)", null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(*) from root.**",
        "count(root.db.d1.s1),",
        Collections.singleton("1,"));

    await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(600, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (final Connection connection = senderEnv.getConnection();
                  final Statement statement = connection.createStatement();
                  final ResultSet result = statement.executeQuery("show pipes")) {
                int pipeNum = 0;
                while (result.next()) {
                  final String pipeName = result.getString(ColumnHeaderConstant.ID);
                  if (!pipeName.contains(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)
                      && pipeName.endsWith("_history")) {
                    pipeNum++;
                  }
                }
                Assert.assertEquals(0, pipeNum);
              }
            });
  }

  @Test
  public void testAutoDropInHistoricalTransferWithTimeRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQuery(
          senderEnv,
          "insert into root.db.d1(time,s1) values (1000,1),(2000,2),(3000,3),(4000,4),(5000,5)",
          null);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "query");
      extractorAttributes.put("extractor.start-time", "1970-01-01T08:00:02+08:00");
      extractorAttributes.put("extractor.end-time", "1970-01-01T08:00:04+08:00");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("3,"));

      TestUtils.assertDataEventuallyOnEnv(
          senderEnv,
          "show pipes",
          "ID,CreationTime,State,PipeSource,PipeProcessor,PipeSink,ExceptionMessage,RemainingEventCount,EstimatedRemainingSeconds,",
          Collections.emptySet());
    }
  }
}
