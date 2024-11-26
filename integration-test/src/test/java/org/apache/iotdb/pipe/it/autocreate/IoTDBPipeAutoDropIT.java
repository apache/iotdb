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

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateReliabilityITFramework.closeQuietly;
import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeAutoDropIT extends AbstractPipeDualAutoIT {

  @Test
  public void testAutoDropInHistoricalTransfer() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Collections.singletonList("insert into root.db.d1(time, s1) values (1, 1)"))) {
        return;
      }
      awaitUntilFlush(senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "query");

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
          Collections.singleton("1,"));

      try (final Connection connection = closeQuietly(senderEnv.getConnection());
          final Statement statement = closeQuietly(connection.createStatement()); ) {
        ResultSet result = statement.executeQuery("show pipes");
        await()
            .pollInSameThread()
            .pollDelay(1L, TimeUnit.SECONDS)
            .pollInterval(1L, TimeUnit.SECONDS)
            .atMost(600, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  try {
                    int pipeNum = 0;
                    while (result.next()) {
                      if (!result
                          .getString(ColumnHeaderConstant.ID)
                          .contains(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
                        pipeNum++;
                      }
                    }
                    Assert.assertEquals(0, pipeNum);
                  } catch (Exception e) {
                    Assert.fail();
                  }
                });
      }
    }
  }

  @Test
  public void testAutoDropInHistoricalTransferWithTimeRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Collections.singletonList(
              "insert into root.db.d1(time, s1) values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)"))) {
        return;
      }
      awaitUntilFlush(senderEnv);

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

      try (final Connection connection = closeQuietly(senderEnv.getConnection());
          final Statement statement = closeQuietly(connection.createStatement()); ) {
        ResultSet result = statement.executeQuery("show pipes");
        await()
            .pollInSameThread()
            .pollDelay(1L, TimeUnit.SECONDS)
            .pollInterval(1L, TimeUnit.SECONDS)
            .atMost(600, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  try {
                    int pipeNum = 0;
                    while (result.next()) {
                      if (!result
                          .getString(ColumnHeaderConstant.ID)
                          .contains(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
                        pipeNum++;
                      }
                    }
                    Assert.assertEquals(0, pipeNum);
                  } catch (Exception e) {
                    Assert.fail();
                  }
                });
      }
    }
  }
}
