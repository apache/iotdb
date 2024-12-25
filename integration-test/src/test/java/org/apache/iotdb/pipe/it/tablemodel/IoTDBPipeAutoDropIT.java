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

package org.apache.iotdb.pipe.it.tablemodel;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2TableModel;
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
import java.util.function.Consumer;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateReliabilityITFramework.closeQuietly;
import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeAutoDropIT extends AbstractPipeTableModelTestIT {

  @Test
  public void testAutoDropInHistoricalTransfer() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    boolean insertResult = true;

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("mode.snapshot", "true");
      extractorAttributes.put("capture.table", "true");

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
          TableModelUtils.getQueryCountSql("test"),
          "_col0,",
          Collections.singleton("100,"),
          "test",
          handleFailure);
    }

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

  @Test
  public void testAutoDropInHistoricalTransferWithTimeRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    boolean insertResult = true;

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("mode.snapshot", "true");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("start-time", "0");
      extractorAttributes.put("end-time", "49");

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
          TableModelUtils.getQueryCountSql("test"),
          "_col0,",
          Collections.singleton("50,"),
          "test",
          handleFailure);
    }

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
