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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeSwitchStatusIT extends AbstractPipeDualAutoIT {
  @Test
  public void testPipeSwitchStatus() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      status =
          client.createPipe(
              new TCreatePipeReq("p3", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p2") && o.state.equals("RUNNING")));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p3") && o.state.equals("RUNNING")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p3").getCode());

      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p2") && o.state.equals("RUNNING")));
      Assert.assertFalse(showPipeResult.stream().anyMatch((o) -> o.id.equals("p3")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p2").getCode());

      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));
      Assert.assertFalse(showPipeResult.stream().anyMatch((o) -> o.id.equals("p2")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p1").getCode());

      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertFalse(showPipeResult.stream().anyMatch((o) -> o.id.equals("p1")));
    }
  }

  @Test
  public void testPipeIllegallySwitchStatus() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), status.getCode());

      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.stream().filter((o) -> o.id.equals("p1")).count());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));
      Assert.assertEquals(1, showPipeResult.stream().filter((o) -> o.id.equals("p1")).count());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));
      Assert.assertEquals(1, showPipeResult.stream().filter((o) -> o.id.equals("p1")).count());
    }
  }

  @Test
  public void testDropPipeAndCreateAgain() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "drop database root.**")) {
        return;
      }

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(
          0,
          showPipeResult.stream()
              .filter(info -> !info.id.startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX))
              .count());

      status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.stream().filter((o) -> o.id.equals("p1")).count());
    }
  }

  @Test
  public void testWrongPipeName() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

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
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("").getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("p0").getCode());
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("p").getCode());
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("*").getCode());
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.stopPipe("").getCode());
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.stopPipe("p0").getCode());
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.stopPipe("p").getCode());
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.stopPipe("*").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));

      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(), client.dropPipe("").getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(), client.dropPipe("p0").getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(), client.dropPipe("p").getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(), client.dropPipe("*").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertFalse(showPipeResult.stream().anyMatch((o) -> o.id.equals("p1")));
    }
  }
}
