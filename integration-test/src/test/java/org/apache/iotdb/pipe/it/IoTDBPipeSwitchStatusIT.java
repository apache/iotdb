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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeSwitchStatusIT {

  private BaseEnv senderEnv;
  private BaseEnv receiverEnv;

  @Before
  public void setUp() throws Exception {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @After
  public void tearDown() {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }

  @Test
  public void testPipeSwitchStatus() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

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
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p2") && o.state.equals("STOPPED")));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p3") && o.state.equals("STOPPED")));

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
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

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
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));

      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.stopPipe("p1").getCode());
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
          TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));
      Assert.assertEquals(1, showPipeResult.stream().filter((o) -> o.id.equals("p1")).count());
    }
  }

  @Test
  public void testDropPipeAndCreateAgain() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (Connection connection = senderEnv.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("insert into root.sg1.d1(time, at1) values (1, 1)");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

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

      try (Connection connection = senderEnv.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("drop database root.**");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p1").getCode());
      showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(0, showPipeResult.size());

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
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

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
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("").getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("p0").getCode());
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("p").getCode());
      Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), client.startPipe("*").getCode());
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("STOPPED")));

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
