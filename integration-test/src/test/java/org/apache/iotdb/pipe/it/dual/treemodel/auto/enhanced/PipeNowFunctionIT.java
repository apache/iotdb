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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

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
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class PipeNowFunctionIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testPipeNowFunction() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> sourceAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.start-time", "now");
      sourceAttributes.put("source.end-time", "now");
      sourceAttributes.put("source.history.start-time", "now");
      sourceAttributes.put("source.history.end-time", "now");
      sourceAttributes.put("source.history.enable", "true");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      sourceAttributes.clear();
      sourceAttributes.put("start-time", "now");
      sourceAttributes.put("end-time", "now");
      sourceAttributes.put("history.start-time", "now");
      sourceAttributes.put("history.end-time", "now");
      sourceAttributes.put("history.enable", "true");
      sourceAttributes.put("user", "root");

      status =
          client.createPipe(
              new TCreatePipeReq("p2", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      sourceAttributes.clear();
      sourceAttributes.put("source.start-time", "now");
      sourceAttributes.put("source.end-time", "now");
      sourceAttributes.put("source.history.start-time", "now");
      sourceAttributes.put("source.history.end-time", "now");
      sourceAttributes.put("history.enable", "true");
      sourceAttributes.put("user", "root");

      status =
          client.createPipe(
              new TCreatePipeReq("p3", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      sourceAttributes.clear();
      sourceAttributes.put("source.start-time", "now");
      sourceAttributes.put("source.end-time", "now");
      sourceAttributes.put("source.history.start-time", "now");
      sourceAttributes.put("source.history.end-time", "now");
      sourceAttributes.put("user", "root");
      client.alterPipe(
          new TAlterPipeReq()
              .setPipeName("p1")
              .setExtractorAttributes(sourceAttributes)
              .setIsReplaceAllExtractorAttributes(false)
              .setProcessorAttributes(new HashMap<>())
              .setIsReplaceAllProcessorAttributes(false)
              .setConnectorAttributes(new HashMap<>())
              .setIsReplaceAllConnectorAttributes(false));

      showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertTrue(
          showPipeResult.stream().anyMatch((o) -> o.id.equals("p1") && o.state.equals("RUNNING")));

      sourceAttributes.clear();
      sourceAttributes.put("start-time", "now");
      sourceAttributes.put("end-time", "now");
      sourceAttributes.put("history.start-time", "now");
      sourceAttributes.put("history.end-time", "now");
      sourceAttributes.put("user", "root");
      client.alterPipe(
          new TAlterPipeReq()
              .setPipeName("p1")
              .setExtractorAttributes(sourceAttributes)
              .setIsReplaceAllExtractorAttributes(false)
              .setProcessorAttributes(new HashMap<>())
              .setIsReplaceAllProcessorAttributes(false)
              .setConnectorAttributes(new HashMap<>())
              .setIsReplaceAllConnectorAttributes(false));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertTrue(showPipeResult.stream().anyMatch((o) -> o.id.equals("p1")));
    }
  }

  @Test
  public void testTreeModeSQLSupportNowFunc() {
    doTest(BaseEnv.TREE_SQL_DIALECT);
  }

  private void doTest(String dialect) {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final String p1 =
        String.format(
            "create pipe p1"
                + " with source ("
                + "'source.history.enable'='true',"
                + "'source.start-time'='now',"
                + "'source.end-time'='now',"
                + "'source.history.start-time'='now',"
                + "'source.history.end-time'='now')"
                + " with sink ("
                + "'sink'='iotdb-thrift-sink',"
                + "'sink.ip'='%s',"
                + "'sink.port'='%s',"
                + "'sink.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection(dialect);
        final Statement statement = connection.createStatement()) {
      statement.execute(p1);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    final String p2 =
        String.format(
            "create pipe p2"
                + " with source ("
                + "'source.history.enable'='true',"
                + "'start-time'='now',"
                + "'end-time'='now',"
                + "'history.start-time'='now',"
                + "'history.end-time'='now')"
                + " with sink ("
                + "'sink'='iotdb-thrift-sink',"
                + "'sink.ip'='%s',"
                + "'sink.port'='%s',"
                + "'sink.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection(dialect);
        final Statement statement = connection.createStatement()) {
      statement.execute(p2);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    final String p3 =
        String.format(
            "create pipe p3"
                + " with source ("
                + "'source.history.enable'='true',"
                + "'source.start-time'='now',"
                + "'source.end-time'='now',"
                + "'source.history.start-time'='now',"
                + "'source.history.end-time'='now')"
                + " with sink ("
                + "'sink'='iotdb-thrift-sink',"
                + "'sink.ip'='%s',"
                + "'sink.port'='%s',"
                + "'sink.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection(dialect);
        final Statement statement = connection.createStatement()) {
      statement.execute(p3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    String alterP3 =
        "alter pipe p3"
            + " modify source ("
            + "'history.enable'='true',"
            + "'start-time'='now',"
            + "'end-time'='now',"
            + "'history.start-time'='now',"
            + "'history.end-time'='now')";
    try (final Connection connection = senderEnv.getConnection(dialect);
        final Statement statement = connection.createStatement()) {
      statement.execute(alterP3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    alterP3 =
        "alter pipe p3"
            + " modify source ("
            + "'source.history.enable'='true',"
            + "'source.start-time'='now',"
            + "'source.end-time'='now',"
            + "'source.history.start-time'='now',"
            + "'source.history.end-time'='now')";
    try (final Connection connection = senderEnv.getConnection(dialect);
        final Statement statement = connection.createStatement()) {
      statement.execute(alterP3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    alterP3 =
        "alter pipe p3"
            + " modify source ("
            + "'source.history.enable'='true',"
            + "'source.start-time'='now',"
            + "'source.end-time'='now',"
            + "'source.history.start-time'='now',"
            + "'source.history.end-time'='now')";
    try (final Connection connection = senderEnv.getConnection(dialect);
        final Statement statement = connection.createStatement()) {
      statement.execute(alterP3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }
  }
}
