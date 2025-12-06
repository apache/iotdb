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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeSyntaxIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testValidPipeName() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final List<String> validPipeNames =
          Arrays.asList("Pipe_1", "null", "`33`", "`root`", "中文", "with");
      final List<String> expectedPipeNames =
          Arrays.asList("Pipe_1", "null", "33", "root", "中文", "with");
      for (final String pipeName : validPipeNames) {
        try (final Connection connection = senderEnv.getConnection();
            final Statement statement = connection.createStatement()) {
          statement.execute(
              String.format(
                  "create pipe %s"
                      + " with sink ("
                      + "'sink'='iotdb-thrift-sink',"
                      + "'sink.ip'='%s',"
                      + "'sink.port'='%s',"
                      + "'sink.batch.enable'='false')",
                  pipeName, receiverIp, receiverPort));
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }

      List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      for (final String pipeName : expectedPipeNames) {
        Assert.assertTrue(
            showPipeResult.stream()
                .anyMatch((o) -> o.id.equals(pipeName) && o.state.equals("RUNNING")));
      }

      for (final String pipeName : validPipeNames) {
        try (final Connection connection = senderEnv.getConnection();
            final Statement statement = connection.createStatement()) {
          statement.execute(String.format("drop pipe %s", pipeName));
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }

      showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(0, showPipeResult.size());
    }
  }

  @Test
  public void testRevertParameterOrder() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe p1"
                  + " with source ("
                  + "'source.realtime.mode'='hybrid',"
                  + "'source.history.enable'='false') "
                  + " with sink ("
                  + "'sink.batch.enable'='false', "
                  + "'sink.port'='%s',"
                  + "'sink.ip'='%s',"
                  + "'sink'='iotdb-thrift-sink')",
              receiverIp, receiverPort));
      fail();
    } catch (SQLException ignore) {
    }
  }

  @Test
  public void testRevertStageOrder() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p1"
                    + " with sink ("
                    + "'sink.batch.enable'='false', "
                    + "'sink.port'='%s',"
                    + "'sink.ip'='%s',"
                    + "'sink'='iotdb-thrift-sink') "
                    + " with source ("
                    + "'source.realtime.mode'='hybrid',"
                    + "'source.history.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(0, showPipeResult.size());
    }
  }

  @Test
  public void testMissingStage() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute("create pipe p1");
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute("create pipe p2 with source ('source'='iotdb-source')");
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            "create pipe p3"
                + " with source ('source'='iotdb-source')"
                + " with processor ('processor'='do-nothing-processor')");
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p4"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p5"
                    + " with source ('source'='iotdb-source')"
                    + " with processor ('processor'='do-nothing-processor')"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(2, showPipeResult.size());
    }
  }

  @Test
  public void testInvalidParameter() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p1"
                    + " with source ()"
                    + " with processor ()"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p2"
                    + " with source ('source'='invalid-param')"
                    + " with processor ()"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p3"
                    + " with source ()"
                    + " with processor ('processor'='invalid-param')"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p4"
                    + " with source ()"
                    + " with processor ()"
                    + " with sink ("
                    + "'sink'='invalid-param',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
    }
  }

  @Test
  public void testBrackets() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe source1"
                    + " with source ('source'='iotdb-source')"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe source2"
                    + " with source (\"source\"=\"iotdb-source\")"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe source3"
                    + " with source ('source'=\"iotdb-source\")"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe source4"
                    + " with source (source=iotdb-source)"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe source5"
                    + " with source ('source'=`iotdb-source`)"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe processor1"
                    + " with processor ('processor'='do-nothing-processor')"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe processor2"
                    + " with processor (\"processor\"=\"do-nothing-processor\")"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe processor3"
                    + " with processor ('processor'=\"do-nothing-processor\")"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe processor4"
                    + " with processor (processor=do-nothing-processor)"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe processor5"
                    + " with processor ('processor'=`do-nothing-processor`)"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe sink1"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe sink2"
                    + " with sink ("
                    + "\"sink\"=\"iotdb-thrift-sink\","
                    + "\"sink.ip\"=\"%s\","
                    + "\"sink.port\"=\"%s\","
                    + "\"sink.batch.enable\"=\"false\")",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe sink3"
                    + " with sink ("
                    + "'sink'=\"iotdb-thrift-sink\","
                    + "\"sink.ip\"='%s',"
                    + "'sink.port'=\"%s\","
                    + "\"sink.batch.enable\"='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe sink4"
                    + " with sink ("
                    + "sink=iotdb-thrift-sink,"
                    + "sink.ip=%s,"
                    + "sink.port=%s,"
                    + "sink.batch.enable=false)",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe sink5"
                    + " with sink ("
                    + "'sink'=`iotdb-thrift-sink`,"
                    + "'sink.ip'=`%s`,"
                    + "'sink.port'=`%s`,"
                    + "'sink.batch.enable'=`false`)",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(9, showPipeResult.size());
    }
  }

  @Test
  public void testShowPipeWithWrongPipeName() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

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

      status =
          client.createPipe(
              new TCreatePipeReq("p2", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      sinkAttributes.replace("sink.batch.enable", "true");

      status =
          client.createPipe(
              new TCreatePipeReq("p3", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(3, showPipeResult.size());

      showPipeResult = client.showPipe(new TShowPipeReq().setPipeName("p1")).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertTrue(showPipeResult.stream().anyMatch((o) -> o.id.equals("p1")));
      Assert.assertFalse(showPipeResult.stream().anyMatch((o) -> o.id.equals("p2")));
      Assert.assertFalse(showPipeResult.stream().anyMatch((o) -> o.id.equals("p3")));

      // Show all pipes whose sink is also used by p1.
      // p1 and p2 share the same sink parameters, so they have the same sink.
      showPipeResult =
          client.showPipe(new TShowPipeReq().setPipeName("p1").setWhereClause(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertTrue(showPipeResult.stream().anyMatch((o) -> o.id.equals("p1")));
      Assert.assertTrue(showPipeResult.stream().anyMatch((o) -> o.id.equals("p2")));
      Assert.assertFalse(showPipeResult.stream().anyMatch((o) -> o.id.equals("p3")));
    }
  }

  @Test
  public void testInclusionPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Empty inclusion
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p2"
                    + " with source ('source.inclusion'='schema, auth.role', 'source.inclusion.exclusion'='all')"
                    + " with processor ()"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      // Invalid inclusion
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p3"
                    + " with source ('source.inclusion'='wrong')"
                    + " with processor ()"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      // Invalid exclusion
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p4"
                    + " with source ('source.inclusion.exclusion'='wrong')"
                    + " with processor ()"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException ignored) {
      }

      // Valid
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p4"
                    + " with source ('source.inclusion'='all', 'source.inclusion.exclusion'='schema.database.drop, auth.role')"
                    + " with processor ()"
                    + " with sink ("
                    + "'sink'='iotdb-thrift-sink',"
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.batch.enable'='false')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
    }
  }

  @Test
  public void testValidPipeWithoutWithSink() {
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create pipe p1('sink'='do-nothing-sink')");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testPipePluginValidation() {
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'xxx'");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            "701: Untrusted uri xxx, current trusted_uri_pattern is file:.*", e.getMessage());
      }
      try {
        statement.execute(
            "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'file:.*'");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals("701: URI is not hierarchical", e.getMessage());
      }
      try {
        statement.execute(
            String.format(
                "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI '%s'",
                new File(
                            System.getProperty("user.dir")
                                + File.separator
                                + "target"
                                + File.separator
                                + "test-classes"
                                + File.separator)
                        .toURI()
                    + "PipePlugin.jar"));
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            "1603: Failed to get executable for PipePlugin TestProcessor, please check the URI.",
            e.getMessage());
      }
      try {
        statement.execute("drop pipePlugin test_processor");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            "1601: Failed to drop pipe plugin TEST_PROCESSOR. Failures: TEST_PROCESSOR does not exist.",
            e.getMessage());
      }
      try {
        statement.execute("drop pipePlugin `Do-Nothing-Sink`");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            "1601: Failed to drop PipePlugin [DO-NOTHING-SINK], the PipePlugin is a built-in PipePlugin",
            e.getMessage());
      }
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
