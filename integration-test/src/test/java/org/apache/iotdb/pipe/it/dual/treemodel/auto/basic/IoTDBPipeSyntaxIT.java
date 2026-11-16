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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.ConfigNodePipePluginMetaKeeper;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeTreeModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.apache.tsfile.external.commons.lang3.SystemUtils;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

// These syntax scenarios create no data. Drop each method's pipes while retaining both clusters;
// the plugin restart scenario also restores its nodes and removes its custom plugin in finally.
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeSyntaxIT extends AbstractPipeTreeModelSharedClusterIT {

  // Legal identifiers, including quoted numbers and reserved words, must create and drop cleanly.
  @Test
  public void testValidPipeName() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {

      final List<String> validPipeNames =
          Arrays.asList("Pipe_1", "null", "`33`", "`root`", "中文", "with");
      final List<String> expectedPipeNames =
          Arrays.asList("Pipe_1", "null", "33", "root", "中文", "with");
      for (final String pipeName : validPipeNames) {
        try {
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
        try {
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

  // Reordering source and sink parameters must retain validation of the disabled history mode.
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

  // Putting the sink before the source must fail and leave no pipe registered.
  @Test
  public void testRevertStageOrder() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {

      try {
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

  // A sink is required; omitted source/processor stages default successfully when a sink is
  // present.
  @Test
  public void testMissingStage() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create pipe p1");
        fail();
      } catch (SQLException ignored) {
      }

      try {
        statement.execute("create pipe p2 with source ('source'='iotdb-source')");
        fail();
      } catch (SQLException ignored) {
      }

      try {
        statement.execute(
            "create pipe p3"
                + " with source ('source'='iotdb-source')"
                + " with processor ('processor'='do-nothing-processor')");
        fail();
      } catch (SQLException ignored) {
      }

      try {
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

      try {
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

  // Empty source/processor attributes are valid; unknown plugin names must not create a pipe.
  @Test
  public void testInvalidParameter() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      try {
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

      try {
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

      try {
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

      try {
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

  // Traversal paths, absolute paths and platform-reserved directory names must be rejected.
  @Test
  public void testDirectoryErrors() throws SQLException {
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      List<String> wrongDirs = Arrays.asList(".", "..", "/hackYou", "..\\hackYouTwice");
      if (SystemUtils.IS_OS_WINDOWS) {
        wrongDirs = new ArrayList<>(wrongDirs);
        wrongDirs.add("BombWindows/:*?");
        wrongDirs.add("AUX");
      }
      for (final String name : wrongDirs) {
        testDirectoryError(name, statement);
      }
    }
  }

  private void testDirectoryError(final String wrongDir, final Statement statement) {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try {
      statement.execute(
          String.format(
              "create pipe `"
                  + wrongDir
                  + "` with source ()"
                  + " with processor ()"
                  + " with sink ("
                  + "'sink'='invalid-param',"
                  + "'sink.ip'='%s',"
                  + "'sink.port'='%s',"
                  + "'sink.batch.enable'='false')",
              receiverIp,
              receiverPort));
      fail();
    } catch (final Exception ignore) {
      // Expected
    }

    try {
      statement.execute(
          String.format(
              "create pipePlugin `"
                  + wrongDir
                  + "` as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI '%s'",
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
      Assert.assertTrue(e.getMessage().contains("701: Failed to create pipe plugin"));
    }
  }

  // Single and double quoted stage attributes are valid; bare and backtick quoted values fail.
  @Test
  public void testBrackets() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

      try {
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

  // SHOW PIPE must filter by name or shared sink, excluding a pipe with different batching
  // settings.
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

  // Empty or unknown inclusion/exclusion sets must fail; a supported nonempty selection succeeds.
  @Test
  public void testInclusionPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      // Empty inclusion
      try {
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
      try {
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
      try {
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
      try {
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

  // The compact CREATE PIPE form must accept a sink without an explicit WITH SINK clause.
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

  // Plugin URI validation and drops of absent/built-in plugins must preserve their specific errors.
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
            "701: Failed to get executable for PipePlugin TestProcessor, please check the URI.",
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

  // Removing an installed plugin jar and restarting must retain the plugin with a visible error.
  @Test
  public void testShowPipePluginAfterJarDeletedAndClusterRestart() throws Exception {
    final String pluginName = "TEST_MISSING_JAR_PROCESSOR";
    final String pluginClassName = "org.apache.iotdb.CountPointProcessor";
    final Path pluginJarPath =
        Paths.get(
                System.getProperty("user.dir"),
                "src",
                "test",
                "resources",
                "pipe-count-point-processor-example.jar")
            .toAbsolutePath();
    System.out.println(pluginJarPath.toUri());

    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipePlugin %s as '%s' USING URI '%s'",
              pluginName, pluginClassName, pluginJarPath.toUri()));
    }

    try {
      takeConfigNodeSnapshots();
      awaitConfigNodeSnapshots(pluginName);
      senderEnv.shutdownAllDataNodes();
      senderEnv.shutdownAllConfigNodes();

      Awaitility.await()
          .pollInSameThread()
          .pollDelay(0, TimeUnit.MILLISECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try {
                  deletePluginJarUnderConfigNodes(pluginName);
                } catch (IOException e) {
                  // Windows can keep a just-stopped plugin class loader open briefly.
                  throw new AssertionError("Plugin jar is still locked after cluster shutdown", e);
                }
              });

      senderEnv.startAllConfigNodes();
      senderEnv.startAllDataNodes();
      ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();

      Awaitility.await()
          .pollInSameThread()
          .pollDelay(0, TimeUnit.MILLISECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                boolean pluginFound = false;
                boolean exceptionMessageFound = false;
                try (final Connection connection = senderEnv.getConnection();
                    final Statement statement = connection.createStatement();
                    final ResultSet resultSet = statement.executeQuery("show pipeplugins")) {
                  while (resultSet.next()) {
                    if (pluginName.equalsIgnoreCase(resultSet.getString("PluginName"))) {
                      pluginFound = true;
                      final String exceptionMessage = resultSet.getString("ExceptionMessage");
                      exceptionMessageFound =
                          exceptionMessage != null && !exceptionMessage.trim().isEmpty();
                      break;
                    }
                  }
                } catch (final SQLException e) {
                  // Only reconnecting and reading metadata is retried after the explicit restart.
                  throw new AssertionError("Failed to query plugins after cluster restart", e);
                }
                Assert.assertTrue("Expected plugin in show pipe plugins result.", pluginFound);
                Assert.assertTrue(
                    "Expected non-empty ExceptionMessage after deleting plugin jar and restarting cluster.",
                    exceptionMessageFound);
              });
    } finally {
      // Keep the shared cluster usable even if jar removal or a restart assertion fails.
      for (final ConfigNodeWrapper node : senderEnv.getConfigNodeWrapperList()) {
        if (!node.isAlive()) {
          node.start();
        }
      }
      for (final DataNodeWrapper node : senderEnv.getDataNodeWrapperList()) {
        if (!node.isAlive()) {
          node.start();
        }
      }
      ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(String.format("drop pipePlugin if exists %s", pluginName));
      }
    }
  }

  private void takeConfigNodeSnapshots() throws Exception {
    final List<TConfigNodeLocation> locations;
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      locations = client.showCluster().getConfigNodeList();
    }
    final List<RaftPeer> peers = new ArrayList<>();
    for (final TConfigNodeLocation location : locations) {
      peers.add(
          Utils.fromNodeInfoAndPriorityToRaftPeer(
              location.getConfigNodeId(), location.getConsensusEndPoint(), 0));
    }
    final RaftProperties properties = new RaftProperties();
    RaftClientConfigKeys.Rpc.setRequestTimeout(
        properties, TimeDuration.valueOf(30, TimeUnit.SECONDS));
    // Snapshot only after plugin creation has released its lock. An automatic snapshot between
    // procedure steps can block the state-machine updater on that lock while the procedure waits
    // for its next consensus write. Persisting here also prevents log replay from restoring the
    // jar.
    try (final RaftClient client =
        RaftClient.newBuilder()
            .setRaftGroup(
                RaftGroup.valueOf(
                    Utils.fromConsensusGroupIdToRaftGroupId(new ConfigRegionId(0)), peers))
            .setProperties(properties)
            .setRetryPolicy(RetryPolicies.noRetry())
            .build()) {
      for (final RaftPeer peer : peers) {
        final RaftClientReply reply =
            client.getSnapshotManagementApi(peer.getId()).create(true, 30_000);
        Assert.assertTrue("Failed to snapshot ConfigNode: " + reply, reply.isSuccess());
      }
    }
  }

  private void awaitConfigNodeSnapshots(final String pluginName) {
    for (final ConfigNodeWrapper configNode : senderEnv.getConfigNodeWrapperList()) {
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(0, TimeUnit.MILLISECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> snapshotContainsPlugin(configNode, pluginName));
    }
  }

  private boolean snapshotContainsPlugin(
      final ConfigNodeWrapper configNode, final String pluginName) throws IOException {
    final Path consensusPath =
        Paths.get(configNode.getNodePath(), "data", "confignode", "consensus");
    try (final Stream<Path> paths = Files.walk(consensusPath, 3)) {
      for (final Path snapshot :
          paths
              .filter(
                  path ->
                      path.getFileName().toString().matches("\\d+_\\d+")
                          && path.getParent().getFileName().toString().equals("sm"))
              .toList()) {
        final Path pluginMetaPath = snapshot.resolve("pipe_plugin_info.bin");
        if (!Files.exists(pluginMetaPath)) {
          continue;
        }
        try (final InputStream input = Files.newInputStream(pluginMetaPath)) {
          final ConfigNodePipePluginMetaKeeper metaKeeper = new ConfigNodePipePluginMetaKeeper();
          metaKeeper.processLoadSnapshot(input);
          if (metaKeeper.containsPipePlugin(pluginName)) {
            return true;
          }
        } catch (final IOException ignored) {
          // Ratis may replace a snapshot while the directory is being inspected.
        }
      }
    }
    return false;
  }

  private void deletePluginJarUnderConfigNodes(final String pluginName) throws IOException {
    for (final ConfigNodeWrapper configNodeWrapper : senderEnv.getConfigNodeWrapperList()) {
      final Path pluginJarDirPath =
          Paths.get(
              configNodeWrapper.getNodePath(), "ext", "pipe", "install", pluginName.toUpperCase());
      if (!Files.exists(pluginJarDirPath)) {
        continue;
      }
      try (final Stream<Path> children = Files.walk(pluginJarDirPath)) {
        for (final Path path :
            children
                .filter(path -> !path.equals(pluginJarDirPath))
                .sorted(Comparator.reverseOrder())
                .toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }
}
