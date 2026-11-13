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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeTreeModelSharedClusterIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeConditionalOperationsIT extends AbstractPipeTreeModelSharedClusterIT {

  // Repeating CREATE PIPE IF NOT EXISTS with different source attributes must preserve the
  // running pipe's original configuration and creation time.
  @Test
  public void testBasicCreatePipeIfNotExists() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create pipe
    String sql =
        String.format(
            "create pipe If Not Exists a2b with source ('source'='iotdb-source', 'source.pattern'='root.test1', 'source.realtime.mode'='stream') with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    // Keep both clients across the repeated command and its metadata checks.
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      statement.execute(sql);

      // show pipe
      final List<TShowPipeInfo> createdPipes =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      createdPipes.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, createdPipes.size());
      // Check status
      Assert.assertEquals("RUNNING", createdPipes.get(0).state);
      // Check configurations
      Assert.assertTrue(createdPipes.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(createdPipes.get(0).pipeExtractor.contains("source.pattern=root.test1"));
      Assert.assertTrue(createdPipes.get(0).pipeExtractor.contains("source.realtime.mode=stream"));
      Assert.assertTrue(
          createdPipes.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          createdPipes
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Record last creation time
      final long creationTime = createdPipes.get(0).creationTime;

      // Create pipe If Not Exists
      sql =
          String.format(
              "create pipe If Not Exists a2b with source ('source'='iotdb-source', 'source.path'='root.test2.**') with sink ('node-urls'='%s')",
              receiverDataNode.getIpAndPortString());
      statement.execute(sql);

      // show pipe
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source.pattern=root.test1"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeExtractor.contains("source.realtime.mode=stream"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test2.**"));
      Assert.assertEquals(creationTime, showPipeResult.get(0).creationTime);
    }
  }

  // DROP PIPE IF EXISTS must remove a running pipe and succeed again after the pipe is absent.
  @Test
  public void testBasicDropPipeIfExists() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create pipe
    final String sql =
        String.format(
            "create pipe If Not Exists a2b with source ('source'='iotdb-source', 'source.path'='root.test1.**') with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    // Reuse the SQL connection through both DROP calls and keep the intervening absence check.
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      statement.execute(sql);

      // Drop pipe If Exists
      statement.execute("Drop pipe If Exists a2b");

      // show pipe
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(0, showPipeResult.size());

      // Drop pipe If Exists
      statement.execute("Drop pipe If Exists a2b");
    }
  }

  public void testBasicAlterPipeIfExists() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Alter pipe If Exists
    String sql =
        String.format(
            "Alter pipe If Exists a2b replace sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(0, showPipeResult.size());
    }

    // Create pipe
    sql =
        String.format(
            "create pipe If Not Exists a2b with source ('source'='iotdb-source', 'source.pattern'='root.test1', 'source.realtime.mode'='stream') with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Alter pipe If Exists
    sql =
        String.format(
            "Alter pipe If Exists a2b replace source () replace processor () replace sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source.pattern=root.test1"));
      Assert.assertFalse(
          showPipeResult.get(0).pipeExtractor.contains("source.realtime.mode=stream"));
      Assert.assertFalse(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
    }
  }
}
