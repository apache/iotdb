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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeAlterIT extends AbstractPipeDualAutoIT {

  @Test
  public void testBasicAlterPipe() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create pipe
    final String sql =
        String.format(
            "create pipe a2b with source ('source'='iotdb-source', 'source.pattern'='root.test1', 'source.realtime.mode'='stream') with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    long lastCreationTime;
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
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
      // Record last creation time
      lastCreationTime = showPipeResult.get(0).creationTime;
    }

    // Stop pipe
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("stop pipe a2b");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
    }

    // Alter pipe (modify)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('source.pattern'='root.test2')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source.pattern=root.test2"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeExtractor.contains("source.realtime.mode=stream"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (replace)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b replace source ('source'='iotdb-source', 'source.path'='root.test1.**')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test1.**"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source.pattern=root.test2"));
      Assert.assertFalse(
          showPipeResult.get(0).pipeExtractor.contains("source.realtime.mode=stream"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (modify)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('sink.batch.enable'='false')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test1.**"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=false"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Start pipe
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("start pipe a2b");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Alter pipe (replace)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b replace processor ('processor'='tumbling-time-sampling-processor')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test1.**"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeProcessor
              .contains("processor=tumbling-time-sampling-processor"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=false"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (modify)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('connector.batch.enable'='true')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test1.**"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeProcessor
              .contains("processor=tumbling-time-sampling-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (modify empty)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test1.**"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeProcessor
              .contains("processor=tumbling-time-sampling-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (replace empty)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b replace source ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test1.**"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeProcessor
              .contains("processor=tumbling-time-sampling-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (replace empty)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b replace processor ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source.path=root.test1.**"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertFalse(
          showPipeResult
              .get(0)
              .pipeProcessor
              .contains("processor=tumbling-time-sampling-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (modify empty)
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertFalse(
          showPipeResult
              .get(0)
              .pipeProcessor
              .contains("processor=tumbling-time-sampling-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }
  }

  @Test
  public void testAlterPipeFailure() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // alter non-existed pipe
    String sql =
        String.format(
            "alter pipe a2b modify sink ('node-urls'='%s', 'batch.enable'='true')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
      fail();
    } catch (SQLException ignore) {
    }

    // Create pipe
    sql =
        String.format(
            "create pipe a2b with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlterPipeSourceAndProcessor() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create pipe
    final String sql =
        String.format(
            "create pipe a2b with source ('source' = 'iotdb-source','source.path' = 'root.db.d1.**') with processor ('processor'='tumbling-time-sampling-processor', 'processor.tumbling-time.interval-seconds'='1', 'processor.down-sampling.split-file'='true') with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Insert data on sender
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            "insert into root.db.d1 (time, at1) values (1000, 1), (1500, 2), (2000, 3), (2500, 4), (3000, 5)",
            "flush"))) {
      fail();
    }

    // Check data on receiver
    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1000,1.0,");
    expectedResSet.add("2000,3.0,");
    expectedResSet.add("3000,5.0,");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.db.**", "Time,root.db.d1.at1,", expectedResSet);

    // Alter pipe (modify 'source.path' and 'processor.tumbling-time.interval-seconds')
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b modify source('source' = 'iotdb-source','source.path'='root.db.d2.**') modify processor ('processor.tumbling-time.interval-seconds'='2')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Insert data on sender
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            "insert into root.db.d2 (time, at1) values (11000, 1), (11500, 2), (12000, 3), (12500, 4), (13000, 5)",
            "flush"))) {
      fail();
    }

    // Insert data on sender
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            "insert into root.db.d1 (time, at1) values (11000, 1), (11500, 2), (12000, 3), (12500, 4), (13000, 5)",
            "flush"))) {
      fail();
    }

    // Check data on receiver
    expectedResSet.clear();
    expectedResSet.add("11000,null,1.0,");
    expectedResSet.add("13000,null,5.0,");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.db.** where time > 10000",
        "Time,root.db.d1.at1,root.db.d2.at1,",
        expectedResSet);
  }
}
