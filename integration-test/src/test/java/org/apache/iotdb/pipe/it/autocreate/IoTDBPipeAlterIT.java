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
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // create pipe
    String sql =
        String.format(
            "create pipe a2b with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    long lastCreationTime;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // record last creation time
      lastCreationTime = showPipeResult.get(0).creationTime;
    }

    // stop pipe
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("stop pipe a2b");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
    }

    // alter pipe (modify)
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('sink.batch.enable'='false')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=false"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // start pipe
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("start pipe a2b");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // alter pipe (replace)
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b replace processor ('processor'='tumbling-time-sampling-processor')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
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
      // check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // alter pipe (modify)
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('connector.batch.enable'='true')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
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
      // check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // alter pipe (replace empty)
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b replace processor ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
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
      // check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // alter pipe (modify empty)
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
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
      // check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }
  }

  @Test
  public void testAlterPipeFailure() {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // alter non-existed pipe
    String sql =
        String.format(
            "alter pipe a2b modify sink ('node-urls'='%s', 'batch.enable'='true')",
            receiverDataNode.getIpAndPortString());
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      fail();
    } catch (SQLException ignore) {
    }

    // create pipe
    sql =
        String.format(
            "create pipe a2b with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlterPipeProcessor() {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // create pipe
    String sql =
        String.format(
            "create pipe a2b with processor ('processor'='tumbling-time-sampling-processor', 'processor.tumbling-time.interval-seconds'='1', 'processor.down-sampling.split-file'='true') with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // insert data on sender
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            "insert into root.db.d1 (time, at1) values (1000, 1), (1500, 2), (2000, 3), (2500, 4), (3000, 5)",
            "flush"))) {
      fail();
    }

    // check data on receiver
    Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1000,1.0,");
    expectedResSet.add("2000,3.0,");
    expectedResSet.add("3000,5.0,");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.at1,", expectedResSet);

    // alter pipe (modify 'processor.tumbling-time.interval-seconds')
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b modify processor ('processor.tumbling-time.interval-seconds'='2')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // insert data on sender
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            "insert into root.db.d1 (time, at1) values (11000, 1), (11500, 2), (12000, 3), (12500, 4), (13000, 5)",
            "flush"))) {
      fail();
    }

    // check data on receiver
    expectedResSet.clear();
    expectedResSet.add("11000,1.0,");
    expectedResSet.add("13000,5.0,");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.** where time > 10000",
        "Time,root.db.d1.at1,",
        expectedResSet);
  }
}
