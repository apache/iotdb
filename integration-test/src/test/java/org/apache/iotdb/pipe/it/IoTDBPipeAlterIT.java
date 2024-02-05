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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;

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
@Category({MultiClusterIT2.class})
public class IoTDBPipeAlterIT extends AbstractPipeDualIT {

  @Test
  public void testBasicPipeAlter() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // create pipe
    String sql =
        String.format(
            "create pipe a2b with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe and record first creation time
    long firstCreationTime;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=false"));
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      firstCreationTime = showPipeResult.get(0).creationTime;
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
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
    }

    // alter pipe
    sql =
        String.format(
            "alter pipe a2b modify sink ('node-urls'='%s', 'batch.enable'='true')",
            receiverDataNode.getIpAndPortString());
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // alter pipe will reset pipe creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > firstCreationTime);
      // alter pipe will clear exception messages
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }
  }

  @Test
  public void testPipeAlterFailure() {
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

    // useless alter
    sql =
        String.format(
            "alter pipe a2b modify sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      fail();
    } catch (SQLException ignore) {
    }
  }
}
