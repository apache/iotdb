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
import org.apache.iotdb.db.it.utils.TestUtils;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeConnectorParallelIT {
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
  public void testIoTConnectorParallel() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    Set<String> expectedResSet = new HashSet<>();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.parallel.tasks", "3");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      try (Connection connection = senderEnv.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("insert into root.sg1.d1(time, s1) values (0, 1)");
        statement.execute("insert into root.sg1.d1(time, s1) values (1, 2)");
        statement.execute("insert into root.sg1.d1(time, s1) values (2, 3)");
        statement.execute("insert into root.sg1.d1(time, s1) values (3, 4)");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      expectedResSet.add("0,1.0,");
      expectedResSet.add("1,2.0,");
      expectedResSet.add("2,3.0,");
      expectedResSet.add("3,4.0,");
      assertDataOnReceiver(receiverEnv, expectedResSet);
      assertDataOnReceiver(receiverEnv, expectedResSet);
    }
  }

  private void assertDataOnReceiver(BaseEnv receiverEnv, Set<String> expectedResSet) {
    try (Connection connection = receiverEnv.getConnection();
        Statement statement = connection.createStatement()) {
      await()
          .atMost(600, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  TestUtils.assertResultSetEqual(
                      statement.executeQuery("select * from root.**"),
                      "Time,root.sg1.d1.s1,",
                      expectedResSet));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
