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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeDemoIT {

  BaseEnv sender_env;
  BaseEnv receiver_env;

  @Before
  public void setUp() throws Exception {
    MultiEnvFactory.createEnv(2);
    sender_env = MultiEnvFactory.getEnv(0);
    receiver_env = MultiEnvFactory.getEnv(1);

    sender_env.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    receiver_env.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

    sender_env.initClusterEnvironment();
    receiver_env.initClusterEnvironment();
  }

  @After
  public void tearDown() {
    sender_env.cleanClusterEnvironment();
    receiver_env.cleanClusterEnvironment();
  }

  @Test
  public void testEnv() throws Exception {
    DataNodeWrapper receiverDataNode = receiver_env.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) sender_env.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.realtime.mode", "log");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      try (Connection connection = sender_env.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("insert into root.vehicle.d0(time, s1) values (0, 1)");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (Connection connection = receiver_env.getConnection();
          Statement statement = connection.createStatement()) {
        await()
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.assertResultSetEqual(
                        statement.executeQuery("select * from root.**"),
                        "Time,root.vehicle.d0.s1,",
                        Collections.singleton("0,1.0,")));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }
}
