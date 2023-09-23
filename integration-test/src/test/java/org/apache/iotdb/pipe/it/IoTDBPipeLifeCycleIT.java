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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeLifeCycleIT {

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
  public void testLifeCycleWithHistoryEnabled() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

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

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleWithHistoryDisabled() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.history.enable", "false");
      // start-time and end-time should not work
      extractorAttributes.put("extractor.history.start-time", "0001.01.01T00:00:00");
      extractorAttributes.put("extractor.history.end-time", "2100.01.01T00:00:00");

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

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleLogMode() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "log");

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

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleFileMode() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "file");

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

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleHybridMode() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "hybrid");

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

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleWithClusterRestart() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    Set<String> expectedResSet = new HashSet<>();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

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

      expectedResSet.add("1,1.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }

    TestUtils.restartCluster(senderEnv);
    TestUtils.restartCluster(receiverEnv);

    try (SyncConfigNodeIServiceClient ignored =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testReceiverRestartWhenTransferring() throws Exception {
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
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      Thread t =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < 100; ++i) {
                    TestUtils.executeNonQueryWithRetry(
                        senderEnv,
                        String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
                    Thread.sleep(100);
                  }
                } catch (InterruptedException ignored) {
                }
              });
      t.start();

      TestUtils.restartCluster(receiverEnv);
      t.join();

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));
    }
  }

  @Test
  public void testReceiverAlreadyHaveTimeSeries() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueryWithRetry(
          receiverEnv, "insert into root.db.d1(time, s1) values (1, 1)");

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

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      Thread.sleep(5000);
      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  @Test
  public void testDoubleLiving() throws Exception {
    // Double living is two clusters with pipes connecting each other.
    DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String senderIp = senderDataNode.getIp();
    int senderPort = senderDataNode.getPort();
    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    for (int i = 0; i < 100; ++i) {
      TestUtils.executeNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
    }
    TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

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
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    for (int i = 100; i < 200; ++i) {
      TestUtils.executeNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
    }

    for (int i = 200; i < 300; ++i) {
      TestUtils.executeNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
    }
    TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderIp);
      connectorAttributes.put("connector.port", Integer.toString(senderPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    for (int i = 300; i < 400; ++i) {
      TestUtils.executeNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
    }

    Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

    TestUtils.restartCluster(senderEnv);
    TestUtils.restartCluster(receiverEnv);

    for (int i = 400; i < 500; ++i) {
      TestUtils.executeNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
    }
    TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
    for (int i = 500; i < 600; ++i) {
      TestUtils.executeNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
    }
    TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");

    for (int i = 400; i < 600; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
  }
}
