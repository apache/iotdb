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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeQueryWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;
import static org.apache.iotdb.db.it.utils.TestUtils.tryExecuteNonQueriesWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.tryExecuteNonQueryWithRetry;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeLifeCycleIT extends AbstractPipeDualIT {
  @Test
  public void testLifeCycleWithHistoryEnabled() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)")) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"))) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)")) {
        return;
      }

      Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)")) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)")) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)")) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)")) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    try (SyncConfigNodeIServiceClient ignored =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)")) {
        return;
      }

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

      final AtomicInteger succeedNum = new AtomicInteger(0);
      Thread t =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < 100; ++i) {
                    if (TestUtils.tryExecuteNonQueryWithRetry(
                        senderEnv,
                        String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
                      succeedNum.incrementAndGet();
                    }
                    Thread.sleep(100);
                  }
                } catch (InterruptedException ignored) {
                }
              });
      t.start();

      try {
        TestUtils.restartCluster(receiverEnv);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      t.join();

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton(succeedNum.get() + ","));
    }
  }

  @Test
  public void testReceiverAlreadyHaveTimeSeries() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)")) {
        return;
      }

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)")) {
        return;
      }

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
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      // add this property to avoid to make self cycle.
      connectorAttributes.put("source.forwarding-pipe-requests", "false");
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
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }

    for (int i = 200; i < 300; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      // add this property to avoid to make self cycle.
      connectorAttributes.put("source.forwarding-pipe-requests", "false");
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
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }

    Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    for (int i = 400; i < 500; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }
    for (int i = 500; i < 600; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    for (int i = 400; i < 600; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
  }

  @Test
  public void testPermission() {
    createUser(senderEnv, "test", "test123");

    assertNonQueryTestFail(
        senderEnv,
        "create pipe testPipe\n"
            + "with connector (\n"
            + "  'connector'='iotdb-thrift-connector',\n"
            + "  'connector.ip'='127.0.0.1',\n"
            + "  'connector.port'='6668'\n"
            + ")",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");
    assertNonQueryTestFail(
        senderEnv,
        "drop pipe testPipe",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");
    assertTestFail(
        senderEnv,
        "show pipes",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");
    assertNonQueryTestFail(
        senderEnv,
        "start pipe testPipe",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");
    assertNonQueryTestFail(
        senderEnv,
        "stop pipe testPipe",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");

    assertNonQueryTestFail(
        senderEnv,
        "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'xxx'",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");
    assertNonQueryTestFail(
        senderEnv,
        "drop pipePlugin TestProcessor",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");
    assertTestFail(
        senderEnv,
        "show pipe plugins",
        "803: No permissions for this operation, please add privilege USE_PIPE",
        "test",
        "test123");

    grantUserSystemPrivileges(senderEnv, "test", PrivilegeType.USE_PIPE);

    tryExecuteNonQueryWithRetry(
        senderEnv,
        "create pipe testPipe\n"
            + "with connector (\n"
            + "  'connector'='iotdb-thrift-connector',\n"
            + "  'connector.ip'='127.0.0.1',\n"
            + "  'connector.port'='6668'\n"
            + ")",
        "test",
        "test123");
    executeQueryWithRetry(senderEnv, "show pipes", "test", "test123");
    tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList("start pipe testPipe", "stop pipe testPipe", "drop pipe testPipe"),
        "test",
        "test123");

    assertNonQueryTestFail(
        senderEnv,
        "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'xxx'",
        "1603: The scheme of URI is not set, please specify the scheme of URI.",
        "test",
        "test123");
    tryExecuteNonQueryWithRetry(senderEnv, "drop pipePlugin TestProcessor", "test", "test123");
    executeQueryWithRetry(senderEnv, "show pipe plugins", "test", "test123");
  }
}
