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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
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
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeLifeCycleIT extends AbstractPipeDualAutoIT {
  @Test
  public void testLifeCycleWithHistoryEnabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"))) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"))) {
        return;
      }

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleWithHistoryDisabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "");

      extractorAttributes.put("extractor.history.enable", "false");
      // start-time and end-time should not work
      extractorAttributes.put("extractor.history.start-time", "0001.01.01T00:00:00");
      extractorAttributes.put("extractor.history.end-time", "2100.01.01T00:00:00");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.ln",
              "create timeseries root.db.d1.s2 with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.db.d1(time, s1) values (2, 2)",
              "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          Collections.singleton("2,2.0,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton("2,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.ln0",
              "create timeseries root.db.d1.s3 with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.db.d1(time, s1) values (3, 3)",
              "flush"))) {
        return;
      }

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          Collections.singleton("2,2.0,"));
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton("2,"));
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          new HashSet<>(Arrays.asList("2,2.0,", "3,3.0,")));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton("3,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("3,"));
    }
  }

  @Test
  public void testLifeCycleLogMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "log");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"))) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleFileMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "file");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"))) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleHybridMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.mode", "hybrid");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"))) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleWithClusterRestart() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Set<String> expectedResSet = new HashSet<>();
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"))) {
        return;
      }

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    try (final SyncConfigNodeIServiceClient ignored =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"))) {
        return;
      }

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testReceiverRestartWhenTransferring() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final AtomicInteger succeedNum = new AtomicInteger(0);
      final Thread t =
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
      } catch (final Throwable e) {
        e.printStackTrace();
        try {
          t.interrupt();
          t.join();
        } catch (Throwable ignored) {
        }
        return;
      }

      t.join();
      if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton(succeedNum.get() + ","));
    }
  }

  @Test
  public void testReceiverAlreadyHaveTimeSeries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv, Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"))) {
        return;
      }

      Thread.sleep(5000);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  @Test
  public void testDoubleLiving() throws Exception {
    // Double living is two clusters with pipes connecting each other.
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    for (int i = 0; i < 100; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
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
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
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

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderIp);
      connectorAttributes.put("connector.port", Integer.toString(senderPort));

      final TSStatus status =
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
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
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
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
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
