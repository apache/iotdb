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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueriesWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueryWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.executeQueryWithRetry;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeLifeCycleIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testLifeCycleWithHistoryEnabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleWithHistoryDisabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");

      sourceAttributes.put("source.history.enable", "false");
      // start-time and end-time should not work
      sourceAttributes.put("source.history.start-time", "0001.01.01T00:00:00");
      sourceAttributes.put("source.history.end-time", "2100.01.01T00:00:00");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create database root.ln",
              "create timeseries root.db.d1.s2 with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.db.d1(time, s1) values (2, 2)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          Collections.singleton("2,2.0,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.db.**",
          "count(timeseries),",
          Collections.singleton("2,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.ln.**",
          "count(timeseries),",
          Collections.singleton("0,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases root.db", "count,", Collections.singleton("1,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases root.ln", "count,", Collections.singleton("1,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create database root.ln0",
              "create timeseries root.db.d1.s3 with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.db.d1(time, s1) values (3, 3)",
              "flush"),
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          Collections.singleton("2,2.0,"));
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "count timeseries root.ln0.**",
          "count(timeseries),",
          Collections.singleton("1,"));
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "count timeseries root.db.**",
          "count(timeseries),",
          Collections.singleton("1,"));
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "count databases root.ln0", "count,", Collections.singleton("1,"));
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "count databases root.db", "count,", Collections.singleton("1,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select s1 from root.db.d1",
          "Time,root.db.d1.s1,",
          new HashSet<>(Arrays.asList("2,2.0,", "3,3.0,")));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.db.**",
          "count(timeseries),",
          Collections.singleton("3,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.ln0.**",
          "count(timeseries),",
          Collections.singleton("0,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("4,"));
    }
  }

  @Test
  public void testLifeCycleLogMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "log");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleFileMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "file");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testLifeCycleHybridMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "hybrid");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
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

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      expectedResSet.add("1,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      expectedResSet.add("2,2.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
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

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      expectedResSet.add("3,3.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    }
  }

  @Test
  public void testReceiverRestartWhenTransferring() throws Exception {
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

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Thread t =
          new Thread(
              () -> {
                Connection connection = null;
                try {
                  connection = senderEnv.getConnection();
                } catch (SQLException e) {
                  // ignore
                }
                try {
                  for (int i = 0; i < 100; ++i) {
                    TestUtils.executeNonQuery(
                        senderEnv,
                        String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
                        connection);
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
      TestUtils.executeNonQuery(senderEnv, "flush", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));
    }
  }

  @Test
  public void testReceiverAlreadyHaveTimeSeries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQuery(
          receiverEnv, "insert into root.db.d1(time, s1) values (1, 1)", null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      Thread.sleep(5000);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
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
      TestUtils.executeNonQuery(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i), null);
    }
    TestUtils.executeNonQuery(senderEnv, "flush", null);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    try (Connection connection = senderEnv.getConnection()) {
      for (int i = 100; i < 200; ++i) {
        TestUtils.executeNonQuery(
            senderEnv,
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
            connection);
      }
      TestUtils.executeNonQuery(senderEnv, "flush", connection);
    }

    try (Connection connection = receiverEnv.getConnection()) {
      for (int i = 200; i < 300; ++i) {
        TestUtils.executeNonQuery(
            receiverEnv,
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
            connection);
      }
      TestUtils.executeNonQuery(receiverEnv, "flush", connection);
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", senderIp);
      sinkAttributes.put("sink.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    try (Connection connection = receiverEnv.getConnection()) {
      for (int i = 300; i < 400; ++i) {
        TestUtils.executeNonQuery(
            receiverEnv,
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
            connection);
      }
      TestUtils.executeNonQuery(receiverEnv, "flush", connection);
    }

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    try (Connection connection = senderEnv.getConnection()) {
      for (int i = 400; i < 500; ++i) {
        TestUtils.executeNonQuery(
            senderEnv,
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
            connection);
      }
      TestUtils.executeNonQuery(senderEnv, "flush", connection);
    }

    try (Connection connection = receiverEnv.getConnection()) {
      for (int i = 500; i < 600; ++i) {
        TestUtils.executeNonQuery(
            receiverEnv,
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
            connection);
      }
      TestUtils.executeNonQuery(receiverEnv, "flush", connection);
    }

    for (int i = 400; i < 600; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
  }

  @Test
  public void testPermission() {
    createUser(senderEnv, "test", "test123123456");

    assertNonQueryTestFail(
        senderEnv,
        "create pipe testPipe\n"
            + "with sink (\n"
            + "  'sink'='iotdb-thrift-sink',\n"
            + "  'sink.ip'='127.0.0.1',\n"
            + "  'sink.port'='6668'\n"
            + ")",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        senderEnv,
        "drop pipe testPipe",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    // Will not throw exception
    executeQueryWithRetry(senderEnv, "show pipes", "test", "test123123456");
    assertNonQueryTestFail(
        senderEnv,
        "start pipe testPipe",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        senderEnv,
        "stop pipe testPipe",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");

    assertNonQueryTestFail(
        senderEnv,
        "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'xxx'",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        senderEnv,
        "drop pipePlugin TestProcessor",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertTestFail(
        senderEnv,
        "show pipePlugins",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");

    grantUserSystemPrivileges(senderEnv, "test", PrivilegeType.SYSTEM);

    executeNonQueryWithRetry(
        senderEnv,
        "create pipe testPipe\n" + "with sink (\n" + "  'sink'='write-back-sink'\n" + ")",
        "test",
        "test123123456");
    executeQueryWithRetry(senderEnv, "show pipes", "test", "test123123456");
    executeNonQueriesWithRetry(
        senderEnv,
        Arrays.asList("start pipe testPipe", "stop pipe testPipe", "drop pipe testPipe"),
        "test",
        "test123123456");

    assertNonQueryTestFail(
        senderEnv,
        "create pipePlugin TestProcessor as 'org.apache.iotdb.db.pipe.example.TestProcessor' USING URI 'xxx'",
        "701: Untrusted uri xxx",
        "test",
        "test123123456");
    executeQueryWithRetry(senderEnv, "show pipe plugins", "test", "test123123456");
  }
}
