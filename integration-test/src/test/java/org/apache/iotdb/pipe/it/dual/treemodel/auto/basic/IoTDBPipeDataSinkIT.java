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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeDataSinkIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testThriftConnectorWithRealtimeFirstDisabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (0, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "log");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.realtime-first", "false");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.vehicle.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,"))));
    }
  }

  @Test
  public void testSinkTabletFormat() throws Exception {
    testSinkFormat("tablet");
  }

  @Test
  public void testSinkTsFileFormat() throws Exception {
    testSinkFormat("tsfile");
  }

  private void testSinkFormat(final String format) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "forced-log");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.format", format);
      sinkAttributes.put("sink.realtime-first", "false");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.vehicle.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("testPipe").getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (4, 1)",
              "insert into root.vehicle.d0(time, s1) values (3, 1), (0, 1)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.vehicle.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(
              new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,", "2,1.0,", "3,1.0,", "4,1.0,"))));
    }
  }

  @Test
  public void testLegacyConnector() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "log");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-legacy-pipe-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      // This version does not matter since it's no longer checked by the legacy receiver
      sinkAttributes.put("sink.version", "1.3");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (0, 1)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.vehicle.**",
          "Time,root.vehicle.d0.s1,",
          Collections.singleton("0,1.0,"));
    }
  }

  @Test
  public void testReceiverAutoCreateByLog() throws Exception {
    testReceiverAutoCreate(
        new HashMap<String, String>() {
          {
            put("source.realtime.mode", "forced-log");
            put("user", "root");
          }
        });
  }

  @Test
  public void testReceiverAutoCreateByFile() throws Exception {
    testReceiverAutoCreate(
        new HashMap<String, String>() {
          {
            put("source.realtime.mode", "batch");
            put("user", "root");
          }
        });
  }

  @Test
  public void testReceiverAutoCreateWithPattern() throws Exception {
    testReceiverAutoCreate(
        new HashMap<String, String>() {
          {
            put("source.realtime.mode", "batch");
            put("source.path", "root.ln.wf01.wt0*.*");
            put("user", "root");
          }
        });
  }

  private void testReceiverAutoCreate(final Map<String, String> sourceAttributes) throws Exception {
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create timeSeries root.ln.wf01.wt01.boolean boolean",
              "create timeSeries root.ln.wf01.wt01.int32 int32",
              "create timeSeries root.ln.wf01.wt01.int64 int64",
              "create timeSeries root.ln.wf01.wt01.float float",
              "create timeSeries root.ln.wf01.wt01.double double",
              "create timeSeries root.ln.wf01.wt01.time_stamp timestamp",
              "create timeSeries root.ln.wf01.wt01.date date",
              "create timeSeries root.ln.wf01.wt01.text text",
              "create timeSeries root.ln.wf01.wt01.string string",
              "create timeSeries root.ln.wf01.wt01.blob blob",
              "create aligned timeSeries root.ln.wf01.wt02(int32 int32, boolean boolean)",
              "insert into root.ln.wf01.wt01(time, boolean, int32, int64, float, double, time_stamp, date, text, string, blob) values (20000, false, 123, 321, 13.3, 14.4, now(), '2000-12-13', 'abc', 'def', X'f103')",
              "insert into root.ln.wf01.wt02(time, int32, boolean) values (20000, 123, false)",
              // For pattern parse
              "insert into root.ln.wf01.wt11(time, redundant_data) values (20000, -1)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeSeries root.ln.wf01.wt01.*",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          Collections.unmodifiableSet(
              new HashSet<>(
                  Arrays.asList(
                      "root.ln.wf01.wt01.boolean,null,root.ln,BOOLEAN,RLE,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.int32,null,root.ln,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.int64,null,root.ln,INT64,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.float,null,root.ln,FLOAT,GORILLA,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.double,null,root.ln,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.time_stamp,null,root.ln,TIMESTAMP,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.date,null,root.ln,DATE,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.text,null,root.ln,TEXT,PLAIN,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.string,null,root.ln,STRING,PLAIN,LZ4,null,null,null,null,BASE,",
                      "root.ln.wf01.wt01.blob,null,root.ln,BLOB,PLAIN,LZ4,null,null,null,null,BASE,"))),
          handleFailure);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show devices root.ln.wf01.wt02",
          "Device,IsAligned,Template,TTL(ms),",
          Collections.singleton("root.ln.wf01.wt02,true,null,INF,"),
          handleFailure);
    }
  }

  @Test
  public void testSyncLoadTsFile() throws Exception {
    testReceiverLoadTsFile("sync");
  }

  @Test
  public void testAsyncLoadTsFile() throws Exception {
    testReceiverLoadTsFile("async");
  }

  private void testReceiverLoadTsFile(final String loadTsFileStrategy) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "forced-log");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.load-tsfile-strategy", loadTsFileStrategy);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.vehicle.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("testPipe").getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (4, 1)",
              "insert into root.vehicle.d0(time, s1) values (3, 1), (0, 1)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.vehicle.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(
              new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,", "2,1.0,", "3,1.0,", "4,1.0,"))));
    }
  }

  @Test
  public void testSyncLoadTsFileWithoutVerify() throws Exception {
    testLoadTsFileWithoutVerify("sync");
  }

  @Test
  public void testAsyncLoadTsFileWithoutVerify() throws Exception {
    testLoadTsFileWithoutVerify("async");
  }

  private void testLoadTsFileWithoutVerify(final String loadTsFileStrategy) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "batch");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.load-tsfile-strategy", loadTsFileStrategy);
      sinkAttributes.put("sink.tsfile.validation", "false");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.vehicle.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))));
    }
  }
}
