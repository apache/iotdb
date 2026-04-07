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

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.iotdb.db.it.IoTDBRestServiceIT.getHttpPost;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeDataSinkIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getDataNodeConfig().setEnableRestService(true);
    senderEnv.getConfig().getCommonConfig().setPipeAutoSplitFullEnabled(true);
  }

  @Test
  public void testThriftSinkWithRealtimeFirstDisabled() throws Exception {
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
  public void testLegacySink() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe testPipe ('sink'='iotdb-legacy-pipe-sink', 'ip'='%s', 'port'='%s', 'version'='1.3')",
              receiverIp, receiverPort));
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
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

  @Test
  public void testSpecialPartialInsert() throws Exception {
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b with sink ('node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
    }

    CloseableHttpClient httpClient = HttpClientBuilder.create().build();

    HttpPost httpPost =
        getHttpPost(
            "http://"
                + senderEnv.getDataNodeWrapper(0).getIp()
                + ":"
                + senderEnv.getDataNodeWrapper(0).getRestServicePort()
                + "/rest/v2/insertRecords");
    String json =
        "{\"timestamps\":[1635232113960,1635232151960,1635232143960,1635232143960],\"measurements_list\":[[\"s33\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"FLOAT\",\"DOUBLE\"],[\"FLOAT\",\"DOUBLE\"],[\"BOOLEAN\",\"TEXT\"]],\"values_list\":[[1,false],[2.1,2],[4,6],[false,\"cccccc\"]],\"is_aligned\":false,\"devices\":[\"root.s1\",\"root.s1\",\"root.s1\",\"root.s3\"]}";
    httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
    for (int i = 0; i < 30; i++) {
      try {
        httpClient.execute(httpPost);
        break;
      } catch (final Exception e) {
        if (i == 29) {
          throw e;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select s88, s77, s66, s55, s44, s33 from root.s1",
        "Time,root.s1.s88,root.s1.s77,root.s1.s66,root.s1.s55,root.s1.s33,",
        new HashSet<>(
            Arrays.asList(
                "1635232113960,null,null,null,null,1,",
                "1635232151960,null,null,2.0,2.1,null,",
                "1635232143960,6.0,4.0,null,null,null,")));
  }

  @Test
  public void testTransferMods() {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.sg_nonAligned",
            "create TIMESERIES root.sg_nonAligned.`非对齐序列带有encoding和压缩方式`.s0 with datatype=boolean, encoding=RLE,compressor=snappy",
            "create timeseries root.sg_nonAligned.`非对齐序列带有encoding和压缩方式`.s1 with datatype=int32, encoding=PLAIN,compressor=LZ4",
            "create timeseries root.sg_nonAligned.`非对齐序列带有encoding和压缩方式`.s2 with datatype=int64,encoding=gorilla,compressor=uncompressed",
            "create timeseries root.sg_nonAligned.`非对齐序列带有encoding和压缩方式`.s3 with datatype=float,encoding=chimp,compressor=gzip",
            "create timeseries root.sg_nonAligned.`非对齐序列带有encoding和压缩方式`.s4 with datatype=double,encoding=ts_2diff,compressor=zstd",
            "create timeseries root.sg_nonAligned.`非对齐序列带有encoding和压缩方式`.s5 with datatype=text,encoding=dictionary,compressor=lzma2",
            "insert into root.sg_nonAligned.`非对齐序列带有encoding和压缩方式`(time,s1, s2,s3,s4,s0,s5) values(1,1,10,5.39,5.51234,true,''),(11,null,20,5.39,15.51234,false,'第2条 device_nonAligned'),(21,3,null,5.39,25.51234,true,'第3条device_nonAligned'),(31,4,40,null,35.51234,true,'第4条device_nonAligned'),(41,5,50,5.39,null,false,'第5条device_nonAligned'),(51,6,60,5.39,55.51234,null,'第6条device_nonAligned'),(61,7,70,5.39,65.51234,false,null),(71,8,80,5.39,75.51234,false,'第8条device_nonAligned'),(81,9,90,5.39,85.51234,false,'第9条device_nonAligned'),(91,10,100,5.39,95.51234,false,'第10条device_nonAligned'),(101,11,110,5.39,105.51234,false,'第11条device_nonAligned'),(111,12,120,5.39,115.51234,false,'第12条device_nonAligned'),(121,13,130,5.39,125.51234,false,'第13条device_nonAligned'),(131,14,140,5.39,135.51234,false,'第14条device_nonAligned'),(141,15,150,5.39,145.51234,false,'第15条'),(151,16,160,5.39,155.51234,false,'第16条'),(161,17,170,5.39,165.51234,false,'第17条'),(171,18,180,5.39,175.51234,false,'第18条'),(181,19,190,5.39,185.51234,false,'第19条'),(191,20,200,5.39,195.51234,false,'第20条'),(201,21,210,5.39,null,false,'第21条')",
            "flush",
            "delete timeseries root.sg_nonAligned.非对齐序列带有encoding和压缩方式.s0",
            String.format(
                "create pipe test with source ('source.realtime.mode'='stream','inclusion'='data','path'='root.sg_nonAligned.非对齐序列带有encoding和压缩方式.**','source.realtime.enable'='true','mods.enable'='true') with sink ('sink'='iotdb-thrift-sink', 'sink.node-urls'='%s')",
                receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString())));

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries root.sg_nonAligned.非对齐序列带有encoding和压缩方式.*",
        "count(timeseries),",
        Collections.singleton("5,"));

    TestUtils.executeNonQueries(
        senderEnv, Arrays.asList("drop pipe test_history", "drop pipe test_realtime"));

    TestUtils.executeNonQuery(receiverEnv, "drop database root.**");

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "delete timeseries root.sg_nonAligned.非对齐序列带有encoding和压缩方式.s1",
            String.format(
                "create pipe test with source ('source.realtime.mode'='stream','inclusion'='data','path'='root.sg_nonAligned.非对齐序列带有encoding和压缩方式.**','source.realtime.enable'='true','mods.enable'='true') with sink ('sink'='iotdb-thrift-sink', 'sink.node-urls'='%s')",
                receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString())));

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries root.sg_nonAligned.非对齐序列带有encoding和压缩方式.*",
        "count(timeseries),",
        Collections.singleton("4,"),
        15);
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv,
        "count timeseries root.sg_nonAligned.非对齐序列带有encoding和压缩方式.*",
        "count(timeseries),",
        Collections.singleton("4,"));
  }
}
