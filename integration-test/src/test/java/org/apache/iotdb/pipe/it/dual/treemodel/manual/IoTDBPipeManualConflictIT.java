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

package org.apache.iotdb.pipe.it.dual.treemodel.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;
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

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipeManualConflictIT extends AbstractPipeDualTreeModelManualIT {
  @Test
  public void testDoubleLivingTimeseries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "data, schema");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "data, schema");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", senderEnv.getDataNodeWrapper(0).getIp());
      sinkAttributes.put("sink.port", Integer.toString(senderEnv.getDataNodeWrapper(0).getPort()));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create timeseries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN",
            "insert into root.ln.wf01.wt01(time, status0) values(now(), false);",
            "flush"),
        null);

    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create timeseries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
            "insert into root.ln.wf01.wt01(time, status1) values(now(), true);",
            "flush"),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "show timeseries root.ln.**",
        "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
        new HashSet<>(
            Arrays.asList(
                "root.ln.wf01.wt01.status0,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                "root.ln.wf01.wt01.status1,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,")));
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "select count(*) from root.ln.** group by level=1",
        "count(root.ln.*.*.*),",
        Collections.singleton("2,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show timeseries root.ln.**",
        "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
        new HashSet<>(
            Arrays.asList(
                "root.ln.wf01.wt01.status0,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                "root.ln.wf01.wt01.status1,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,")));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(*) from root.ln.** group by level=1",
        "count(root.ln.*.*.*),",
        Collections.singleton("2,"));
  }

  @Test
  public void testDoubleLivingTemplate() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "data, schema");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "data, schema");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", senderEnv.getDataNodeWrapper(0).getIp());
      sinkAttributes.put("sink.port", Integer.toString(senderEnv.getDataNodeWrapper(0).getPort()));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create device template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set device template t1 to root.sg1",
            "create timeseries using device template on root.sg1.d1",
            "insert into root.sg1.d1(time, s1, s2, s3) values(0, 1, 2, 3);",
            "flush"),
        null);

    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create timeseries using device template on root.sg1.d2",
            "insert into root.sg1.d2(time, s1, s2, s3) values(0, 1, 2, 3);",
            "flush"),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "count timeseries root.sg1.**",
        "count(timeseries),",
        Collections.singleton("6,"));
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "select count(*) from root.sg1.** group by level=1",
        "count(root.sg1.*.*),",
        Collections.singleton("6,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries root.sg1.**",
        "count(timeseries),",
        Collections.singleton("6,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(*) from root.sg1.** group by level=1",
        "count(root.sg1.*.*),",
        Collections.singleton("6,"));
  }
}
