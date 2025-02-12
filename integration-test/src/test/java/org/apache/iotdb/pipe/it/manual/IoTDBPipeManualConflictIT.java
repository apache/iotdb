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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Ignore
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeManualConflictIT extends AbstractPipeDualManualIT {
  @Test
  public void testDoubleLivingTimeseries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "data, schema");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "data, schema");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderEnv.getDataNodeWrapper(0).getIp());
      connectorAttributes.put(
          "connector.port", Integer.toString(senderEnv.getDataNodeWrapper(0).getPort()));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            "create timeseries root.ln.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN",
            "insert into root.ln.wf01.wt01(time, status0) values(now(), false);",
            "flush"))) {
      return;
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        receiverEnv,
        Arrays.asList(
            "create timeseries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
            "insert into root.ln.wf01.wt01(time, status1) values(now(), true);",
            "flush"))) {
      return;
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "show timeseries",
        "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
        new HashSet<>(
            Arrays.asList(
                "root.ln.wf01.wt01.status0,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                "root.ln.wf01.wt01.status1,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,")));
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "select count(*) from root.** group by level=1",
        "count(root.ln.*.*.*),",
        Collections.singleton("2,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show timeseries",
        "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
        new HashSet<>(
            Arrays.asList(
                "root.ln.wf01.wt01.status0,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                "root.ln.wf01.wt01.status1,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,")));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(*) from root.** group by level=1",
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
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "data, schema");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "data, schema");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderEnv.getDataNodeWrapper(0).getIp());
      connectorAttributes.put(
          "connector.port", Integer.toString(senderEnv.getDataNodeWrapper(0).getPort()));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            "create device template t1 (s1 INT64 encoding=RLE, s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.sg1",
            "set device template t1 to root.sg1",
            "create timeseries using device template on root.sg1.d1",
            "insert into root.sg1.d1(time, s1, s2, s3) values(0, 1, 2, 3);",
            "flush"))) {
      return;
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        receiverEnv,
        Arrays.asList(
            "create timeseries using device template on root.sg1.d2",
            "insert into root.sg1.d2(time, s1, s2, s3) values(0, 1, 2, 3);",
            "flush"))) {
      return;
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "count timeseries", "count(timeseries),", Collections.singleton("6,"));
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "select count(*) from root.** group by level=1",
        "count(root.sg1.*.*),",
        Collections.singleton("6,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton("6,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(*) from root.** group by level=1",
        "count(root.sg1.*.*),",
        Collections.singleton("6,"));
  }
}
