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
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
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

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeMetaHistoricalIT extends AbstractPipeDualManualIT {
  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment(3, 3);
  }

  @Test
  public void testTemplateInclusion() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.ln",
              "create database root.db",
              "set ttl to root.ln 3600000",
              "create user `thulab` 'passwd'",
              "create role `admin`",
              "grant role `admin` to `thulab`",
              "grant read on root.** to role `admin`",
              "create schema template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)",
              "set schema template t1 to root.ln.wf01",
              "set schema template t1 to root.db.wf01",
              "create timeseries using schema template on root.ln.wf01.wt01",
              "create timeseries using schema template on root.db.wf01.wt01",
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              // Insert large timestamp to avoid deletion by ttl
              "insert into root.ln.wf01.wt01(time, temperature, status) values (1800000000000, 23, true)"))) {
        return;
      }
      awaitUntilFlush(senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "data, schema");
      extractorAttributes.put("extractor.inclusion.exclusion", "schema.timeseries.ordinary");
      extractorAttributes.put("extractor.path", "root.ln.**");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "list user",
          ColumnHeaderConstant.USER + ",",
          Collections.singleton("root,"));
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "list role", ColumnHeaderConstant.ROLE + ",", Collections.emptySet());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionOrigin,TimePartitionInterval,",
          // Receiver's SchemaReplicationFactor/DataReplicationFactor shall be 3/2 regardless of the
          // sender
          Collections.singleton("root.ln,3,2,0,604800000,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.ln.wf01.wt01.temperature,root.ln.wf01.wt01.status,",
          Collections.singleton("1800000000000,23.0,true,"));

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "create timeseries using schema template on root.ln.wf01.wt02")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton("4,"));
    }
  }

  @Test
  public void testAuthInclusion() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.ln",
              "set ttl to root.ln 3600000",
              "create user `thulab` 'passwd'",
              "create role `admin`",
              "grant role `admin` to `thulab`",
              "grant read on root.** to role `admin`",
              "create schema template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)",
              "set schema template t1 to root.ln.wf01",
              "create timeseries using schema template on root.ln.wf01.wt01",
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              // Insert large timestamp to avoid deletion by ttl
              "insert into root.ln.wf01.wt01(time, temperature, status) values (1800000000000, 23, true)"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "auth");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "list user of role `admin`",
          ColumnHeaderConstant.USER + ",",
          Collections.singleton("thulab,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "list privileges of role `admin`",
          ColumnHeaderConstant.ROLE
              + ","
              + ColumnHeaderConstant.PATH
              + ","
              + ColumnHeaderConstant.PRIVILEGES
              + ","
              + ColumnHeaderConstant.GRANT_OPTION
              + ",",
          new HashSet<>(
              Arrays.asList("admin,root.**,READ_DATA,false,", "admin,root.**,READ_SCHEMA,false,")));

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionOrigin,TimePartitionInterval,",
          Collections.emptySet());
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.**", "Time", Collections.emptySet());

      if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "CREATE ROLE test")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "list role",
          ColumnHeaderConstant.ROLE + ",",
          new HashSet<>(Arrays.asList("admin,", "test,")));
    }
  }

  @Test
  public void testTimeSeriesInclusion() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.sg",
              "create timeseries root.sg.a.b int32",
              "create aligned timeseries root.sg.`apache|timecho-tag-attr`.d1(s1 INT32 tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2), s2 DOUBLE tags(tag3=v3, tag4=v4) attributes(attr3=v3, attr4=v4))"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "schema");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeseries",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          new HashSet<>(
              Arrays.asList(
                  "root.sg.a.b,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.`apache|timecho-tag-attr`.d1.s1,null,root.sg,INT32,TS_2DIFF,LZ4,{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"},null,null,BASE,",
                  "root.sg.`apache|timecho-tag-attr`.d1.s2,null,root.sg,DOUBLE,GORILLA,LZ4,{\"tag4\":\"v4\",\"tag3\":\"v3\"},{\"attr4\":\"v4\",\"attr3\":\"v3\"},null,null,BASE,")));
    }
  }
}
