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
public class IoTDBPipeInclusionIT extends AbstractPipeDualTreeModelManualIT {
  @Test
  public void testPureSchemaInclusion() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "schema");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
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

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              // TODO: add database creation after the database auto creating on receiver can be
              // banned
              "create timeSeries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "ALTER timeSeries root.ln.wf01.wt01.status ADD TAGS tag3=v3",
              "ALTER timeSeries root.ln.wf01.wt01.status ADD ATTRIBUTES attr4=v4"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeseries root.ln.**",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          Collections.singleton(
              "root.ln.wf01.wt01.status,null,root.ln,BOOLEAN,PLAIN,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,"));

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "ALTER timeSeries root.** set STORAGE_PROPERTIES compressor=ZSTD",
              "insert into root.ln.wf01.wt01(time, status) values(now(), false)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeseries root.ln.**",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          Collections.singleton(
              "root.ln.wf01.wt01.status,null,root.ln,BOOLEAN,PLAIN,ZSTD,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,"));

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.ln.**", "Time,", Collections.emptySet());
    }
  }

  @Test
  public void testPureSchemaInclusionWithMultiplePattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("path", "root.ln.wf01.wt01.status,root.ln.wf02.**");
      sourceAttributes.put("source.inclusion", "schema");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
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

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "ALTER timeseries root.ln.wf01.wt01.status ADD TAGS tag3=v3",
              "ALTER timeseries root.ln.wf01.wt01.status ADD ATTRIBUTES attr4=v4",
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "ALTER timeseries root.ln.wf02.wt01.status ADD TAGS tag3=v3",
              "ALTER timeseries root.ln.wf02.wt01.status ADD ATTRIBUTES attr4=v4",
              "create timeseries root.ln.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "ALTER timeseries root.ln.wf03.wt01.status ADD TAGS tag3=v3",
              "ALTER timeseries root.ln.wf03.wt01.status ADD ATTRIBUTES attr4=v4"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeseries root.ln.**",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          new HashSet<String>() {
            {
              add(
                  "root.ln.wf01.wt01.status,null,root.ln,BOOLEAN,PLAIN,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,");
              add(
                  "root.ln.wf02.wt01.status,null,root.ln,BOOLEAN,PLAIN,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,");
            }
          });

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.ln.wf01.wt01(time, status) values(now(), false)", "flush"),
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.ln.**", "Time,", Collections.emptySet());
    }
  }

  @Test
  public void testPureSchemaInclusionWithExclusionPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("extractor.inclusion", "schema");
      sourceAttributes.put("user", "root");

      // Include root.ln.**
      sourceAttributes.put("path", "root.ln.**");
      // Exclude root.ln.wf02.* and root.ln.wf03.wt01.status
      sourceAttributes.put("path.exclusion", "root.ln.wf02.**, root.ln.wf03.wt01.status");

      sinkAttributes.put("connector", "iotdb-thrift-connector");
      sinkAttributes.put("connector.ip", receiverIp);
      sinkAttributes.put("connector.port", Integer.toString(receiverPort));

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
          Arrays.asList(
              // Should be included
              "create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "ALTER timeseries root.ln.wf01.wt01.status ADD TAGS tag3=v3",
              "ALTER timeseries root.ln.wf01.wt01.status ADD ATTRIBUTES attr4=v4",
              // Should be excluded by root.ln.wf02.*
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "ALTER timeseries root.ln.wf02.wt01.status ADD TAGS tag3=v3",
              "ALTER timeseries root.ln.wf02.wt01.status ADD ATTRIBUTES attr4=v4",
              // Should be excluded by root.ln.wf03.wt01.status
              "create timeseries root.ln.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "ALTER timeseries root.ln.wf03.wt01.status ADD TAGS tag3=v3",
              "ALTER timeseries root.ln.wf03.wt01.status ADD ATTRIBUTES attr4=v4"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeseries root.ln.**",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          // Only wf01 should be synced
          Collections.singleton(
              "root.ln.wf01.wt01.status,null,root.ln,BOOLEAN,PLAIN,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,"));

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.ln.wf01.wt01(time, status) values(now(), false)", "flush"),
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.ln.**", "Time,", Collections.emptySet());
    }
  }

  @Test
  public void testAuthExclusion() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "auth");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
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

      TestUtils.executeNonQuery(senderEnv, "create user `ln_write_user` 'write_pwd123456'", null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "list user", "user,", Collections.singleton("root,"));
    }
  }

  @Test
  public void testAuthInclusionWithPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "auth");
      sourceAttributes.put("path", "root.ln.**");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
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

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create user `ln_write_user` 'write_pwd123456'",
              "grant system,security on root.** to USER ln_write_user with grant option",
              "GRANT READ_DATA, WRITE_DATA ON root.** TO USER ln_write_user;"),
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "LIST PRIVILEGES OF USER ln_write_user",
          "ROLE,PATH,PRIVILEGES,GRANT OPTION,",
          new HashSet<>(
              Arrays.asList(
                  ",root.**,SYSTEM,true,",
                  ",root.**,SECURITY,true,",
                  ",root.ln.**,READ_DATA,false,",
                  ",root.ln.**,WRITE_DATA,false,")));
    }
  }

  @Test
  public void testPureDeleteInclusion() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "data.delete");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
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

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.ln.wf01.wt01(time, status) values(0, true)",
              "flush"),
          null);

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          receiverEnv,
          Arrays.asList(
              "create timeseries root.ln.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.ln.wf01.wt01(time, status1) values(0, true)",
              "flush"),
          null);

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQuery(senderEnv, "delete from root.**", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.ln.**",
          "Time,root.ln.wf01.wt01.status1,",
          Collections.emptySet());
    }
  }
}
