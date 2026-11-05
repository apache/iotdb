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

package org.apache.iotdb.pipe.it.triple;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT3;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT3.class})
public class IoTDBPipeForwardIT extends AbstractPipeTripleManualIT {

  @Override
  protected int getEnv1DataNodeCount() {
    return 3;
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    for (final BaseEnv env : Arrays.asList(env1, env2, env3)) {
      env.getConfig()
          .getCommonConfig()
          .setPipeAutoSplitFullEnabled(true)
          .setAutoCreateSchemaEnabled(true)
          .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    }
  }

  @Test
  public void testLocalWriteIsForwardedToDownstreamWithDefaultSourceParameters() throws Exception {
    TestUtils.executeNonQueries(
        env1,
        Collections.singletonList(
            "create pipe a2b with source('forwarding-pipe-requests'='false',"
                + "'capture.tree'='true','capture.table'='true') with sink('node-urls'='"
                + env2.getDataNodeWrapper(0).getIpAndPortString()
                + "')"),
        null);
    TestUtils.executeNonQueries(
        env2,
        Arrays.asList(
            "create pipe b2a with source('forwarding-pipe-requests'='false',"
                + "'capture.tree'='true','capture.table'='true') with sink('node-urls'='"
                + env1.getDataNodeWrapperList().stream()
                    .map(DataNodeWrapper::getIpAndPortString)
                    .collect(Collectors.joining(","))
                + "')",
            "create pipe b2c with sink('node-urls'='"
                + env3.getDataNodeWrapper(0).getIpAndPortString()
                + "')"),
        null);

    final List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int sensor = 0; sensor < 10; sensor++) {
      schemas.add(new MeasurementSchema("s_" + sensor, TSDataType.INT32));
    }
    try (final ISession session = env2.getSessionConnection()) {
      for (int batch = 0; batch < 60; batch++) {
        for (int device = 0; device < 2; device++) {
          final Tablet tablet = new Tablet("root.test.sg_0.d_" + device, schemas, 1000);
          for (int row = 0; row < 1000; row++) {
            final long timestamp = 1577808000000L + (batch * 1000L + row) * 500;
            tablet.addTimestamp(row, timestamp);
            for (int sensor = 0; sensor < schemas.size(); sensor++) {
              tablet.addValue("s_" + sensor, row, batch * 1000 + row);
            }
          }
          session.insertAlignedTablet(tablet);
        }
        // Pace the writes so the workload continues after the source's initial flush.
        Thread.sleep(500);
      }
    }
    for (final BaseEnv env : Arrays.asList(env1, env2, env3)) {
      TestUtils.assertDataEventuallyOnEnv(
          env,
          "select count(s_0) from root.test.sg_0.* align by device",
          "Device,count(s_0),",
          new HashSet<>(Arrays.asList("root.test.sg_0.d_0,60000,", "root.test.sg_0.d_1,60000,")),
          60);
    }
  }

  @Test
  public void testForwardingPipeRequests() throws Exception {
    final DataNodeWrapper env2DataNode = env2.getDataNodeWrapper(0);
    final String env2Ip = env2DataNode.getIp();
    final int env2Port = env2DataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env1.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("inclusion", "all");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("mark-as-pipe-request", "false");
      sinkAttributes.put("ip", env2Ip);
      sinkAttributes.put("port", Integer.toString(env2Port));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    final DataNodeWrapper env3DataNode = env3.getDataNodeWrapper(0);
    final String env3Ip = env3DataNode.getIp();
    final int env3Port = env3DataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env2.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("inclusion", "all");
      sourceAttributes.put("forwarding-pipe-requests", "false");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("ip", env3Ip);
      sinkAttributes.put("port", Integer.toString(env3Port));

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
        env1,
        Arrays.asList(
            "create database root.sg",
            "create timeseries root.sg.wf01.GPS.status0 with datatype=BOOLEAN,encoding=PLAIN",
            "insert into root.sg.wf01.GPS (time, status0) values (0, 1)"),
        null);
    TestUtils.assertDataEventuallyOnEnv(
        env3,
        "select status0 from root.sg.**",
        "Time,root.sg.wf01.GPS.status0,",
        Collections.singleton("0,true,"));
  }
}
