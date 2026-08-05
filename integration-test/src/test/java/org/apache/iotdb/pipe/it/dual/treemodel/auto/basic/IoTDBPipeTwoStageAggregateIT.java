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
import org.apache.iotdb.it.env.MultiEnvFactory;
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
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeTwoStageAggregateIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 1);
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
  }

  @Test
  public void testCountPointProcessorUsesSourceCredentials() throws Exception {
    final String sourceUser = "countPointUser";
    final String sourcePassword = "StrngPsWd@623454";
    final String sourceDevice = "root.twostage_source.d1";
    final String processorOutputSeries = "root.twostage_source.result.point_count";
    // The processor uses the configured output series as the tablet device and its measurement
    // node as the tablet measurement.
    final String outputDevice = processorOutputSeries;
    final String outputSeries = outputDevice + ".point_count";

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "CREATE DATABASE root.twostage_source",
            "CREATE TIMESERIES " + sourceDevice + ".s1 WITH DATATYPE=INT32,ENCODING=RLE",
            "CREATE USER " + sourceUser + " '" + sourcePassword + "'",
            "GRANT READ_DATA ON " + sourceDevice + ".s1 TO USER " + sourceUser),
        null);
    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "CREATE DATABASE root.twostage_source",
            "CREATE TIMESERIES " + outputSeries + " WITH DATATYPE=INT64,ENCODING=RLE",
            "INSERT INTO " + outputDevice + "(time,point_count) VALUES (0,0)"),
        null);

    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", sourceDevice + ".s1");
    sourceAttributes.put("source.watermark.interval-ms", "500");
    sourceAttributes.put("user", sourceUser);
    sourceAttributes.put("password", sourcePassword);

    final Map<String, String> processorAttributes = new HashMap<>();
    processorAttributes.put("processor", "count-point-processor");
    processorAttributes.put("processor.output.series", processorOutputSeries);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put("sink", "iotdb-thrift-sink");
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", receiverDataNode.getIp());
    sinkAttributes.put("sink.port", Integer.toString(receiverDataNode.getPort()));
    sinkAttributes.put("sink.user", "root");
    sinkAttributes.put("sink.password", "root");

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final TSStatus createStatus =
          client.createPipe(
              new TCreatePipeReq("countPointPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), createStatus.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.startPipe("countPointPipe").getCode());
    }

    TestUtils.executeNonQueries(
        senderEnv,
        Collections.singletonList(
            "INSERT INTO " + sourceDevice + "(time,s1) VALUES (1,1),(2,2),(3,3)"),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT MAX_VALUE(point_count) FROM " + outputDevice,
        "MAX_VALUE(" + outputSeries + "),",
        Collections.singleton("3,"));
  }
}
