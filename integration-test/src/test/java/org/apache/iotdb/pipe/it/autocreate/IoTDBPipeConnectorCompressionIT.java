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
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeConnectorCompressionIT extends AbstractPipeDualAutoIT {

  @Test
  public void testCompression1() throws Exception {
    doTest("iotdb-thrift-connector", "hybrid", true, "snappy");
  }

  @Test
  public void testCompression2() throws Exception {
    doTest("iotdb-thrift-connector", "hybrid", true, "snappy, lzma2");
  }

  @Test
  public void testCompression3() throws Exception {
    doTest("iotdb-thrift-sync-connector", "hybrid", true, "snappy");
  }

  @Test
  public void testCompression4() throws Exception {
    doTest("iotdb-thrift-sync-connector", "log", false, "lzma2");
  }

  private void doTest(
      String connectorType, String realtimeMode, boolean useBatchMode, String compressionTypes)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (2010-01-01T10:00:00+08:00, 1)",
              "insert into root.db.d1(time, s1) values (2010-01-02T10:00:00+08:00, 2)",
              "flush"))) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor", "iotdb-extractor");
      extractorAttributes.put("extractor.realtime.mode", realtimeMode);

      processorAttributes.put("processor", "do-nothing-processor");

      connectorAttributes.put("connector", connectorType);
      connectorAttributes.put("connector.batch.enable", useBatchMode ? "true" : "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.user", "root");
      connectorAttributes.put("connector.password", "root");
      connectorAttributes.put("connector.compressor", compressionTypes);

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (now(), 3)", "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("3,"));
    }
  }
}
