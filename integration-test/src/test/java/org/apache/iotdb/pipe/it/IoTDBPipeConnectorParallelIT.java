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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeConnectorParallelIT extends AbstractPipeDualIT {
  @Test
  public void testIoTConnectorParallel() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    Set<String> expectedResSet = new HashSet<>();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.parallel.tasks", "3");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.sg1.d1(time, s1) values (0, 1)",
              "insert into root.sg1.d1(time, s1) values (1, 2)",
              "insert into root.sg1.d1(time, s1) values (2, 3)",
              "insert into root.sg1.d1(time, s1) values (3, 4)"))) {
        return;
      }

      expectedResSet.add("0,1.0,");
      expectedResSet.add("1,2.0,");
      expectedResSet.add("2,3.0,");
      expectedResSet.add("3,4.0,");
      TestUtils.assertDataOnEnv(
          receiverEnv, "select * from root.**", "Time,root.sg1.d1.s1,", expectedResSet);
    }
  }
}
