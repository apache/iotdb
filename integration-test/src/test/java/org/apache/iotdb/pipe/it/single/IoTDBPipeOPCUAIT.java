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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipeOPCUAIT extends AbstractPipeSingleIT {
  @Test
  public void testOPCUASink() throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          env, "insert into root.db.d1(time, s1) values (1, 1)")) {
        return;
      }

      final Map<String, String> connectorAttributes = new HashMap<>();
      connectorAttributes.put("sink", "opc-ua-sink");
      connectorAttributes.put("opcua.model", "client-server");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", connectorAttributes)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("testPipe").getCode());

      // Test reconstruction
      connectorAttributes.put("password", "test");
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", connectorAttributes)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());

      // Test conflict
      connectorAttributes.put("password", "conflict");
      Assert.assertEquals(
          TSStatusCode.PIPE_ERROR.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", connectorAttributes)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());
    }
  }
}
