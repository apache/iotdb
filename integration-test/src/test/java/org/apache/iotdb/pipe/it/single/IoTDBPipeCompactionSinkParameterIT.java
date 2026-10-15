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
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipeCompactionSinkParameterIT extends AbstractPipeSingleIT {

  @Test
  public void testCompactionSinkParameterDefaultAndExplicitValueArePersisted() throws Exception {
    final Map<String, String> connectorAttributes = new HashMap<>();
    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.ip", "127.0.0.1");
    connectorAttributes.put("connector.port", "6667");

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(new TCreatePipeReq("defaultCompactionPipe", connectorAttributes))
              .getCode());

      final Map<String, String> disabledConnectorAttributes = new HashMap<>(connectorAttributes);
      disabledConnectorAttributes.put(
          PipeSinkConstant.SINK_ENABLE_COMPACTION_KEY, Boolean.FALSE.toString());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(new TCreatePipeReq("disabledCompactionPipe", disabledConnectorAttributes))
              .getCode());

      final List<TShowPipeInfo> pipeInfoList =
          client.showPipe(new TShowPipeReq().setUserName("root")).getPipeInfoList();
      final TShowPipeInfo defaultPipeInfo =
          pipeInfoList.stream()
              .filter(info -> "defaultCompactionPipe".equals(info.getId()))
              .findFirst()
              .orElseThrow();
      final TShowPipeInfo disabledPipeInfo =
          pipeInfoList.stream()
              .filter(info -> "disabledCompactionPipe".equals(info.getId()))
              .findFirst()
              .orElseThrow();
      Assert.assertTrue(defaultPipeInfo.getPipeConnector().contains("enable-compaction=true"));
      Assert.assertTrue(disabledPipeInfo.getPipeConnector().contains("enable-compaction=false"));
    }
  }
}
