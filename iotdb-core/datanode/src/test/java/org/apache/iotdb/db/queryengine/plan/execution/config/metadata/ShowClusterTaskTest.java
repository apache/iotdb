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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import com.timecho.iotdb.commons.commission.obligation.ObligationStatus;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ShowClusterTaskTest {

  @Test
  public void testShowClusterUsesUnknownForMissingActivationSamples() throws Exception {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    ShowClusterTask.buildTsBlock(createClusterResponseWithoutActivationSamples(), future);

    assertUnknownActivationStatus(future.get().getResultSet(), 7);
  }

  @Test
  public void testShowClusterDetailsUsesUnknownForMissingActivationSamples() throws Exception {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    ShowClusterDetailsTask.buildTSBlock(createClusterResponseWithoutActivationSamples(), future);

    assertUnknownActivationStatus(future.get().getResultSet(), 13);
  }

  private static void assertUnknownActivationStatus(TsBlock resultSet, int columnIndex) {
    assertEquals(3, resultSet.getPositionCount());
    for (int row = 0; row < resultSet.getPositionCount(); row++) {
      assertFalse(resultSet.getColumn(columnIndex).isNull(row));
      assertEquals(
          ObligationStatus.UNKNOWN.toSimpleString(),
          resultSet
              .getColumn(columnIndex)
              .getBinary(row)
              .getStringValue(TSFileConfig.STRING_CHARSET));
    }
  }

  private static TShowClusterResp createClusterResponseWithoutActivationSamples() {
    TConfigNodeLocation configNode =
        new TConfigNodeLocation()
            .setConfigNodeId(0)
            .setInternalEndPoint(endpoint(10710))
            .setConsensusEndPoint(endpoint(10720));
    TDataNodeLocation dataNode =
        new TDataNodeLocation()
            .setDataNodeId(1)
            .setClientRpcEndPoint(endpoint(6667))
            .setInternalEndPoint(endpoint(10730))
            .setMPPDataExchangeEndPoint(endpoint(10740))
            .setDataRegionConsensusEndPoint(endpoint(10750))
            .setSchemaRegionConsensusEndPoint(endpoint(10760));
    TAINodeLocation aiNode =
        new TAINodeLocation().setAiNodeId(2).setInternalEndPoint(endpoint(10810));

    return new TShowClusterResp()
        .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
        .setConfigNodeList(Collections.singletonList(configNode))
        .setDataNodeList(Collections.singletonList(dataNode))
        .setAiNodeList(Collections.singletonList(aiNode))
        .setNodeStatus(new HashMap<>())
        .setNodeVersionInfo(new HashMap<>())
        .setNodeActivateInfo(new HashMap<>());
  }

  private static TEndPoint endpoint(int port) {
    return new TEndPoint("127.0.0.1", port);
  }
}
