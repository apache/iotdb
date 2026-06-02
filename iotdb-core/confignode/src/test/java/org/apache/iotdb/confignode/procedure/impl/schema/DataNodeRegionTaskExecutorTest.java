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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class DataNodeRegionTaskExecutorTest {

  @Test
  public void testPrintFailureMap() {
    final TestRegionTaskExecutor executor = new TestRegionTaskExecutor();
    executor.processResponseOfOneDataNode(
        new TDataNodeLocation().setDataNodeId(0),
        Collections.singletonList(new TConsensusGroupId(TConsensusGroupType.DataRegion, 3)),
        StatusUtils.OK);
    executor.processResponseOfOneDataNode(
        new TDataNodeLocation().setDataNodeId(0),
        Arrays.asList(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2)),
        RpcUtils.getStatus(
            Arrays.asList(
                StatusUtils.OK, new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode()))));
    Assert.assertEquals("{DataNodeId: 0=[TSStatus(code:509)]}", executor.printFailureMap());

    executor.processResponseOfOneDataNodeWithSuccessResult(
        new TDataNodeLocation().setDataNodeId(0),
        Arrays.asList(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2)),
        RpcUtils.getStatus(
            Arrays.asList(
                StatusUtils.OK, new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode()))));
    Assert.assertTrue(StatusUtils.OK == executor.getSuccessResult().get(0));
  }

  private static class TestRegionTaskExecutor extends DataNodeTSStatusTaskExecutor<Void> {

    private TestRegionTaskExecutor() {
      super(new ConfigNodeProcedureEnv(null, null), null, false, null, null);
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      interruptTask();
    }
  }
}
