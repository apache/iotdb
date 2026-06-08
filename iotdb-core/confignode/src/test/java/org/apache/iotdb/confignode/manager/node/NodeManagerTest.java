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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class NodeManagerTest {

  @Test
  public void applyConfigNodeShouldReturnSuccessAfterBothConsensusWrites() throws Exception {
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    IManager configManager = Mockito.mock(IManager.class);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    Mockito.when(consensusManager.write(Mockito.isA(ApplyConfigNodePlan.class)))
        .thenReturn(success);
    Mockito.when(consensusManager.write(Mockito.isA(UpdateVersionInfoPlan.class)))
        .thenReturn(success);
    NodeManager nodeManager = new NodeManager(configManager, Mockito.mock(NodeInfo.class));

    TSStatus status = nodeManager.applyConfigNode(configNodeLocation(), versionInfo());

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    InOrder inOrder = Mockito.inOrder(consensusManager);
    inOrder.verify(consensusManager).write(Mockito.isA(ApplyConfigNodePlan.class));
    inOrder.verify(consensusManager).write(Mockito.isA(UpdateVersionInfoPlan.class));
  }

  @Test
  public void applyConfigNodeShouldReturnFailureAndSkipVersionInfoWhenApplyFails()
      throws Exception {
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    IManager configManager = Mockito.mock(IManager.class);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    TSStatus failure =
        new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()).setMessage("apply failed");
    Mockito.when(consensusManager.write(Mockito.isA(ApplyConfigNodePlan.class)))
        .thenReturn(failure);
    NodeManager nodeManager = new NodeManager(configManager, Mockito.mock(NodeInfo.class));

    TSStatus status = nodeManager.applyConfigNode(configNodeLocation(), versionInfo());

    Assert.assertEquals(failure.getCode(), status.getCode());
    Mockito.verify(consensusManager, Mockito.never())
        .write(Mockito.isA(UpdateVersionInfoPlan.class));
  }

  @Test
  public void applyConfigNodeShouldReturnFailureAndSkipVersionInfoWhenConsensusThrows()
      throws Exception {
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    IManager configManager = Mockito.mock(IManager.class);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.isA(ApplyConfigNodePlan.class)))
        .thenThrow(new ConsensusException("write failed"));
    NodeManager nodeManager = new NodeManager(configManager, Mockito.mock(NodeInfo.class));

    TSStatus status = nodeManager.applyConfigNode(configNodeLocation(), versionInfo());

    Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(consensusManager, Mockito.never())
        .write(Mockito.isA(UpdateVersionInfoPlan.class));
  }

  private TConfigNodeLocation configNodeLocation() {
    return new TConfigNodeLocation(
        0, new TEndPoint("127.0.0.1", 10710), new TEndPoint("127.0.0.1", 10720));
  }

  private TNodeVersionInfo versionInfo() {
    return new TNodeVersionInfo("test-version", "test-build");
  }
}
