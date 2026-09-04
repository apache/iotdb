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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.RatisRequestFailedException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NodeManagerTest {

  private final TConfigNodeLocation removedConfigNode = newConfigNodeLocation(0);
  private final TConfigNodeLocation firstCandidate = newConfigNodeLocation(1);
  private final TConfigNodeLocation secondCandidate = newConfigNodeLocation(2);
  private final List<TConfigNodeLocation> configNodes =
      Arrays.asList(removedConfigNode, firstCandidate, secondCandidate);

  private IConsensus consensus;
  private ConsensusManager consensusManager;
  private NodeInfo nodeInfo;
  private CQManager cqManager;
  private NodeManager nodeManager;
  private int originalCnConnectionTimeout;
  private int originalTransferLeaderTimeout;

  @Before
  public void setUp() {
    originalCnConnectionTimeout =
        CommonDescriptor.getInstance().getConfig().getCnConnectionTimeoutInMS();
    originalTransferLeaderTimeout =
        ConfigNodeDescriptor.getInstance().getConf().getRatisTransferLeaderTimeoutMs();
    CommonDescriptor.getInstance().getConfig().setCnConnectionTimeoutInMS(60_000);
    ConfigNodeDescriptor.getInstance().getConf().setRatisTransferLeaderTimeoutMs(1_000);

    consensus = Mockito.mock(IConsensus.class);
    consensusManager = Mockito.mock(ConsensusManager.class);
    IManager configManager = Mockito.mock(IManager.class);
    nodeInfo = Mockito.mock(NodeInfo.class);
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    cqManager = Mockito.mock(CQManager.class);

    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getCQManager()).thenReturn(cqManager);
    Mockito.when(configManager.getLoadManager()).thenReturn(loadManager);
    Mockito.when(consensusManager.getConsensusImpl()).thenReturn(consensus);
    Mockito.when(consensusManager.getConsensusGroupId())
        .thenReturn(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID);
    Mockito.when(nodeInfo.getRegisteredConfigNodes()).thenReturn(configNodes);
    Mockito.when(loadManager.filterConfigNodeThroughStatus(Mockito.any(NodeStatus[].class)))
        .thenReturn(
            configNodes.stream()
                .map(TConfigNodeLocation::getConfigNodeId)
                .collect(Collectors.toList()));
    Mockito.when(nodeInfo.getRegisteredConfigNodes(Mockito.anyList())).thenReturn(configNodes);

    nodeManager = new NodeManager(configManager, nodeInfo);
  }

  @After
  public void tearDown() {
    CommonDescriptor.getInstance()
        .getConfig()
        .setCnConnectionTimeoutInMS(originalCnConnectionTimeout);
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setRatisTransferLeaderTimeoutMs(originalTransferLeaderTimeout);
  }

  @Test
  public void transferLeaderShouldTryAnotherCandidateAfterTransientFailure() throws Exception {
    Mockito.when(consensusManager.getLeaderLocation())
        .thenReturn(removedConfigNode, removedConfigNode, secondCandidate);
    Mockito.doThrow(new RatisRequestFailedException(new Exception("transfer failed")))
        .doNothing()
        .when(consensus)
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    TSStatus status =
        nodeManager.checkConfigNodeBeforeRemove(new RemoveConfigNodePlan(removedConfigNode));

    Assert.assertEquals(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), status.getCode());
    Assert.assertEquals(secondCandidate.getInternalEndPoint(), status.getRedirectNode());
    ArgumentCaptor<Peer> peerCaptor = ArgumentCaptor.forClass(Peer.class);
    Mockito.verify(consensus, Mockito.times(2))
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), peerCaptor.capture());
    Assert.assertEquals(
        Arrays.asList(firstCandidate.getConfigNodeId(), secondCandidate.getConfigNodeId()),
        peerCaptor.getAllValues().stream().map(Peer::getNodeId).collect(Collectors.toList()));
  }

  @Test
  public void configNodeWithoutCalendarDurationCapabilityIsRejectedWhileCalendarCQExists() {
    TNodeVersionInfo unsupportedVersion = new TNodeVersionInfo("old", "old");
    TConfigNodeRegisterReq registerReq =
        new TConfigNodeRegisterReq().setConfigNodeLocation(firstCandidate);
    registerReq.setVersionInfo(unsupportedVersion);

    // The CQ manager is mocked in setUp; make the persisted metadata barrier active for this test.
    Mockito.when(cqManager.hasCalendarDurationCQ()).thenReturn(true);

    TConfigNodeRegisterResp registerResp = nodeManager.registerConfigNode(registerReq);
    Assert.assertEquals(
        TSStatusCode.SEMANTIC_ERROR.getStatusCode(), registerResp.getStatus().getCode());
    Assert.assertEquals(-1, registerResp.getConfigNodeId());
    Assert.assertEquals(
        TSStatusCode.SEMANTIC_ERROR.getStatusCode(),
        nodeManager
            .updateConfigNodeIfNecessary(firstCandidate.getConfigNodeId(), unsupportedVersion)
            .getCode());
    Mockito.verify(nodeInfo, Mockito.never()).generateNextNodeId();
  }

  @Test
  public void transferLeaderShouldRedirectToActualLeaderAfterFailedResponse() throws Exception {
    Mockito.when(consensusManager.getLeaderLocation())
        .thenReturn(removedConfigNode, secondCandidate);
    Mockito.doThrow(new RatisRequestFailedException(new Exception("transfer failed")))
        .when(consensus)
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    TSStatus status =
        nodeManager.checkConfigNodeBeforeRemove(new RemoveConfigNodePlan(removedConfigNode));

    Assert.assertEquals(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), status.getCode());
    Assert.assertEquals(secondCandidate.getInternalEndPoint(), status.getRedirectNode());
    Mockito.verify(consensus)
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));
  }

  @Test
  public void transferLeaderShouldFailAfterAllCandidatesFail() throws Exception {
    Mockito.when(consensusManager.getLeaderLocation())
        .thenReturn(removedConfigNode, removedConfigNode, removedConfigNode);
    Mockito.doThrow(new RatisRequestFailedException(new Exception("transfer failed")))
        .when(consensus)
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    TSStatus status =
        nodeManager.checkConfigNodeBeforeRemove(new RemoveConfigNodePlan(removedConfigNode));

    Assert.assertEquals(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(consensus, Mockito.times(2))
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));
  }

  @Test
  public void transferLeaderShouldNotRetryNonRatisFailure() throws Exception {
    Mockito.when(consensusManager.getLeaderLocation()).thenReturn(removedConfigNode);
    Mockito.doThrow(new ConsensusException("non-retriable failure"))
        .when(consensus)
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    TSStatus status =
        nodeManager.checkConfigNodeBeforeRemove(new RemoveConfigNodePlan(removedConfigNode));

    Assert.assertEquals(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(consensus)
        .transferLeader(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));
  }

  private static TConfigNodeLocation newConfigNodeLocation(int configNodeId) {
    return new TConfigNodeLocation(
        configNodeId,
        new TEndPoint("127.0.0.1", 10710 + configNodeId),
        new TEndPoint("127.0.0.1", 10720 + configNodeId));
  }
}
