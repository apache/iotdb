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

package org.apache.iotdb.confignode.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ConfigNodeShutdownHookTest {

  private static final CnToCnNodeRequestType REPORT =
      CnToCnNodeRequestType.REPORT_CONFIG_NODE_SHUTDOWN;

  private final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
  private final CommonConfig commonConf = CommonDescriptor.getInstance().getConfig();
  private final ConfigNode configNode = mock(ConfigNode.class);
  private final NodeManager nodeManager = mock(NodeManager.class);
  private final LoadManager loadManager = mock(LoadManager.class);
  private final ConsensusManager consensusManager = mock(ConsensusManager.class);
  private final IConsensus consensus = mock(IConsensus.class);
  private final SyncConfigNodeClientPool clientPool = mock(SyncConfigNodeClientPool.class);
  private final TEndPoint seed = new TEndPoint("127.0.0.2", 10710);
  private final TEndPoint peer = new TEndPoint("127.0.0.3", 10710);
  private final List<TConfigNodeLocation> registeredNodes = new ArrayList<>();

  private TConfigNodeLocation localNode;
  private TEndPoint previousSeed;
  private NodeStatus previousStatus;
  private String previousReason;
  private int previousConnectionTimeout;

  @Before
  public void setUp() {
    previousSeed = conf.getSeedConfigNode();
    previousStatus = commonConf.getNodeStatus();
    previousReason = commonConf.getStatusReason();
    previousConnectionTimeout = commonConf.getCnConnectionTimeoutInMS();
    commonConf.setCnConnectionTimeoutInMS(5000);
    localNode =
        new TConfigNodeLocation(
            conf.getConfigNodeId(),
            new TEndPoint(conf.getInternalAddress(), conf.getInternalPort()),
            new TEndPoint(conf.getInternalAddress(), conf.getConsensusPort()));
    conf.setSeedConfigNode(seed);
    registeredNodes.add(localNode);
    registeredNodes.add(new TConfigNodeLocation(100, peer, new TEndPoint("127.0.0.3", 10720)));
    ConfigManager configManager = mock(ConfigManager.class);
    when(configNode.getConfigManager()).thenReturn(configManager);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(consensusManager.getConsensusImpl()).thenReturn(consensus);
    when(consensusManager.getConsensusGroupId())
        .thenReturn(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID);
    when(nodeManager.getRegisteredConfigNodes()).thenReturn(registeredNodes);
    when(loadManager.getNodeStatus(100)).thenReturn(NodeStatus.Running);
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(any(), any(), eq(REPORT)))
        .thenReturn(new TSStatus(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode()));
  }

  @After
  public void tearDown() {
    conf.setSeedConfigNode(previousSeed);
    commonConf.setNodeStatusWithReason(previousStatus, previousReason);
    commonConf.setCnConnectionTimeoutInMS(previousConnectionTimeout);
  }

  @Test
  public void testStoppingSeedReportsToPeerAfterDeactivation() throws Exception {
    conf.setSeedConfigNode(localNode.getInternalEndPoint());
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    // Deactivation may make local metadata unavailable; the report must use its saved endpoints.
    doAnswer(
            invocation -> {
              registeredNodes.clear();
              return null;
            })
        .when(configNode)
        .deactivate();

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    InOrder order = inOrder(nodeManager, configNode, clientPool);
    order.verify(nodeManager).getRegisteredConfigNodes();
    order.verify(configNode).deactivate();
    order.verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    verify(consensus, never()).transferLeader(any(), any());
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testLeaderTransfersBeforeDeactivation() throws Exception {
    conf.setSeedConfigNode(localNode.getInternalEndPoint());
    when(consensusManager.isLeader()).thenReturn(true);
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    InOrder order = inOrder(consensus, configNode, clientPool);
    order
        .verify(consensus)
        .transferLeader(
            ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID,
            new Peer(
                ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID,
                100,
                registeredNodes.get(1).getConsensusEndPoint()));
    order.verify(configNode).deactivate();
    order.verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    verify(nodeManager).getRegisteredConfigNodes();
    verifyNoMoreInteractions(consensus, clientPool);
  }

  @Test
  public void testTransferFailureStillDeactivatesAndReports() throws Exception {
    conf.setSeedConfigNode(localNode.getInternalEndPoint());
    when(consensusManager.isLeader()).thenReturn(true);
    doThrow(new ConsensusException("transfer timed out"))
        .when(consensus)
        .transferLeader(any(), any());
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    InOrder order = inOrder(consensus, configNode, clientPool);
    order.verify(consensus).transferLeader(any(), any());
    order.verify(configNode).deactivate();
    order.verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
  }

  @Test
  public void testLeaderWithoutRunningPeerStillShutsDown() throws Exception {
    conf.setSeedConfigNode(localNode.getInternalEndPoint());
    when(consensusManager.isLeader()).thenReturn(true);
    when(loadManager.getNodeStatus(100)).thenReturn(NodeStatus.Unknown);
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verify(consensus, never()).transferLeader(any(), any());
    verify(configNode).deactivate();
    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
  }

  @Test
  public void testReportRetriesUntilNewLeaderIsReady() {
    conf.setSeedConfigNode(localNode.getInternalEndPoint());
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(
            new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()),
            new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()),
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verify(clientPool, times(3)).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testUnavailableRegisteredNodeFallsBackToPeer() {
    registeredNodes.add(1, new TConfigNodeLocation(101, seed, new TEndPoint("127.0.0.2", 10720)));
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verify(clientPool, times(2)).sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT);
    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testFollowerReportsWithoutStatusCacheOrSeed() {
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verifyNoMoreInteractions(loadManager, consensus);
    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testSeedIsUsedOnlyWhenNoRegisteredPeersExist() {
    registeredNodes.remove(1);
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testSeedIsUsedBeforeConfigManagerInitialization() {
    when(configNode.getConfigManager()).thenReturn(null);
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testLocalSeedWithoutPeersDoesNotReportToItself() {
    registeredNodes.remove(1);
    conf.setSeedConfigNode(localNode.getInternalEndPoint());

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testReportFollowsRedirect() {
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(
            new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()).setRedirectNode(seed));
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    InOrder order = inOrder(clientPool);
    order.verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    order.verify(clientPool).sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testRedirectToStoppingNodeFallsBackToPeer() {
    registeredNodes.add(1, new TConfigNodeLocation(101, seed, new TEndPoint("127.0.0.2", 10720)));
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT))
        .thenReturn(
            new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                .setRedirectNode(localNode.getInternalEndPoint()));
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(seed, localNode, REPORT);
    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    verify(clientPool, never())
        .sendSyncRequestToConfigNodeWithRetry(localNode.getInternalEndPoint(), localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testReportStopsRetryingAfterDeadline() {
    commonConf.setCnConnectionTimeoutInMS(100);
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenAnswer(
            invocation -> {
              TimeUnit.MILLISECONDS.sleep(150);
              return new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
            });

    new ConfigNodeShutdownHook(configNode, clientPool).run();

    verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
    verifyNoMoreInteractions(clientPool);
  }

  @Test
  public void testReportStopsRetryingWhenInterrupted() {
    when(clientPool.sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode());
            });
    try {
      new ConfigNodeShutdownHook(configNode, clientPool).run();

      assertTrue(Thread.currentThread().isInterrupted());
      verify(clientPool).sendSyncRequestToConfigNodeWithRetry(peer, localNode, REPORT);
      verifyNoMoreInteractions(clientPool);
    } finally {
      Thread.interrupted();
    }
  }
}
