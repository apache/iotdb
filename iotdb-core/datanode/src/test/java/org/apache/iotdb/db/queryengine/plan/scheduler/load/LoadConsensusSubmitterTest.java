/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Pins how the write peer of a LOAD consensus command is resolved.
 *
 * <p>A Ratis leader is reported as a {@link Peer} that carries the DataNode id of the leader and no
 * endpoint at all, so the route has to be matched by node id. Matching by endpoint instead threw
 * while every command was being dispatched, before anything had been sent.
 */
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataRegionConsensusImpl.class, ClusterPartitionFetcher.class})
public class LoadConsensusSubmitterTest {

  private static final int REGION_ID = 1;

  private IoTDBConfig config;
  private String originalProtocolClass;

  @Before
  public void setUp() {
    config = IoTDBDescriptor.getInstance().getConfig();
    originalProtocolClass = config.getDataRegionConsensusProtocolClass();
  }

  @After
  public void tearDown() {
    config.setDataRegionConsensusProtocolClass(originalProtocolClass);
  }

  @Test
  public void testRatisLeaderIsMatchedByItsNodeId() throws Exception {
    final TRegionReplicaSet replicaSet =
        replicaSet(location(11, "127.0.0.1", 9003), location(12, "127.0.0.2", 9003));
    stubLeader(12);

    final TDataNodeLocation writePeer =
        resolveWritePeer(replicaSet, ConsensusFactory.RATIS_CONSENSUS);

    assertEquals(12, writePeer.getDataNodeId());
  }

  @Test
  public void testRatisLeaderOutsideTheRouteIsNotReplacedByAnotherReplica() throws Exception {
    final TRegionReplicaSet replicaSet =
        replicaSet(location(11, "127.0.0.1", 9003), location(12, "127.0.0.2", 9003));
    // The leader of the partition is not in the route the coordinator holds: the route is stale,
    // and
    // sending the command to a follower would stage the pieces on a node that cannot commit them,
    // so
    // no peer is resolved at all.
    stubLeader(99);

    assertNull(resolveWritePeer(replicaSet, ConsensusFactory.RATIS_CONSENSUS));
  }

  @Test
  public void testIoTConsensusWritesToTheWriteNodeOfTheRoute() throws Exception {
    final TRegionReplicaSet replicaSet =
        replicaSet(location(11, "127.0.0.1", 9003), location(12, "127.0.0.2", 9003));
    stubLeader(12);

    // IoTConsensus has no leader to follow: its write node is the first location of the route,
    // which
    // is the same target the ordinary write path dispatches to.
    final TDataNodeLocation writePeer =
        resolveWritePeer(replicaSet, ConsensusFactory.IOT_CONSENSUS);

    assertEquals(11, writePeer.getDataNodeId());
  }

  @Test
  public void testCommandIsNotSentWhenNoWritePeerCanBeResolved() {
    final TRegionReplicaSet replicaSet =
        replicaSet(location(11, "127.0.0.1", 9003), location(12, "127.0.0.2", 9003));
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    stubLeader(99);

    final LoadTsFileConsensusNode node =
        LoadTsFileConsensusNode.begin(
            new PlanNodeId("load-begin"), "load-id", "file-1", false, null, 0);
    final TSStatusCode status =
        TSStatusCode.representOf(newSubmitter().submit(replicaSet, node).getCode());

    assertEquals(TSStatusCode.DISPATCH_ERROR, status);
  }

  /**
   * The commands of one transaction all go to the replica set the splitter resolved, whatever the
   * local partition table holds now: a route refreshed in the middle of a transaction would send
   * the rest of a task to regions that hold a different plan, while the staged bytes stay where the
   * pieces were written.
   */
  @Test
  public void testTheRouteOfATransactionIsNotRefreshedBetweenItsCommands() {
    final TRegionReplicaSet pinnedRoute =
        replicaSet(location(11, "127.0.0.1", 9003), location(12, "127.0.0.2", 9003));
    final TRegionReplicaSet freshRoute =
        replicaSet(location(21, "127.0.0.3", 9003), location(22, "127.0.0.4", 9003));
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    stubLeader(99);

    final ClusterPartitionFetcher partitionFetcher = Mockito.mock(ClusterPartitionFetcher.class);
    PowerMockito.mockStatic(ClusterPartitionFetcher.class);
    PowerMockito.when(ClusterPartitionFetcher.getInstance()).thenReturn(partitionFetcher);
    Mockito.when(partitionFetcher.getRegionReplicaSet(Mockito.any()))
        .thenReturn(Collections.singletonList(freshRoute));

    final LoadTsFileConsensusNode node =
        LoadTsFileConsensusNode.begin(
            new PlanNodeId("load-begin"), "load-id", "file-1", false, null, 0);
    newSubmitter().submit(pinnedRoute, node);

    // The command was prepared for the pinned route, and the refreshed one was not consulted.
    assertEquals(pinnedRoute, node.getRegionReplicaSet());
  }

  /**
   * Replaces the local consensus layer with one whose leader of the region is {@code leaderNodeId}:
   * the node id of the leader, and no endpoint.
   */
  private static void stubLeader(final int leaderNodeId) {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    PowerMockito.mockStatic(DataRegionConsensusImpl.class);
    PowerMockito.when(DataRegionConsensusImpl.getInstance()).thenReturn(consensus);
    Mockito.when(consensus.getLeader(Mockito.any()))
        .thenReturn(new Peer(groupId(), leaderNodeId, null));
  }

  private static TDataNodeLocation resolveWritePeer(
      final TRegionReplicaSet replicaSet, final String protocol) {
    return newSubmitter().resolveWritePeer(replicaSet, groupId(), protocol);
  }

  @SuppressWarnings("unchecked")
  private static LoadConsensusSubmitter newSubmitter() {
    return new LoadConsensusSubmitter(
        (IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            Mockito.mock(IClientManager.class));
  }

  private static ConsensusGroupId groupId() {
    return ConsensusGroupId.Factory.createFromTConsensusGroupId(regionId());
  }

  private static TConsensusGroupId regionId() {
    return new TConsensusGroupId(TConsensusGroupType.DataRegion, REGION_ID);
  }

  private static TRegionReplicaSet replicaSet(final TDataNodeLocation... locations) {
    return new TRegionReplicaSet(regionId(), Arrays.asList(locations));
  }

  private static TDataNodeLocation location(final int dataNodeId, final String ip, final int port) {
    final TEndPoint endPoint = new TEndPoint(ip, port);
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(endPoint)
        .setInternalEndPoint(endPoint)
        .setMPPDataExchangeEndPoint(endPoint)
        .setDataRegionConsensusEndPoint(endPoint)
        .setSchemaRegionConsensusEndPoint(endPoint);
  }
}
