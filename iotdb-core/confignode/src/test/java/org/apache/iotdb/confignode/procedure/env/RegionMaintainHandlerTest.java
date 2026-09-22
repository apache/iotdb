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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class RegionMaintainHandlerTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> regionTypes() {
    return Arrays.asList(
        new Object[] {TConsensusGroupType.DataRegion},
        new Object[] {TConsensusGroupType.SchemaRegion});
  }

  @Parameterized.Parameter public TConsensusGroupType regionType;

  private final TDataNodeLocation original =
      new TDataNodeLocation()
          .setDataNodeId(1)
          .setInternalEndPoint(new TEndPoint("127.0.0.1", 10730));
  private final TDataNodeLocation coordinator = new TDataNodeLocation().setDataNodeId(2);
  private final TDataNodeLocation otherReplica = new TDataNodeLocation().setDataNodeId(3);
  private final TDataNodeLocation nonReplica = new TDataNodeLocation().setDataNodeId(4);

  private TConsensusGroupId regionId;
  private LoadManager loadManager;
  private RegionMaintainHandler handler;
  private String originalConsensusProtocol;

  @Before
  public void setUp() {
    originalConsensusProtocol =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(RATIS_CONSENSUS);

    regionId = new TConsensusGroupId(regionType, 1);
    ConfigManager configManager = mock(ConfigManager.class);
    NodeManager nodeManager = mock(NodeManager.class);
    PartitionManager partitionManager = mock(PartitionManager.class);
    loadManager = mock(LoadManager.class);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(partitionManager.getAllReplicaSets())
        .thenReturn(
            Collections.singletonList(
                new TRegionReplicaSet(
                    regionId, Arrays.asList(original, coordinator, otherReplica))));
    when(nodeManager.filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly))
        .thenReturn(
            Arrays.asList(
                new TDataNodeConfiguration().setLocation(original),
                new TDataNodeConfiguration().setLocation(coordinator),
                new TDataNodeConfiguration().setLocation(otherReplica),
                new TDataNodeConfiguration().setLocation(nonReplica)));
    when(loadManager.getRegionLeaderMap()).thenReturn(Collections.emptyMap());
    handler = new RegionMaintainHandler(configManager);
  }

  @After
  public void tearDown() {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(originalConsensusProtocol);
  }

  @Test
  public void testFilterReplicaWithoutLeaderCache() {
    assertEquals(
        Optional.of(otherReplica),
        handler.filterDataNodeWithOtherRegionReplica(
            regionId, Arrays.asList(original, coordinator)));
  }

  @Test
  public void testFilterReplicaWithUnknownLeader() {
    when(loadManager.getRegionLeaderMap()).thenReturn(Collections.singletonMap(regionId, -1));
    assertEquals(
        Optional.of(otherReplica),
        handler.filterDataNodeWithOtherRegionReplica(
            regionId, Arrays.asList(original, coordinator)));
  }

  @Test
  public void testFilterReplicaPrefersKnownLeader() {
    when(loadManager.getRegionLeaderMap())
        .thenReturn(Collections.singletonMap(regionId, otherReplica.getDataNodeId()));
    assertEquals(
        Optional.of(otherReplica),
        handler.filterDataNodeWithOtherRegionReplica(regionId, original));
  }

  @Test
  public void testFilterReplicaWithoutEligibleReplica() {
    assertFalse(
        handler
            .filterDataNodeWithOtherRegionReplica(
                regionId, Arrays.asList(original, coordinator, otherReplica))
            .isPresent());
  }

  @Test
  public void testTransferLeaderWithoutLeaderCache() throws Exception {
    assertLeaderTransfer();
  }

  @Test
  public void testTransferLeaderWithUnknownLeader() throws Exception {
    when(loadManager.getRegionLeaderMap()).thenReturn(Collections.singletonMap(regionId, -1));
    assertLeaderTransfer();
  }

  @Test
  public void testTransferLeaderWhenCacheEntryDisappears() throws Exception {
    // The cache can change between replica selection and the transfer's leader lookup.
    when(loadManager.getRegionLeaderMap())
        .thenReturn(Collections.singletonMap(regionId, -1))
        .thenReturn(Collections.emptyMap());
    assertLeaderTransfer();
  }

  @SuppressWarnings("unchecked")
  private void assertLeaderTransfer() throws Exception {
    RouteBalancer routeBalancer = mock(RouteBalancer.class);
    when(loadManager.getRouteBalancer()).thenReturn(routeBalancer);
    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
        mock(IClientManager.class);
    SyncDataNodeInternalServiceClient client = mock(SyncDataNodeInternalServiceClient.class);
    when(clientManager.borrowClient(original.getInternalEndPoint())).thenReturn(client);
    TRegionLeaderChangeReq request = new TRegionLeaderChangeReq(regionId, otherReplica);
    long timestamp = 123L;
    when(client.changeRegionLeader(request))
        .thenReturn(
            new TRegionLeaderChangeResp(
                new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), timestamp));

    SyncDataNodeClientPool clientPool = SyncDataNodeClientPool.getInstance();
    Field clientManagerField = SyncDataNodeClientPool.class.getDeclaredField("clientManager");
    clientManagerField.setAccessible(true);
    Object originalClientManager = clientManagerField.get(clientPool);
    try {
      clientManagerField.set(clientPool, clientManager);
      handler.transferRegionLeader(regionId, original, coordinator);

      verify(client).changeRegionLeader(request);
      ArgumentCaptor<Map<TConsensusGroupId, ConsensusGroupHeartbeatSample>> cacheUpdate =
          ArgumentCaptor.forClass(Map.class);
      verify(loadManager).forceUpdateConsensusGroupCache(cacheUpdate.capture());
      assertEquals(1, cacheUpdate.getValue().size());
      ConsensusGroupHeartbeatSample sample = cacheUpdate.getValue().get(regionId);
      assertEquals(otherReplica.getDataNodeId(), sample.getLeaderId());
      assertEquals(timestamp, sample.getSampleLogicalTimestamp());
      verify(routeBalancer).balanceRegionLeaderAndPriority();
    } finally {
      clientManagerField.set(clientPool, originalClientManager);
    }
  }
}
