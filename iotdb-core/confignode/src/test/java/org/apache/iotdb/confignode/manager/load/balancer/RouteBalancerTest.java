/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.AddRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.apache.tsfile.utils.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RouteBalancerTest {

  private static final String DATABASE = "root.routes";
  private final PartitionInfo partitionInfo = new PartitionInfo();
  private final IManager configManager = mock(IManager.class);
  private final LoadManager loadManager = mock(LoadManager.class);
  private PartitionManager partitionManager;
  private final List<Pair<Long, Map<TConsensusGroupId, TRegionReplicaSet>>> pushed =
      new ArrayList<>();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    partitionManager = mock(PartitionManager.class);
    when(partitionManager.getAllActiveReplicaSetsSnapshot())
        .thenAnswer(invocation -> partitionInfo.getAllActiveReplicaSetsSnapshot());
    when(partitionManager.getRegionReplicaSetVersion())
        .thenAnswer(invocation -> partitionInfo.getRegionReplicaSetVersion());
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.getRegionLeaderMap()).thenReturn(Map.of());
    createDatabase(DATABASE);
  }

  @Test
  public void testCreateDuringBalanceDoesNotPublishPartialMap() {
    final TRegionReplicaSet data = addRegion(DATABASE, TConsensusGroupType.DataRegion, 0);
    final TRegionReplicaSet schema = newReplicaSet(TConsensusGroupType.SchemaRegion, 1);
    final RouteBalancer balancer = newRouteBalancer();
    balancer.startLeaderEpoch(1, () -> true);
    when(loadManager.getRegionLeaderMap())
        .thenAnswer(
            invocation -> {
              createRegion(DATABASE, schema);
              return Map.of();
            })
        .thenReturn(Map.of());

    balancer.refreshRegionPriorityIfNeeded();

    assertPublished(balancer, 1, Set.of(data.getRegionId(), schema.getRegionId()));
  }

  @Test
  public void testPublishedSnapshotIsRejectedAfterRegionCreation() {
    final TRegionReplicaSet data = addRegion(DATABASE, TConsensusGroupType.DataRegion, 0);
    final RouteBalancer balancer = newRouteBalancer();
    balancer.startLeaderEpoch(1, () -> true);
    balancer.refreshRegionPriorityIfNeeded();
    assertPublished(balancer, 1, Set.of(data.getRegionId()));

    final TRegionReplicaSet schema = newReplicaSet(TConsensusGroupType.SchemaRegion, 1);
    createRegion(DATABASE, schema);

    assertNull(balancer.getRegionPriorityMapSnapshot());
  }

  @Test
  public void testPublishedSnapshotIsRejectedAfterDatabasePreDelete() {
    final TRegionReplicaSet data = addRegion(DATABASE, TConsensusGroupType.DataRegion, 0);
    final RouteBalancer balancer = newRouteBalancer();
    balancer.startLeaderEpoch(1, () -> true);
    balancer.refreshRegionPriorityIfNeeded();
    assertPublished(balancer, 1, Set.of(data.getRegionId()));

    partitionInfo.preDeleteDatabase(
        new PreDeleteDatabasePlan(DATABASE, PreDeleteDatabasePlan.PreDeleteType.EXECUTE));

    assertNull(balancer.getRegionPriorityMapSnapshot());
  }

  @Test
  public void testConcurrentRegionChangesCannotReturnStaleSnapshot() throws Exception {
    final TRegionReplicaSet firstRegion = addRegion(DATABASE, TConsensusGroupType.DataRegion, 0);
    final RouteBalancer balancer = newRouteBalancer();
    balancer.startLeaderEpoch(1, () -> true);
    balancer.refreshRegionPriorityIfNeeded();
    assertPublished(balancer, 1, Set.of(firstRegion.getRegionId()));

    final TRegionReplicaSet secondRegion = newReplicaSet(TConsensusGroupType.SchemaRegion, 1);
    assertSnapshotRejectedDuringChange(balancer, () -> createRegion(DATABASE, secondRegion));

    balancer.refreshRegionPriorityIfNeeded();
    assertPublished(balancer, 2, Set.of(firstRegion.getRegionId(), secondRegion.getRegionId()));
    assertSnapshotRejectedDuringChange(
        balancer,
        () ->
            partitionInfo.preDeleteDatabase(
                new PreDeleteDatabasePlan(DATABASE, PreDeleteDatabasePlan.PreDeleteType.EXECUTE)));
  }

  @Test
  public void testPreDeleteDuringBalanceDoesNotPublishObsoleteMap() {
    addRegion(DATABASE, TConsensusGroupType.SchemaRegion, 0);
    addRegion(DATABASE, TConsensusGroupType.DataRegion, 1);
    createDatabase("root.keep");
    final TRegionReplicaSet kept = addRegion("root.keep", TConsensusGroupType.DataRegion, 2);
    final RouteBalancer balancer = newRouteBalancer();
    balancer.startLeaderEpoch(1, () -> true);
    balancer.refreshRegionPriorityIfNeeded();
    final long initialVersion = balancer.getRegionPriorityMapSnapshot().getLeft();
    createDatabase("root.empty");
    when(loadManager.getRegionLeaderMap())
        .thenAnswer(
            invocation -> {
              partitionInfo.preDeleteDatabase(
                  new PreDeleteDatabasePlan(DATABASE, PreDeleteDatabasePlan.PreDeleteType.EXECUTE));
              return Map.of();
            })
        .thenReturn(Map.of());

    balancer.refreshRegionPriorityIfNeeded();

    final long deletionVersion = assertPublished(balancer, 2, Set.of(kept.getRegionId()));
    assertTrue(deletionVersion > initialVersion);
    partitionInfo.deleteDatabase(new DeleteDatabasePlan(DATABASE));
    balancer.refreshRegionPriorityIfNeeded();
    assertEquals(deletionVersion, assertPublished(balancer, 2, Set.of(kept.getRegionId())));
  }

  @Test
  public void testLosingLeadershipDuringBalanceInvalidatesSnapshot() {
    final RouteBalancer balancer = newRouteBalancer();
    final AtomicBoolean leader = new AtomicBoolean(true);
    balancer.startLeaderEpoch(10, leader::get);
    balancer.refreshRegionPriorityIfNeeded();
    final long oldVersion = assertPublished(balancer, 1, Set.of());
    final TRegionReplicaSet schema = addRegion(DATABASE, TConsensusGroupType.SchemaRegion, 0);
    when(loadManager.getRegionLeaderMap())
        .thenAnswer(
            invocation -> {
              leader.set(false);
              return Map.of();
            })
        .thenReturn(Map.of());

    balancer.refreshRegionPriorityIfNeeded();

    assertNull(balancer.getRegionPriorityMapSnapshot());
    assertEquals(1, pushed.size());
    balancer.startLeaderEpoch(11, () -> true);
    balancer.refreshRegionPriorityIfNeeded();
    assertTrue(assertPublished(balancer, 2, Set.of(schema.getRegionId())) > oldVersion);
  }

  @Test
  public void testPersistentLeaderEpochOrdersRouteMapVersionsAcrossRestart() throws Exception {
    final Path systemDir = temporaryFolder.newFolder().toPath();
    final RouteBalancer firstProcess = newRouteBalancer();
    firstProcess.startLeaderEpochFromPersistentState(systemDir, () -> true);
    firstProcess.refreshRegionPriorityIfNeeded();
    final long firstVersion = assertPublished(firstProcess, 1, Set.of());
    final RouteBalancer restartedProcess = newRouteBalancer();
    restartedProcess.startLeaderEpochFromPersistentState(systemDir, () -> true);
    restartedProcess.refreshRegionPriorityIfNeeded();

    assertTrue(assertPublished(restartedProcess, 2, Set.of()) > firstVersion);
  }

  @Test
  public void testLeaderEpochRetriesWhenLogicalClockIsInitiallyUnavailable() {
    final AtomicLong logicalClock = new AtomicLong();
    final RouteBalancer balancer = newRouteBalancer();
    balancer.startLeaderEpoch(logicalClock::get, () -> true);

    balancer.refreshRegionPriorityIfNeeded();
    assertNull(balancer.getRegionPriorityMapSnapshot());

    logicalClock.set(1);
    balancer.refreshRegionPriorityIfNeeded();
    assertPublished(balancer, 1, Set.of());
  }

  @Test
  public void testReplicaLocationChangesPublishNewRouteVersions() {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 3);
    final TDataNodeLocation firstLocation = newDataNodeLocation(1, 1000);
    final TDataNodeLocation secondLocation = newDataNodeLocation(2, 2000);
    createRegion(
        DATABASE, new TRegionReplicaSet(regionId, new ArrayList<>(List.of(firstLocation))));
    final RouteBalancer balancer = newRouteBalancer();
    balancer.startLeaderEpoch(1, () -> true);

    balancer.refreshRegionPriorityIfNeeded();
    final long initialTimestamp = assertPublished(balancer, 1, Set.of(regionId));

    partitionInfo.addRegionLocation(new AddRegionLocationPlan(regionId, secondLocation));
    balancer.refreshRegionPriorityIfNeeded();
    final long addedTimestamp = assertPublished(balancer, 2, Set.of(regionId));
    assertTrue(addedTimestamp > initialTimestamp);
    assertEquals(
        List.of(firstLocation, secondLocation),
        balancer.getRegionPriorityMapSnapshot().getRight().get(regionId).getDataNodeLocations());

    final TDataNodeLocation updatedSecondLocation = newDataNodeLocation(2, 3000);
    partitionInfo.updateDataNode(
        new UpdateDataNodePlan(new TDataNodeConfiguration().setLocation(updatedSecondLocation)));
    balancer.refreshRegionPriorityIfNeeded();
    final long updatedTimestamp = assertPublished(balancer, 3, Set.of(regionId));
    assertTrue(updatedTimestamp > addedTimestamp);
    assertEquals(
        List.of(firstLocation, updatedSecondLocation),
        balancer.getRegionPriorityMapSnapshot().getRight().get(regionId).getDataNodeLocations());

    partitionInfo.removeRegionLocation(new RemoveRegionLocationPlan(regionId, firstLocation));
    balancer.refreshRegionPriorityIfNeeded();
    final long removedTimestamp = assertPublished(balancer, 4, Set.of(regionId));
    assertTrue(removedTimestamp > updatedTimestamp);
    assertEquals(
        List.of(updatedSecondLocation),
        balancer.getRegionPriorityMapSnapshot().getRight().get(regionId).getDataNodeLocations());
  }

  private long assertPublished(
      RouteBalancer balancer, int broadcasts, Set<TConsensusGroupId> expectedIds) {
    final Pair<Long, Map<TConsensusGroupId, TRegionReplicaSet>> snapshot =
        balancer.getRegionPriorityMapSnapshot();
    assertEquals(expectedIds, snapshot.getRight().keySet());
    assertEquals(broadcasts, pushed.size());
    assertEquals(pushed.get(broadcasts - 1).getLeft(), snapshot.getLeft());
    assertEquals(pushed.get(broadcasts - 1).getRight(), snapshot.getRight());
    return snapshot.getLeft();
  }

  private void assertSnapshotRejectedDuringChange(RouteBalancer balancer, Runnable change)
      throws Exception {
    final AtomicInteger versionReads = new AtomicInteger();
    final CountDownLatch beforeFinalVersionCheck = new CountDownLatch(1);
    final CountDownLatch allowFinalVersionCheck = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              if (versionReads.incrementAndGet() == 2) {
                beforeFinalVersionCheck.countDown();
                assertTrue(allowFinalVersionCheck.await(5, TimeUnit.SECONDS));
              }
              return partitionInfo.getRegionReplicaSetVersion();
            })
        .when(partitionManager)
        .getRegionReplicaSetVersion();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<Pair<Long, Map<TConsensusGroupId, TRegionReplicaSet>>> result =
          executor.submit(balancer::getRegionPriorityMapSnapshot);
      assertTrue(beforeFinalVersionCheck.await(5, TimeUnit.SECONDS));
      change.run();
      allowFinalVersionCheck.countDown();
      assertNull(result.get(5, TimeUnit.SECONDS));
    } finally {
      allowFinalVersionCheck.countDown();
      executor.shutdownNow();
    }
  }

  private RouteBalancer newRouteBalancer() {
    return new RouteBalancer(configManager) {
      @Override
      void broadcastRegionPriorityMap(
          Pair<Long, Map<TConsensusGroupId, TRegionReplicaSet>> snapshot,
          BooleanSupplier leaderEpoch) {
        pushed.add(snapshot);
      }
    };
  }

  private void createDatabase(String name) {
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(name)));
  }

  private TRegionReplicaSet addRegion(String database, TConsensusGroupType type, int id) {
    final TRegionReplicaSet replicaSet = newReplicaSet(type, id);
    createRegion(database, replicaSet);
    return replicaSet;
  }

  private void createRegion(String database, TRegionReplicaSet replicaSet) {
    final CreateRegionGroupsPlan plan = new CreateRegionGroupsPlan();
    plan.addRegionGroup(database, replicaSet);
    partitionInfo.createRegionGroups(plan);
  }

  private TRegionReplicaSet newReplicaSet(TConsensusGroupType type, int id) {
    return new TRegionReplicaSet(new TConsensusGroupId(type, id), List.of());
  }

  private TDataNodeLocation newDataNodeLocation(int dataNodeId, int endpointPort) {
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(new TEndPoint("127.0.0.1", endpointPort))
        .setInternalEndPoint(new TEndPoint("127.0.0.1", endpointPort + 1))
        .setMPPDataExchangeEndPoint(new TEndPoint("127.0.0.1", endpointPort + 2))
        .setDataRegionConsensusEndPoint(new TEndPoint("127.0.0.1", endpointPort + 3))
        .setSchemaRegionConsensusEndPoint(new TEndPoint("127.0.0.1", endpointPort + 4));
  }
}
