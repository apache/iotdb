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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The second phase of a consensus LOAD is a two-phase commit: every touched region is prepared
 * first and the commit is sent only once every one of them agreed. A region that cannot be prepared
 * therefore rolls the whole transaction back, and a commit that fails afterwards rolls nothing
 * back, because the regions that agreed must not be left with half a load on disk.
 */
public class TwoPhaseConsensusLoadStrategyAbortTest {

  private static final TConsensusGroupId REGION_1 =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private static final TConsensusGroupId REGION_2 =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);

  /**
   * Fails the given operation for the given region with a permanent error, accepts the rest. The
   * route of a region stays resolvable, so a command that is retried is retried on the route the
   * transaction pinned.
   */
  private static LoadConsensusSubmitter submitterFailingOn(
      final LoadTsFileConsensusOp op,
      final TConsensusGroupId regionId,
      final List<String> submitted) {
    return submitterFailingWith(op, regionId, submitted, TSStatusCode.LOAD_FILE_ERROR);
  }

  /** Fails the given operation for the given region with a retryable error, accepts the rest. */
  private static LoadConsensusSubmitter submitterFailingTransientlyOn(
      final LoadTsFileConsensusOp op,
      final TConsensusGroupId regionId,
      final List<String> submitted) {
    return submitterFailingWith(op, regionId, submitted, TSStatusCode.DISPATCH_ERROR);
  }

  private static LoadConsensusSubmitter submitterFailingWith(
      final LoadTsFileConsensusOp op,
      final TConsensusGroupId regionId,
      final List<String> submitted,
      final TSStatusCode failure) {
    final LoadConsensusSubmitter submitter = mock(LoadConsensusSubmitter.class);
    when(submitter.submit(any(), any()))
        .thenAnswer(
            invocation -> {
              final TRegionReplicaSet replicaSet = invocation.getArgument(0);
              final LoadTsFileConsensusNode node = invocation.getArgument(1);
              submitted.add(node.getOp().name() + "@" + replicaSet.getRegionId().getId());
              if (node.getOp() == op && replicaSet.getRegionId().equals(regionId)) {
                return RpcUtils.getStatus(failure);
              }
              return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
            });
    when(submitter.resolveRoute(any(), any(Integer.class)))
        .thenAnswer(invocation -> currentRoute(invocation.getArgument(0)));
    return submitter;
  }

  /** The route a region is pinned to while the commands of its task are being retried. */
  private static TRegionReplicaSet currentRoute(final TConsensusGroupId regionId) {
    return regionId.equals(REGION_1)
        ? new TRegionReplicaSet(REGION_1, Collections.singletonList(location(11)))
        : new TRegionReplicaSet(REGION_2, Collections.singletonList(location(12)));
  }

  private static TwoPhaseConsensusLoadStrategy strategy(
      final LoadConsensusSubmitter submitter,
      final Set<TRegionReplicaSet> replicaSets,
      final Map<TConsensusGroupId, String> loadIds,
      final Map<TConsensusGroupId, Long> pieceCounts)
      throws Exception {
    final TwoPhaseConsensusLoadStrategy strategy =
        new TwoPhaseConsensusLoadStrategy(
            mock(LoadTsFileDispatcherImpl.class),
            mock(DataPartitionBatchFetcher.class),
            mock(LoadTsFileDataCacheMemoryBlock.class),
            submitter,
            "root",
            false);
    setField(strategy, "regionRoutes", routesOf(replicaSets));
    setField(strategy, "regionLoadIds", loadIds);
    setField(strategy, "regionPieceCounts", pieceCounts);
    setField(strategy, "regionTotalBytes", new LinkedHashMap<TConsensusGroupId, Long>());
    return strategy;
  }

  private static void setField(final Object target, final String name, final Object value)
      throws Exception {
    final Field field = TwoPhaseConsensusLoadStrategy.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static boolean prepareAndCommit(
      final TwoPhaseConsensusLoadStrategy strategy, final LoadSingleTsFileNode node)
      throws Exception {
    final Method method =
        TwoPhaseConsensusLoadStrategy.class.getDeclaredMethod(
            "prepareAndCommitAllRegions", LoadSingleTsFileNode.class);
    method.setAccessible(true);
    return (boolean) method.invoke(strategy, node);
  }

  /** The route map a transaction holds, built from the replica sets of its regions. */
  private static Map<TConsensusGroupId, TRegionReplicaSet> routesOf(
      final Set<TRegionReplicaSet> replicaSets) {
    final Map<TConsensusGroupId, TRegionReplicaSet> routes = new LinkedHashMap<>();
    for (final TRegionReplicaSet replicaSet : replicaSets) {
      routes.put(replicaSet.getRegionId(), replicaSet);
    }
    return routes;
  }

  private static Set<TRegionReplicaSet> twoRegionsInOrder() {
    final Set<TRegionReplicaSet> replicaSets = new LinkedHashSet<>();
    replicaSets.add(new TRegionReplicaSet(REGION_1, new ArrayList<>()));
    replicaSets.add(new TRegionReplicaSet(REGION_2, new ArrayList<>()));
    return replicaSets;
  }

  private static Map<TConsensusGroupId, String> loadIds() {
    final Map<TConsensusGroupId, String> loadIds = new LinkedHashMap<>();
    loadIds.put(REGION_1, "load-1");
    loadIds.put(REGION_2, "load-2");
    return loadIds;
  }

  private static Map<TConsensusGroupId, Long> pieceCounts() {
    final Map<TConsensusGroupId, Long> counts = new LinkedHashMap<>();
    counts.put(REGION_1, 3L);
    counts.put(REGION_2, 2L);
    return counts;
  }

  /**
   * A command that fails transiently is retried on the route the region has now: the route is
   * looked up again with the cache dropped first, and the route that changed is adopted by the
   * whole transaction, so the commands that follow a switched write node or a finished migration
   * are sent where the staged bytes of the task are.
   */
  @Test
  public void testAFailedCommandFollowsTheRouteTheRegionHasNow() throws Exception {
    final TRegionReplicaSet pinnedRoute =
        new TRegionReplicaSet(REGION_1, Collections.singletonList(location(11)));
    final TRegionReplicaSet freshRoute =
        new TRegionReplicaSet(REGION_1, Collections.singletonList(location(12)));
    final List<String> submitted = new ArrayList<>();
    final LoadConsensusSubmitter submitter = mock(LoadConsensusSubmitter.class);
    when(submitter.submit(any(), any()))
        .thenAnswer(
            invocation -> {
              final TRegionReplicaSet route = invocation.getArgument(0);
              submitted.add(
                  route.getRegionId().getId()
                      + "@"
                      + route.getDataNodeLocations().get(0).getDataNodeId());
              return route.equals(pinnedRoute)
                  ? RpcUtils.getStatus(TSStatusCode.DISPATCH_ERROR)
                  : RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
            });
    when(submitter.resolveRoute(any(), any(Integer.class))).thenReturn(freshRoute);

    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitter,
            new LinkedHashSet<>(Collections.singletonList(pinnedRoute)),
            loadIds(),
            pieceCounts());
    final TSStatusCode status = submitPiece(strategy, REGION_1);

    assertEquals(TSStatusCode.SUCCESS_STATUS, status);
    assertEquals(
        "1@11, then the retry on the route the region has now",
        Arrays.asList("1@11", "1@12"),
        submitted);
    assertEquals(freshRoute, routesOf(strategy).get(REGION_1));
  }

  /**
   * A route that cannot be looked up ends the command: the attempt that failed is reported instead
   * of the command being repeated on a route the partition table no longer knows, which is the
   * route a migration or a write-node switch took the region away from.
   */
  @Test
  public void testARouteThatCannotBeLookedUpEndsTheCommand() throws Exception {
    final TRegionReplicaSet pinnedRoute =
        new TRegionReplicaSet(REGION_1, Collections.singletonList(location(11)));
    final List<String> submitted = new ArrayList<>();
    final LoadConsensusSubmitter submitter = mock(LoadConsensusSubmitter.class);
    when(submitter.submit(any(), any()))
        .thenAnswer(
            invocation -> {
              final TRegionReplicaSet route = invocation.getArgument(0);
              submitted.add(
                  route.getRegionId().getId()
                      + "@"
                      + route.getDataNodeLocations().get(0).getDataNodeId());
              return RpcUtils.getStatus(TSStatusCode.DISPATCH_ERROR);
            });
    when(submitter.resolveRoute(any(), any(Integer.class))).thenReturn(null);

    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitter,
            new LinkedHashSet<>(Collections.singletonList(pinnedRoute)),
            loadIds(),
            pieceCounts());
    final TSStatusCode status = submitPiece(strategy, REGION_1);

    assertEquals(TSStatusCode.DISPATCH_ERROR, status);
    assertEquals(Collections.singletonList("1@11"), submitted);
  }

  /** Submits a piece for a region through the bounded submission of the strategy. */
  private static TSStatusCode submitPiece(
      final TwoPhaseConsensusLoadStrategy strategy, final TConsensusGroupId regionId)
      throws Exception {
    final Method method =
        TwoPhaseConsensusLoadStrategy.class.getDeclaredMethod(
            "submitConsensusWithRetry", TConsensusGroupId.class, LoadTsFileConsensusNode.class);
    method.setAccessible(true);
    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.piece(
            new PlanNodeId("load-piece"), "load-1", "file-1", 0L, new ArrayList<>());
    return TSStatusCode.representOf(
        ((org.apache.iotdb.common.rpc.thrift.TSStatus) method.invoke(strategy, regionId, piece))
            .getCode());
  }

  @SuppressWarnings("unchecked")
  private static Map<TConsensusGroupId, TRegionReplicaSet> routesOf(
      final TwoPhaseConsensusLoadStrategy strategy) throws Exception {
    final Field field = TwoPhaseConsensusLoadStrategy.class.getDeclaredField("regionRoutes");
    field.setAccessible(true);
    return (Map<TConsensusGroupId, TRegionReplicaSet>) field.get(strategy);
  }

  private static TDataNodeLocation location(final int dataNodeId) {
    final TEndPoint endPoint = new TEndPoint("127.0.0." + dataNodeId, 9003);
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(endPoint)
        .setInternalEndPoint(endPoint)
        .setMPPDataExchangeEndPoint(endPoint)
        .setDataRegionConsensusEndPoint(endPoint)
        .setSchemaRegionConsensusEndPoint(endPoint);
  }

  @Test
  public void testEveryTouchedRegionIsPreparedAndCommittedOnSuccess() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingOn(LoadTsFileConsensusOp.ABORT, null, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertTrue(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    // Both regions vote first: no region commits before every region was prepared.
    assertEquals(Arrays.asList("PREPARE@1", "PREPARE@2", "COMMIT@1", "COMMIT@2"), submitted);
  }

  /**
   * The failure that used to leave staged data behind: a region that refuses to prepare turns the
   * whole transaction into a rollback, so the region that already agreed does not keep an import
   * the other one refused.
   */
  @Test
  public void testPrepareFailureOfTheSecondRegionRollsBackEveryRegion() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingOn(LoadTsFileConsensusOp.PREPARE, REGION_2, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(Arrays.asList("PREPARE@1", "PREPARE@2", "ABORT@1", "ABORT@2"), submitted);
  }

  @Test
  public void testPrepareFailureOfTheFirstRegionAbortsEveryRegion() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingOn(LoadTsFileConsensusOp.PREPARE, REGION_1, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(Arrays.asList("PREPARE@1", "ABORT@1", "ABORT@2"), submitted);
  }

  /**
   * Once every region agreed, the decision cannot be undone: a commit that fails on one region must
   * not roll the other regions back, so the remaining regions are committed as well. The region
   * whose commit was not acknowledged is still sent the terminal ABORT of its task: a COMMIT that
   * failed can mean that it never landed, in which case the region would keep its staged directory
   * for a task nobody ever finishes.
   */
  @Test
  public void testCommitFailureDoesNotRollBackTheRegionsThatAgreed() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingOn(LoadTsFileConsensusOp.COMMIT, REGION_1, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    // The unresolved commit of region 1 is resolved before the next region is committed, so the
    // task of that region is left in exactly one of its two terminal states.
    assertEquals(
        Arrays.asList("PREPARE@1", "PREPARE@2", "COMMIT@1", "ABORT@1", "COMMIT@2"), submitted);
  }

  /**
   * A COMMIT whose answer is a permanent rejection is resolved as well: the task of that region is
   * ended one way or the other, so a region that never applied the COMMIT does not keep staged data
   * that nothing else refers to.
   */
  @Test
  public void testCommitRejectionIsResolvedWithTheTerminalAbort() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingOn(LoadTsFileConsensusOp.COMMIT, REGION_2, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(
        Arrays.asList("PREPARE@1", "PREPARE@2", "COMMIT@1", "COMMIT@2", "ABORT@2"), submitted);
  }

  /**
   * An ABORT that cannot reach the region does not turn into a wrong answer, and the attempts stay
   * bounded: the load reports the failure it hit rather than repeating a command forever on a route
   * that cannot be resolved.
   */
  @Test
  public void testAnUnresolvableRouteBoundsTheAttemptsOfTheRollback() throws Exception {
    final List<String> submitted = Collections.synchronizedList(new ArrayList<>());
    final LoadConsensusSubmitter submitter = mock(LoadConsensusSubmitter.class);
    when(submitter.submit(any(), any()))
        .thenAnswer(
            invocation -> {
              final TRegionReplicaSet replicaSet = invocation.getArgument(0);
              final LoadTsFileConsensusNode node = invocation.getArgument(1);
              submitted.add(node.getOp().name() + "@" + replicaSet.getRegionId().getId());
              return RpcUtils.getStatus(TSStatusCode.DISPATCH_ERROR);
            });
    when(submitter.resolveRoute(any(), any(Integer.class))).thenReturn(null);

    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(submitter, twoRegionsInOrder(), loadIds(), pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    // One failed attempt per region: the route is looked up again, comes back empty, and the
    // command is not repeated on a route the partition table does not know any more.
    assertEquals(Arrays.asList("PREPARE@1", "ABORT@1", "ABORT@2"), submitted);
  }

  /**
   * A COMMIT that failed transiently is repeated: the decision was taken, so a region that did not
   * answer yet must still receive it instead of keeping staged data nobody commits. Only once the
   * attempts are used up is the outcome resolved with the terminal ABORT of the task.
   */
  @Test
  public void testCommitIsRetriedOnTransientFailure() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingTransientlyOn(LoadTsFileConsensusOp.COMMIT, REGION_1, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(
        Arrays.asList(
            "PREPARE@1", "PREPARE@2", "COMMIT@1", "COMMIT@1", "COMMIT@1", "ABORT@1", "COMMIT@2"),
        submitted);
  }

  /**
   * An ABORT is repeated before it is given up, whatever kind of failure it hit, because dropping
   * staged data is idempotent: an ABORT of a task this region no longer holds succeeds. What a
   * region still keeps after the last attempt is reclaimed by the cleaner.
   */
  @Test
  public void testAbortIsRetriedOnFailure() throws Exception {
    // Two regions, both unable to prepare: each of them must be told to drop its staged data. The
    // route of each region stays resolvable, which is what a retry is allowed to happen on.
    final List<String> submitted = Collections.synchronizedList(new ArrayList<>());
    final LoadConsensusSubmitter submitter = mock(LoadConsensusSubmitter.class);
    when(submitter.submit(any(), any()))
        .thenAnswer(
            invocation -> {
              final TRegionReplicaSet replicaSet = invocation.getArgument(0);
              final LoadTsFileConsensusNode node = invocation.getArgument(1);
              submitted.add(node.getOp().name() + "@" + replicaSet.getRegionId().getId());
              return RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR);
            });
    when(submitter.resolveRoute(any(), any(Integer.class)))
        .thenAnswer(invocation -> currentRoute(invocation.getArgument(0)));
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(submitter, twoRegionsInOrder(), loadIds(), pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(
        Arrays.asList(
            "PREPARE@1", "ABORT@1", "ABORT@1", "ABORT@1", "ABORT@2", "ABORT@2", "ABORT@2"),
        submitted);
  }
}
