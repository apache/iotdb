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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The LOAD keeps dispatching pieces while a region of the file migrates, so the pieces dispatched
 * after the migration must follow the route the region has now instead of the one pinned before it.
 */
public class TwoPhaseConsensusLoadStrategyMigrationTest {

  private static final TConsensusGroupId REGION =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private static final TRegionReplicaSet ROUTE_BEFORE_MIGRATION =
      new TRegionReplicaSet(REGION, Collections.singletonList(location(11)));

  private static final TRegionReplicaSet ROUTE_AFTER_MIGRATION =
      new TRegionReplicaSet(REGION, Collections.singletonList(location(12)));

  @Test
  public void testAPieceDispatchedAfterTheMigrationFollowsTheNewRoute() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitter(
                submitted,
                (pieceIndex, route) -> pieceIndex == 1 && route.equals(ROUTE_BEFORE_MIGRATION),
                ROUTE_AFTER_MIGRATION));

    assertTrue(dispatch(strategy, piece(0), ROUTE_BEFORE_MIGRATION));
    // The region migrated while the LOAD was between two pieces: the second piece still goes to the
    // pinned route first, fails there, and is retried on the route the region has now.
    assertTrue(dispatch(strategy, piece(1), ROUTE_AFTER_MIGRATION));

    assertEquals(Arrays.asList("0@11", "1@11", "1@12"), submitted);
    assertEquals(ROUTE_AFTER_MIGRATION, routesOf(strategy).get(REGION));
    assertEquals(Long.valueOf(2L), pieceCounts(strategy).get(REGION));
  }

  @Test
  public void testAPieceWhoseRouteIsGoneAfterTheMigrationIsNotDispatched() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitter(
                submitted,
                (pieceIndex, route) -> pieceIndex == 1 && route.equals(ROUTE_BEFORE_MIGRATION),
                null));

    assertTrue(dispatch(strategy, piece(0), ROUTE_BEFORE_MIGRATION));
    // The route the piece was pinned to is gone and cannot be looked up again: the piece fails
    // instead of being sent to a route the partition table no longer knows.
    assertFalse(dispatch(strategy, piece(1), ROUTE_AFTER_MIGRATION));

    assertEquals(Arrays.asList("0@11", "1@11"), submitted);
    assertEquals(Long.valueOf(1L), pieceCounts(strategy).get(REGION));
  }

  private static TwoPhaseConsensusLoadStrategy strategy(final LoadConsensusSubmitter submitter) {
    return new TwoPhaseConsensusLoadStrategy(
        mock(LoadTsFileDispatcherImpl.class),
        new DataPartitionResolver(mock(DataPartitionBatchFetcher.class), "root"),
        mock(LoadTsFileDataCacheMemoryBlock.class),
        submitter,
        false);
  }

  /**
   * Submits on the given route, fails when the predicate says the route is stale, and resolves the
   * route to the given one afterwards.
   */
  private static LoadConsensusSubmitter submitter(
      final List<String> submitted,
      final BiPredicate<Long, TRegionReplicaSet> staleRoute,
      final TRegionReplicaSet resolvedRoute) {
    final LoadConsensusSubmitter submitter = mock(LoadConsensusSubmitter.class);
    when(submitter.submit(any(), any()))
        .thenAnswer(
            invocation -> {
              final TRegionReplicaSet route = invocation.getArgument(0);
              final LoadTsFileConsensusNode piece = invocation.getArgument(1);
              submitted.add(
                  piece.getPieceIndex()
                      + "@"
                      + route.getDataNodeLocations().get(0).getDataNodeId());
              return staleRoute.test(piece.getPieceIndex(), route)
                  ? RpcUtils.getStatus(TSStatusCode.DISPATCH_ERROR)
                  : RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
            });
    when(submitter.resolveRoute(any(), any(Integer.class))).thenReturn(resolvedRoute);
    return submitter;
  }

  private static LoadTsFilePieceNode piece(final long pieceIndex) {
    return new LoadTsFilePieceNode(new PlanNodeId("piece-" + pieceIndex), null, pieceIndex);
  }

  private static boolean dispatch(
      final TwoPhaseConsensusLoadStrategy strategy,
      final LoadTsFilePieceNode piece,
      final TRegionReplicaSet route)
      throws Exception {
    final Method method =
        TwoPhaseConsensusLoadStrategy.class.getDeclaredMethod(
            "dispatchOnePieceNode", LoadTsFilePieceNode.class, TRegionReplicaSet.class);
    method.setAccessible(true);
    return (boolean) method.invoke(strategy, piece, route);
  }

  @SuppressWarnings("unchecked")
  private static Map<TConsensusGroupId, TRegionReplicaSet> routesOf(
      final TwoPhaseConsensusLoadStrategy strategy) throws Exception {
    final Field field = TwoPhaseConsensusLoadStrategy.class.getDeclaredField("regionRoutes");
    field.setAccessible(true);
    return (Map<TConsensusGroupId, TRegionReplicaSet>) field.get(strategy);
  }

  @SuppressWarnings("unchecked")
  private static Map<TConsensusGroupId, Long> pieceCounts(
      final TwoPhaseConsensusLoadStrategy strategy) throws Exception {
    final Field field = TwoPhaseConsensusLoadStrategy.class.getDeclaredField("regionPieceCounts");
    field.setAccessible(true);
    return (Map<TConsensusGroupId, Long>) field.get(strategy);
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
}
