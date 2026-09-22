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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
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
 * The second phase of a consensus LOAD has to leave no staged pieces behind: whenever a region
 * cannot be prepared or committed, the regions that have not committed yet are rolled back with
 * ABORT instead of being left with half a load on disk.
 */
public class TwoPhaseConsensusLoadStrategyAbortTest {

  private static final TConsensusGroupId REGION_1 =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private static final TConsensusGroupId REGION_2 =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);

  /** Fails the given operation for the given region, and accepts everything else. */
  private static LoadConsensusSubmitter submitterFailingOn(
      final LoadTsFileConsensusOp op,
      final TConsensusGroupId regionId,
      final List<String> submitted) {
    final LoadConsensusSubmitter submitter = mock(LoadConsensusSubmitter.class);
    when(submitter.submit(any(), any()))
        .thenAnswer(
            invocation -> {
              final TRegionReplicaSet replicaSet = invocation.getArgument(0);
              final LoadTsFileConsensusNode node = invocation.getArgument(1);
              submitted.add(node.getOp().name() + "@" + replicaSet.getRegionId().getId());
              if (node.getOp() == op && replicaSet.getRegionId().equals(regionId)) {
                return RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR);
              }
              return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
            });
    return submitter;
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
    setField(strategy, "allReplicaSets", replicaSets);
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
    assertEquals(Arrays.asList("PREPARE@1", "COMMIT@1", "PREPARE@2", "COMMIT@2"), submitted);
  }

  /**
   * The failure that used to leave staged data behind: the region that committed keeps its import,
   * while every region that did not commit is rolled back.
   */
  @Test
  public void testPrepareFailureAbortsTheRegionsThatDidNotCommit() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingOn(LoadTsFileConsensusOp.PREPARE, REGION_2, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(Arrays.asList("PREPARE@1", "COMMIT@1", "PREPARE@2", "ABORT@2"), submitted);
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

  @Test
  public void testCommitFailureAbortsTheRegionItFailedOn() throws Exception {
    final List<String> submitted = new ArrayList<>();
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(
            submitterFailingOn(LoadTsFileConsensusOp.COMMIT, REGION_1, submitted),
            twoRegionsInOrder(),
            loadIds(),
            pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(Arrays.asList("PREPARE@1", "COMMIT@1", "ABORT@2"), submitted);
  }

  @Test
  public void testAbortIsNotRetriedOnFailure() throws Exception {
    // Two regions, both unable to prepare: each of them must be told to drop its staged data.
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
    final TwoPhaseConsensusLoadStrategy strategy =
        strategy(submitter, twoRegionsInOrder(), loadIds(), pieceCounts());

    assertFalse(prepareAndCommit(strategy, mock(LoadSingleTsFileNode.class)));
    assertEquals(Arrays.asList("PREPARE@1", "ABORT@1", "ABORT@2"), submitted);
  }
}
