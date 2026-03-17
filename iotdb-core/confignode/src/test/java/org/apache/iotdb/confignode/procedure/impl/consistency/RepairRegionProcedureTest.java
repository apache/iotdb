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

package org.apache.iotdb.confignode.procedure.impl.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.CompositeKeyCodec;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DataPointLocator;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DiffEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.RowRefIndex;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairAction;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairConflictResolver;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.ModEntrySummary;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairPlan;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairRecord;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairSession;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairStrategy;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RepairRegionProcedureTest {

  @Test
  public void serDeTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 10);
    RepairRegionProcedure procedure =
        new RepairRegionProcedure(
            groupId,
            new TestExecutionContext(Collections.emptyMap(), new SimulatedReplicaState()));
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure, ProcedureFactory.getInstance().create(buffer));
    } finally {
      RepairRegionProcedure.unregisterExecutionContext(procedure.getExecutionContextId());
    }
  }

  @Test
  public void executeRepairFlowTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    RowRefIndex rowRefIndex =
        new RowRefIndex.Builder()
            .addDevice("root.sg.d1", Arrays.asList("s1", "s2"))
            .setTimeBucketStart(0L)
            .build();
    long compositeKey = CompositeKeyCodec.encode(0, 0, 100L, 0L, 1L);
    DiffEntry diffEntry = new DiffEntry(compositeKey, 42L, DiffEntry.DiffType.LEADER_HAS);
    DataPointLocator largeLocator = new DataPointLocator("root.sg.d1", "s1", 100L);
    DataPointLocator smallLocator = new DataPointLocator("root.sg.d1", "s2", 200L);

    MerkleFileContent smallFile =
        new MerkleFileContent(
            1L,
            1L,
            Collections.singletonList(new MerkleEntry("root.sg.d1", "s2", 0L, 3_600_000L, 10, 1L)),
            "small.tsfile");
    MerkleFileContent largeFile =
        new MerkleFileContent(
            2L,
            2L,
            Collections.singletonList(
                new MerkleEntry("root.sg.d1", "s1", 0L, 3_600_000L, 1000, 2L)),
            "large.tsfile");

    Map<String, Long> tsFileSizes = new HashMap<>();
    tsFileSizes.put("small.tsfile", 8L * 1024 * 1024);
    tsFileSizes.put("large.tsfile", 128L * 1024 * 1024);

    Map<String, Integer> totalPointCounts = new HashMap<>();
    totalPointCounts.put("small.tsfile", 10);
    totalPointCounts.put("large.tsfile", 1000);

    SimulatedReplicaState replicaState = new SimulatedReplicaState();
    replicaState.addLeaderPoint("small.tsfile", smallLocator, 1L, "small-value");
    replicaState.addLeaderPoint("large.tsfile", largeLocator, 10L, "value");
    replicaState.addFollowerTsFile("large.tsfile");

    TestPartitionRepairContext partitionContext =
        new TestPartitionRepairContext(
            0L,
            false,
            Arrays.asList(smallFile, largeFile),
            Arrays.asList(smallFile, largeFile),
            rowRefIndex,
            Collections.singletonList(diffEntry),
            true,
            1L,
            Collections.emptyList(),
            tsFileSizes,
            totalPointCounts,
            Collections.emptyList(),
            Collections.emptyList(),
            (repairPlans, rootHashMatched) -> {
              RepairPlan smallPlan = repairPlans.get("small.tsfile");
              RepairPlan largePlan = repairPlans.get("large.tsfile");
              return !rootHashMatched
                  && smallPlan != null
                  && largePlan != null
                  && smallPlan.getStrategy() == RepairStrategy.DIRECT_TSFILE_TRANSFER
                  && largePlan.getStrategy() == RepairStrategy.POINT_STREAMING
                  && replicaState.followerHasTsFile("small.tsfile")
                  && replicaState.isConsistentWithLeader();
            });

    TestExecutionContext executionContext =
        new TestExecutionContext(Collections.singletonMap(0L, partitionContext), replicaState);
    ExposedRepairRegionProcedure procedure =
        new ExposedRepairRegionProcedure(groupId, executionContext);

    executeProcedureToCompletion(procedure, 32);
    Assert.assertEquals(
        Collections.singletonList("small.tsfile"), executionContext.transferredTsFiles);
    Assert.assertEquals(1, executionContext.generatedRecords.size());
    Assert.assertEquals(1, executionContext.appliedInsertRecords.size());
    Assert.assertTrue(executionContext.appliedDeleteRecords.isEmpty());
    Assert.assertEquals(
        Arrays.asList("append:INSERT", "commit", "delete"), executionContext.repairJournalEvents);
    Assert.assertEquals(1, executionContext.committedPartitions.size());
    Assert.assertEquals(Long.valueOf(0L), executionContext.committedPartitions.get(0));
    Assert.assertEquals(1000L, executionContext.advancedWatermark);
    Assert.assertTrue(executionContext.closed);
    Assert.assertFalse(executionContext.rolledBack);
    Assert.assertEquals(1000L, procedure.getGlobalRepairedWatermark());
    Assert.assertTrue(replicaState.isConsistentWithLeader());
  }

  @Test
  public void executeRepairFlowDeletesFollowerOnlyDataWhenLeaderDeletionWinsTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    RowRefIndex rowRefIndex =
        new RowRefIndex.Builder()
            .addDevice("root.sg.d1", Collections.singletonList("s1"))
            .setTimeBucketStart(0L)
            .build();
    long extraCompositeKey = CompositeKeyCodec.encode(0, 0, 100L, 0L, 1L);
    DiffEntry extraFollowerPoint =
        new DiffEntry(extraCompositeKey, 84L, DiffEntry.DiffType.FOLLOWER_HAS);
    DataPointLocator stableLocator = new DataPointLocator("root.sg.d1", "s1", 200L);
    DataPointLocator extraLocator = new DataPointLocator("root.sg.d1", "s1", 100L);

    MerkleFileContent largeFile =
        new MerkleFileContent(
            2L,
            2L,
            Collections.singletonList(
                new MerkleEntry("root.sg.d1", "s1", 0L, 3_600_000L, 1000, 2L)),
            "large.tsfile");

    Map<String, Long> tsFileSizes = Collections.singletonMap("large.tsfile", 128L * 1024 * 1024);
    Map<String, Integer> totalPointCounts = Collections.singletonMap("large.tsfile", 1000);

    SimulatedReplicaState replicaState = new SimulatedReplicaState();
    replicaState.addLeaderPoint("large.tsfile", stableLocator, 20L, "stable-value");
    replicaState.addFollowerPoint("large.tsfile", stableLocator, 20L, "stable-value");
    replicaState.addFollowerPoint("large.tsfile", extraLocator, 5L, "stale-value");

    ModEntrySummary leaderDeletion = new ModEntrySummary("root.sg.d1", "s1", 0L, 150L, 10L);
    TestPartitionRepairContext partitionContext =
        new TestPartitionRepairContext(
            0L,
            false,
            Collections.singletonList(largeFile),
            Collections.singletonList(largeFile),
            rowRefIndex,
            Collections.singletonList(extraFollowerPoint),
            true,
            1L,
            Collections.emptyList(),
            tsFileSizes,
            totalPointCounts,
            Collections.singletonList(leaderDeletion),
            Collections.emptyList(),
            (repairPlans, rootHashMatched) -> {
              RepairPlan largePlan = repairPlans.get("large.tsfile");
              return !rootHashMatched
                  && largePlan != null
                  && largePlan.getStrategy() == RepairStrategy.POINT_STREAMING
                  && replicaState.isConsistentWithLeader();
            });

    TestExecutionContext executionContext =
        new TestExecutionContext(Collections.singletonMap(0L, partitionContext), replicaState);
    ExposedRepairRegionProcedure procedure =
        new ExposedRepairRegionProcedure(groupId, executionContext);

    executeProcedureToCompletion(procedure, 32);
    Assert.assertTrue(executionContext.transferredTsFiles.isEmpty());
    Assert.assertEquals(1, executionContext.generatedRecords.size());
    Assert.assertTrue(executionContext.appliedInsertRecords.isEmpty());
    Assert.assertEquals(1, executionContext.appliedDeleteRecords.size());
    Assert.assertEquals(
        Arrays.asList("append:DELETE", "commit", "delete"), executionContext.repairJournalEvents);
    Assert.assertTrue(replicaState.isConsistentWithLeader());
    Assert.assertFalse(replicaState.hasFollowerPoint(extraLocator));
    Assert.assertFalse(executionContext.rolledBack);
  }

  @Test
  public void executeRepairFlowDeletesLeaderDataWhenFollowerTombstoneWinsTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 3);
    RowRefIndex rowRefIndex =
        new RowRefIndex.Builder()
            .addDevice("root.sg.d1", Collections.singletonList("s1"))
            .setTimeBucketStart(0L)
            .build();
    long staleCompositeKey = CompositeKeyCodec.encode(0, 0, 100L, 0L, 1L);
    DiffEntry staleLeaderPoint =
        new DiffEntry(staleCompositeKey, 126L, DiffEntry.DiffType.LEADER_HAS);
    DataPointLocator stableLocator = new DataPointLocator("root.sg.d1", "s1", 200L);
    DataPointLocator staleLocator = new DataPointLocator("root.sg.d1", "s1", 100L);

    MerkleFileContent largeFile =
        new MerkleFileContent(
            2L,
            2L,
            Collections.singletonList(
                new MerkleEntry("root.sg.d1", "s1", 0L, 3_600_000L, 1000, 2L)),
            "large.tsfile");

    Map<String, Long> tsFileSizes = Collections.singletonMap("large.tsfile", 128L * 1024 * 1024);
    Map<String, Integer> totalPointCounts = Collections.singletonMap("large.tsfile", 1000);

    SimulatedReplicaState replicaState = new SimulatedReplicaState();
    replicaState.addLeaderPoint("large.tsfile", stableLocator, 20L, "stable-value");
    replicaState.addFollowerPoint("large.tsfile", stableLocator, 20L, "stable-value");
    replicaState.addLeaderPoint("large.tsfile", staleLocator, 5L, "stale-leader-value");

    ModEntrySummary followerDeletion = new ModEntrySummary("root.sg.d1", "s1", 0L, 150L, 10L);
    TestPartitionRepairContext partitionContext =
        new TestPartitionRepairContext(
            0L,
            false,
            Collections.singletonList(largeFile),
            Collections.singletonList(largeFile),
            rowRefIndex,
            Collections.singletonList(staleLeaderPoint),
            true,
            1L,
            Collections.emptyList(),
            tsFileSizes,
            totalPointCounts,
            Collections.emptyList(),
            Collections.singletonList(followerDeletion),
            (repairPlans, rootHashMatched) -> {
              RepairPlan largePlan = repairPlans.get("large.tsfile");
              return !rootHashMatched
                  && largePlan != null
                  && largePlan.getStrategy() == RepairStrategy.POINT_STREAMING
                  && replicaState.isConsistentWithLeader();
            });

    TestExecutionContext executionContext =
        new TestExecutionContext(Collections.singletonMap(0L, partitionContext), replicaState);
    ExposedRepairRegionProcedure procedure =
        new ExposedRepairRegionProcedure(groupId, executionContext);

    executeProcedureToCompletion(procedure, 32);
    Assert.assertTrue(executionContext.transferredTsFiles.isEmpty());
    Assert.assertEquals(1, executionContext.generatedRecords.size());
    Assert.assertTrue(executionContext.appliedInsertRecords.isEmpty());
    Assert.assertEquals(1, executionContext.appliedDeleteRecords.size());
    Assert.assertEquals(
        Arrays.asList("append:DELETE", "commit", "delete"), executionContext.repairJournalEvents);
    Assert.assertTrue(replicaState.isConsistentWithLeader());
    Assert.assertFalse(replicaState.hasLeaderPoint(staleLocator));
    Assert.assertFalse(executionContext.rolledBack);
  }

  @Test
  public void executeRepairFlowStreamsFollowerDataBackToLeaderTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 4);
    RowRefIndex rowRefIndex =
        new RowRefIndex.Builder()
            .addDevice("root.sg.d1", Collections.singletonList("s1"))
            .setTimeBucketStart(0L)
            .build();
    long missingCompositeKey = CompositeKeyCodec.encode(0, 0, 100L, 0L, 1L);
    DiffEntry followerOwnedPoint =
        new DiffEntry(missingCompositeKey, 168L, DiffEntry.DiffType.FOLLOWER_HAS);
    DataPointLocator stableLocator = new DataPointLocator("root.sg.d1", "s1", 200L);
    DataPointLocator followerLocator = new DataPointLocator("root.sg.d1", "s1", 100L);

    MerkleFileContent largeFile =
        new MerkleFileContent(
            2L,
            2L,
            Collections.singletonList(
                new MerkleEntry("root.sg.d1", "s1", 0L, 3_600_000L, 1000, 2L)),
            "large.tsfile");

    Map<String, Long> tsFileSizes = Collections.singletonMap("large.tsfile", 128L * 1024 * 1024);
    Map<String, Integer> totalPointCounts = Collections.singletonMap("large.tsfile", 1000);

    SimulatedReplicaState replicaState = new SimulatedReplicaState();
    replicaState.addLeaderPoint("large.tsfile", stableLocator, 20L, "stable-value");
    replicaState.addFollowerPoint("large.tsfile", stableLocator, 20L, "stable-value");
    replicaState.addFollowerPoint("large.tsfile", followerLocator, 25L, "follower-owned-value");

    TestPartitionRepairContext partitionContext =
        new TestPartitionRepairContext(
            0L,
            false,
            Collections.singletonList(largeFile),
            Collections.singletonList(largeFile),
            rowRefIndex,
            Collections.singletonList(followerOwnedPoint),
            true,
            1L,
            Collections.emptyList(),
            tsFileSizes,
            totalPointCounts,
            Collections.emptyList(),
            Collections.emptyList(),
            (repairPlans, rootHashMatched) -> {
              RepairPlan largePlan = repairPlans.get("large.tsfile");
              return !rootHashMatched
                  && largePlan != null
                  && largePlan.getStrategy() == RepairStrategy.POINT_STREAMING
                  && replicaState.isConsistentWithLeader();
            });

    TestExecutionContext executionContext =
        new TestExecutionContext(Collections.singletonMap(0L, partitionContext), replicaState);
    ExposedRepairRegionProcedure procedure =
        new ExposedRepairRegionProcedure(groupId, executionContext);

    executeProcedureToCompletion(procedure, 32);
    Assert.assertTrue(executionContext.transferredTsFiles.isEmpty());
    Assert.assertEquals(1, executionContext.generatedRecords.size());
    Assert.assertEquals(1, executionContext.appliedInsertRecords.size());
    Assert.assertTrue(executionContext.appliedDeleteRecords.isEmpty());
    Assert.assertEquals(
        Arrays.asList("append:INSERT", "commit", "delete"), executionContext.repairJournalEvents);
    Assert.assertTrue(replicaState.isConsistentWithLeader());
    Assert.assertTrue(replicaState.hasLeaderPoint(followerLocator));
    Assert.assertFalse(executionContext.rolledBack);
  }

  private static class TestExecutionContext
      implements RepairRegionProcedure.RepairExecutionContext {

    private final Map<Long, RepairRegionProcedure.PartitionRepairContext> partitionContexts;
    private final SimulatedReplicaState replicaState;
    private final List<String> transferredTsFiles = new ArrayList<>();
    private final List<RepairRecord> generatedRecords = new ArrayList<>();
    private final List<RepairRecord> appliedInsertRecords = new ArrayList<>();
    private final List<RepairRecord> appliedDeleteRecords = new ArrayList<>();
    private final List<String> repairJournalEvents = new ArrayList<>();
    private final List<Long> committedPartitions = new ArrayList<>();
    private long advancedWatermark = -1L;
    private boolean closed;
    private boolean rolledBack;

    private TestExecutionContext(
        Map<Long, RepairRegionProcedure.PartitionRepairContext> partitionContexts,
        SimulatedReplicaState replicaState) {
      this.partitionContexts = partitionContexts;
      this.replicaState = replicaState;
    }

    @Override
    public boolean isReplicationComplete() {
      return true;
    }

    @Override
    public long computeSafeWatermark() {
      return 1000L;
    }

    @Override
    public List<Long> collectPendingPartitions(
        long globalRepairedWatermark, long safeWatermark, RepairProgressTable repairProgressTable) {
      return new ArrayList<>(partitionContexts.keySet());
    }

    @Override
    public RepairRegionProcedure.PartitionRepairContext getPartitionContext(long partitionId) {
      return partitionContexts.get(partitionId);
    }

    @Override
    public RepairRecord buildRepairRecord(
        RepairRegionProcedure.PartitionRepairContext partitionContext,
        DiffEntry diffEntry,
        RowRefIndex rowRefIndex,
        RepairConflictResolver conflictResolver) {
      DataPointLocator locator = rowRefIndex.resolve(diffEntry.getCompositeKey());
      SimulatedPoint leaderPoint = replicaState.getLeaderPoint(locator);
      SimulatedPoint followerPoint = replicaState.getFollowerPoint(locator);
      RepairRecord record = null;
      if (diffEntry.getType() == DiffEntry.DiffType.LEADER_HAS) {
        if (leaderPoint == null) {
          return null;
        }
        RepairAction action =
            conflictResolver.resolveLeaderHas(locator, leaderPoint.getProgressIndex());
        if (action == RepairAction.SEND_TO_FOLLOWER) {
          record =
              RepairRecord.insertToFollower(
                  locator,
                  leaderPoint.getProgressIndex(),
                  leaderPoint.getValue(),
                  locator.getTimestamp());
        } else if (action == RepairAction.DELETE_ON_LEADER) {
          record =
              RepairRecord.deleteOnLeader(
                  locator, leaderPoint.getProgressIndex(), locator.getTimestamp());
        }
      } else if (diffEntry.getType() == DiffEntry.DiffType.FOLLOWER_HAS) {
        if (followerPoint == null) {
          return null;
        }
        RepairAction action =
            conflictResolver.resolveFollowerHas(locator, followerPoint.getProgressIndex());
        if (action == RepairAction.DELETE_ON_FOLLOWER) {
          record =
              RepairRecord.deleteOnFollower(
                  locator, followerPoint.getProgressIndex(), locator.getTimestamp());
        } else if (action == RepairAction.SEND_TO_LEADER) {
          record =
              RepairRecord.insertToLeader(
                  locator,
                  followerPoint.getProgressIndex(),
                  followerPoint.getValue(),
                  locator.getTimestamp());
        }
      }
      if (record != null) {
        generatedRecords.add(record);
      }
      return record;
    }

    @Override
    public void transferTsFile(String tsFilePath) {
      transferredTsFiles.add(tsFilePath);
      replicaState.transferTsFile(tsFilePath);
    }

    @Override
    public RepairSession createRepairSession(long partitionId) {
      return new RepairSession(
          partitionId,
          (sessionId, ignoredPartitionId, inserts, deletes) -> {
            appliedInsertRecords.addAll(inserts);
            appliedDeleteRecords.addAll(deletes);
            replicaState.applyInserts(inserts);
            replicaState.applyDeletes(deletes);
          },
          new RepairSession.RepairSessionJournal() {
            @Override
            public void append(String sessionId, RepairRecord record) {
              repairJournalEvents.add("append:" + record.getType());
            }

            @Override
            public void markCommitted(String sessionId) {
              repairJournalEvents.add("commit");
            }

            @Override
            public void delete(String sessionId) {
              repairJournalEvents.add("delete");
            }
          });
    }

    @Override
    public void onPartitionCommitted(
        long partitionId, long repairedTo, RepairProgressTable repairProgressTable) {
      committedPartitions.add(partitionId);
    }

    @Override
    public void onWatermarkAdvanced(long globalWatermark, RepairProgressTable repairProgressTable) {
      this.advancedWatermark = globalWatermark;
    }

    @Override
    public void rollbackPartition(
        long partitionId, RepairSession repairSession, RepairProgressTable repairProgressTable) {
      rolledBack = true;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static class ExposedRepairRegionProcedure extends RepairRegionProcedure {

    private ExposedRepairRegionProcedure(
        TConsensusGroupId consensusGroupId, RepairExecutionContext executionContext) {
      super(consensusGroupId, executionContext);
    }

    private Procedure<?>[] executeOnce() throws InterruptedException {
      return doExecute(null);
    }
  }

  private static class TestPartitionRepairContext
      implements RepairRegionProcedure.PartitionRepairContext {

    private final long partitionId;
    private final boolean rootHashMatched;
    private final List<MerkleFileContent> leaderMerkleFiles;
    private final List<MerkleFileContent> mismatchedLeaderMerkleFiles;
    private final RowRefIndex rowRefIndex;
    private final List<DiffEntry> decodedDiffs;
    private final boolean diffDecodeSuccessful;
    private final long estimatedDiffCount;
    private final List<String> fallbackTsFiles;
    private final Map<String, Long> tsFileSizes;
    private final Map<String, Integer> totalPointCounts;
    private final List<ModEntrySummary> leaderDeletions;
    private final List<ModEntrySummary> followerDeletions;
    private final RepairVerifier verifier;

    private TestPartitionRepairContext(
        long partitionId,
        boolean rootHashMatched,
        List<MerkleFileContent> leaderMerkleFiles,
        List<MerkleFileContent> mismatchedLeaderMerkleFiles,
        RowRefIndex rowRefIndex,
        List<DiffEntry> decodedDiffs,
        boolean diffDecodeSuccessful,
        long estimatedDiffCount,
        List<String> fallbackTsFiles,
        Map<String, Long> tsFileSizes,
        Map<String, Integer> totalPointCounts,
        List<ModEntrySummary> leaderDeletions,
        List<ModEntrySummary> followerDeletions,
        RepairVerifier verifier) {
      this.partitionId = partitionId;
      this.rootHashMatched = rootHashMatched;
      this.leaderMerkleFiles = leaderMerkleFiles;
      this.mismatchedLeaderMerkleFiles = mismatchedLeaderMerkleFiles;
      this.rowRefIndex = rowRefIndex;
      this.decodedDiffs = decodedDiffs;
      this.diffDecodeSuccessful = diffDecodeSuccessful;
      this.estimatedDiffCount = estimatedDiffCount;
      this.fallbackTsFiles = fallbackTsFiles;
      this.tsFileSizes = tsFileSizes;
      this.totalPointCounts = totalPointCounts;
      this.leaderDeletions = leaderDeletions;
      this.followerDeletions = followerDeletions;
      this.verifier = verifier;
    }

    @Override
    public long getPartitionId() {
      return partitionId;
    }

    @Override
    public boolean isRootHashMatched() {
      return rootHashMatched;
    }

    @Override
    public List<MerkleFileContent> getLeaderMerkleFiles() {
      return leaderMerkleFiles;
    }

    @Override
    public List<MerkleFileContent> getMismatchedLeaderMerkleFiles() {
      return mismatchedLeaderMerkleFiles;
    }

    @Override
    public RowRefIndex getRowRefIndex() {
      return rowRefIndex;
    }

    @Override
    public List<DiffEntry> decodeDiffs() {
      return decodedDiffs;
    }

    @Override
    public boolean isDiffDecodeSuccessful() {
      return diffDecodeSuccessful;
    }

    @Override
    public long estimateDiffCount() {
      return estimatedDiffCount;
    }

    @Override
    public List<String> getFallbackTsFiles() {
      return fallbackTsFiles;
    }

    @Override
    public long getTsFileSize(String tsFilePath) {
      return tsFileSizes.getOrDefault(tsFilePath, 0L);
    }

    @Override
    public int getTotalPointCount(String tsFilePath) {
      return totalPointCounts.getOrDefault(tsFilePath, 0);
    }

    @Override
    public List<ModEntrySummary> getLeaderDeletions() {
      return leaderDeletions;
    }

    @Override
    public List<ModEntrySummary> getFollowerDeletions() {
      return followerDeletions;
    }

    @Override
    public boolean verify(Map<String, RepairPlan> repairPlans, boolean rootHashMatched) {
      return verifier.verify(repairPlans, rootHashMatched);
    }
  }

  private static void executeProcedureToCompletion(
      ExposedRepairRegionProcedure procedure, int maxSteps) throws Exception {
    int steps = 0;
    Procedure<?>[] next;
    do {
      next = procedure.executeOnce();
      steps++;
    } while (next != null && steps < maxSteps);
    Assert.assertTrue("procedure should finish within " + maxSteps + " steps", steps < maxSteps);
  }

  @FunctionalInterface
  private interface RepairVerifier {
    boolean verify(Map<String, RepairPlan> repairPlans, boolean rootHashMatched);
  }

  private static class SimulatedReplicaState {

    private final Map<String, List<SimulatedPoint>> leaderPointsByTsFile = new LinkedHashMap<>();
    private final Map<DataPointLocator, SimulatedPoint> leaderPoints = new LinkedHashMap<>();
    private final Map<DataPointLocator, SimulatedPoint> followerPoints = new LinkedHashMap<>();
    private final Set<String> leaderTsFiles = new LinkedHashSet<>();
    private final Set<String> followerTsFiles = new LinkedHashSet<>();

    private void addLeaderPoint(
        String tsFilePath, DataPointLocator locator, long progressIndex, Object value) {
      SimulatedPoint point = new SimulatedPoint(tsFilePath, locator, progressIndex, value);
      leaderTsFiles.add(tsFilePath);
      leaderPoints.put(locator, point);
      leaderPointsByTsFile.computeIfAbsent(tsFilePath, ignored -> new ArrayList<>()).add(point);
    }

    private void addFollowerTsFile(String tsFilePath) {
      followerTsFiles.add(tsFilePath);
    }

    private void addFollowerPoint(
        String tsFilePath, DataPointLocator locator, long progressIndex, Object value) {
      followerTsFiles.add(tsFilePath);
      followerPoints.put(locator, new SimulatedPoint(tsFilePath, locator, progressIndex, value));
    }

    private SimulatedPoint getLeaderPoint(DataPointLocator locator) {
      return leaderPoints.get(locator);
    }

    private SimulatedPoint getFollowerPoint(DataPointLocator locator) {
      return followerPoints.get(locator);
    }

    private boolean hasFollowerPoint(DataPointLocator locator) {
      return followerPoints.containsKey(locator);
    }

    private boolean hasLeaderPoint(DataPointLocator locator) {
      return leaderPoints.containsKey(locator);
    }

    private boolean followerHasTsFile(String tsFilePath) {
      return followerTsFiles.contains(tsFilePath);
    }

    private void transferTsFile(String tsFilePath) {
      followerTsFiles.add(tsFilePath);
      for (SimulatedPoint leaderPoint :
          leaderPointsByTsFile.getOrDefault(tsFilePath, Collections.emptyList())) {
        followerPoints.put(leaderPoint.getLocator(), leaderPoint);
      }
    }

    private void applyInserts(List<RepairRecord> inserts) {
      for (RepairRecord record : inserts) {
        if (record.getTargetReplica() == RepairRecord.TargetReplica.LEADER) {
          SimulatedPoint followerPoint = followerPoints.get(record.getLocator());
          String tsFilePath = followerPoint == null ? "stream.tsfile" : followerPoint.getTsFilePath();
          leaderTsFiles.add(tsFilePath);
          leaderPoints.put(
              record.getLocator(),
              new SimulatedPoint(
                  tsFilePath,
                  record.getLocator(),
                  record.getProgressIndex(),
                  record.getValue()));
        } else {
          SimulatedPoint leaderPoint = leaderPoints.get(record.getLocator());
          String tsFilePath = leaderPoint == null ? "stream.tsfile" : leaderPoint.getTsFilePath();
          followerTsFiles.add(tsFilePath);
          followerPoints.put(
              record.getLocator(),
              new SimulatedPoint(
                  tsFilePath,
                  record.getLocator(),
                  record.getProgressIndex(),
                  record.getValue()));
        }
      }
    }

    private void applyDeletes(List<RepairRecord> deletes) {
      for (RepairRecord record : deletes) {
        if (record.getTargetReplica() == RepairRecord.TargetReplica.LEADER) {
          leaderPoints.remove(record.getLocator());
        } else {
          followerPoints.remove(record.getLocator());
        }
      }
    }

    private boolean isConsistentWithLeader() {
      return leaderTsFiles.equals(followerTsFiles) && Objects.equals(leaderPoints, followerPoints);
    }
  }

  private static class SimulatedPoint {

    private final String tsFilePath;
    private final DataPointLocator locator;
    private final long progressIndex;
    private final Object value;

    private SimulatedPoint(
        String tsFilePath, DataPointLocator locator, long progressIndex, Object value) {
      this.tsFilePath = tsFilePath;
      this.locator = locator;
      this.progressIndex = progressIndex;
      this.value = value;
    }

    private String getTsFilePath() {
      return tsFilePath;
    }

    private DataPointLocator getLocator() {
      return locator;
    }

    private long getProgressIndex() {
      return progressIndex;
    }

    private Object getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SimulatedPoint)) {
        return false;
      }
      SimulatedPoint that = (SimulatedPoint) o;
      return progressIndex == that.progressIndex
          && Objects.equals(tsFilePath, that.tsFilePath)
          && Objects.equals(locator, that.locator)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tsFilePath, locator, progressIndex, value);
    }
  }
}
