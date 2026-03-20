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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.iotv2.consistency.LogicalMismatchScope;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consistency.ConsistencyProgressManager;
import org.apache.iotdb.mpp.rpc.thrift.TApplyLogicalRepairBatchReq;
import org.apache.iotdb.mpp.rpc.thrift.TDecodeLeafDiffReq;
import org.apache.iotdb.mpp.rpc.thrift.TDecodeLeafDiffResp;
import org.apache.iotdb.mpp.rpc.thrift.TEstimateLeafDiffReq;
import org.apache.iotdb.mpp.rpc.thrift.TEstimateLeafDiffResp;
import org.apache.iotdb.mpp.rpc.thrift.TFinishLogicalRepairSessionReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetConsistencyEligibilityReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetConsistencyEligibilityResp;
import org.apache.iotdb.mpp.rpc.thrift.TGetSnapshotSubtreeReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetSnapshotSubtreeResp;
import org.apache.iotdb.mpp.rpc.thrift.TLeafDiffEntry;
import org.apache.iotdb.mpp.rpc.thrift.TLeafDiffEstimate;
import org.apache.iotdb.mpp.rpc.thrift.TLogicalRepairBatch;
import org.apache.iotdb.mpp.rpc.thrift.TLogicalRepairLeafSelector;
import org.apache.iotdb.mpp.rpc.thrift.TPartitionConsistencyEligibility;
import org.apache.iotdb.mpp.rpc.thrift.TSnapshotSubtreeNode;
import org.apache.iotdb.mpp.rpc.thrift.TStreamLogicalRepairReq;
import org.apache.iotdb.mpp.rpc.thrift.TStreamLogicalRepairResp;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

/** Runtime bridge between the repair procedure and the logical snapshot RPCs. */
public class LiveDataRegionRepairExecutionContext
    implements RepairRegionProcedure.RepairExecutionContext {

  private static final String TREE_KIND_LIVE = "LIVE";
  private static final String TREE_KIND_TOMBSTONE = "TOMBSTONE";
  private static final long EXACT_DIFF_DECODE_THRESHOLD = 2048L;

  private final ConfigManager configManager;
  private final ConsistencyProgressManager consistencyProgressManager;
  private final TConsensusGroupId consensusGroupId;
  private final TDataNodeLocation leaderLocation;
  private final List<TDataNodeLocation> followerLocations;
  private final String routeVersionToken;
  private final Set<Long> requestedPartitions;
  private final String requestedRepairEpoch;
  private final boolean repairMode;

  private final Map<Long, TPartitionConsistencyEligibility> leaderEligibilityByPartition =
      new TreeMap<>();
  private final Map<Integer, Map<Long, TPartitionConsistencyEligibility>>
      followerEligibilityByNodeId = new LinkedHashMap<>();
  private final Map<Long, LivePartitionRepairContext> partitionContexts = new TreeMap<>();
  private final Map<String, RepairOperation> repairOperationsById = new LinkedHashMap<>();

  private long syncLag = Long.MAX_VALUE;
  private long safeWatermark = Long.MIN_VALUE;

  public LiveDataRegionRepairExecutionContext(
      ConfigManager configManager, TConsensusGroupId consensusGroupId) {
    this(configManager, consensusGroupId, Collections.emptySet(), null, true);
  }

  public LiveDataRegionRepairExecutionContext(
      ConfigManager configManager,
      TConsensusGroupId consensusGroupId,
      Collection<Long> partitionFilter,
      String requestedRepairEpoch,
      boolean repairMode) {
    this.configManager = configManager;
    this.consistencyProgressManager = configManager.getConsistencyProgressManager();
    this.consensusGroupId = requireDataRegion(consensusGroupId);
    this.requestedPartitions =
        partitionFilter == null ? Collections.emptySet() : new LinkedHashSet<>(partitionFilter);
    this.requestedRepairEpoch = requestedRepairEpoch;
    this.repairMode = repairMode;

    TRegionReplicaSet replicaSet =
        configManager
            .getPartitionManager()
            .getAllReplicaSetsMap(TConsensusGroupType.DataRegion)
            .get(this.consensusGroupId);
    if (replicaSet == null || replicaSet.getDataNodeLocations() == null) {
      throw new IllegalStateException("DataRegion " + consensusGroupId + " does not exist");
    }
    int leaderId =
        configManager.getLoadManager().getRegionLeaderMap().getOrDefault(this.consensusGroupId, -1);
    if (leaderId <= 0) {
      throw new IllegalStateException("Cannot determine leader for DataRegion " + consensusGroupId);
    }
    this.leaderLocation =
        replicaSet.getDataNodeLocations().stream()
            .filter(location -> location.getDataNodeId() == leaderId)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Leader "
                            + leaderId
                            + " is not part of replica set for "
                            + consensusGroupId));
    this.followerLocations =
        replicaSet.getDataNodeLocations().stream()
            .filter(location -> location.getDataNodeId() != leaderId)
            .sorted(Comparator.comparingInt(TDataNodeLocation::getDataNodeId))
            .collect(Collectors.toList());
    this.routeVersionToken = buildRouteBindingToken(leaderId);
    refreshLeaderEligibility();
  }

  @Override
  public boolean isReplicationComplete() {
    return syncLag == 0L;
  }

  @Override
  public long computeSafeWatermark() {
    return safeWatermark;
  }

  @Override
  public List<Long> collectPendingPartitions(
      long currentSafeWatermark, RepairProgressTable repairProgressTable) {
    if (!isRouteBindingStillValid()) {
      return Collections.emptyList();
    }
    refreshLeaderEligibility();
    partitionContexts.clear();
    repairOperationsById.clear();

    if (!isReplicationComplete() || safeWatermark == Long.MIN_VALUE) {
      return Collections.emptyList();
    }

    followerEligibilityByNodeId.clear();
    for (TDataNodeLocation followerLocation : followerLocations) {
      EligibilitySnapshot snapshot = fetchEligibility(followerLocation, !repairMode);
      if (!snapshot.isAvailable()) {
        return Collections.emptyList();
      }
      followerEligibilityByNodeId.put(
          followerLocation.getDataNodeId(), snapshot.getPartitionsMap());
    }

    List<Long> candidatePartitions =
        ConsistencyPartitionSelector.selectCandidatePartitions(
            leaderEligibilityByPartition,
            requestedPartitions,
            repairMode,
            requestedRepairEpoch,
            repairProgressTable,
            buildReplicaObservationTokens());
    if (candidatePartitions.isEmpty()) {
      return Collections.emptyList();
    }

    long now = System.currentTimeMillis();
    List<Long> repairablePartitions = new ArrayList<>();
    for (Long partitionId : candidatePartitions) {
      TPartitionConsistencyEligibility leaderEligibility =
          leaderEligibilityByPartition.get(partitionId);
      RepairProgressTable.PartitionProgress progress =
          repairProgressTable.getPartition(partitionId);
      if (leaderEligibility == null) {
        continue;
      }
      if (!isSnapshotComparable(leaderEligibility)) {
        repairProgressTable.markDirty(partitionId);
        continue;
      }

      boolean allFollowersReady = true;
      boolean rootMatched = true;
      Set<LogicalMismatchScope.Scope> unionScopes = new LinkedHashSet<>();
      try {
        for (TDataNodeLocation followerLocation : followerLocations) {
          TPartitionConsistencyEligibility followerEligibility =
              followerEligibilityByNodeId
                  .getOrDefault(followerLocation.getDataNodeId(), Collections.emptyMap())
                  .get(partitionId);
          if (!isSnapshotComparable(followerEligibility)) {
            allFollowersReady = false;
            break;
          }
          boolean liveMatched = sameRootDigest(leaderEligibility, followerEligibility);
          boolean tombstoneMatched = sameTombstoneDigest(leaderEligibility, followerEligibility);
          if (!liveMatched || !tombstoneMatched) {
            rootMatched = false;
            if (repairMode
                && progress != null
                && progress.getCheckState() == RepairProgressTable.CheckState.MISMATCH
                && progress.getMismatchScopeRef() != null
                && Objects.equals(
                    buildRepairEpoch(partitionId, leaderEligibility), progress.getRepairEpoch())) {
              unionScopes.addAll(LogicalMismatchScope.deserialize(progress.getMismatchScopeRef()));
            } else {
              if (!liveMatched) {
                unionScopes.addAll(comparePartition(followerLocation, partitionId, TREE_KIND_LIVE));
              }
              if (!tombstoneMatched) {
                unionScopes.addAll(
                    comparePartition(followerLocation, partitionId, TREE_KIND_TOMBSTONE));
              }
            }
          }
        }
      } catch (StaleSnapshotCompareException e) {
        repairProgressTable.markDirty(partitionId);
        continue;
      }

      if (!allFollowersReady) {
        repairProgressTable.markDirty(partitionId);
        continue;
      }

      if (rootMatched) {
        String replicaObservationToken = buildReplicaObservationToken(partitionId);
        repairProgressTable.markVerified(
            partitionId,
            now,
            safeWatermark,
            leaderEligibility.getPartitionMutationEpoch(),
            leaderEligibility.getSnapshotEpoch(),
            RepairProgressTable.SnapshotState.READY,
            replicaObservationToken);
        continue;
      }

      String repairEpochRef = buildRepairEpoch(partitionId, leaderEligibility);
      if (repairMode
          && progress != null
          && progress.getCheckState() == RepairProgressTable.CheckState.MISMATCH
          && progress.getMismatchScopeRef() != null
          && !Objects.equals(repairEpochRef, progress.getRepairEpoch())) {
        repairProgressTable.markDirty(partitionId);
        continue;
      }
      if (unionScopes.isEmpty()) {
        String replicaObservationToken = buildReplicaObservationToken(partitionId);
        repairProgressTable.markVerified(
            partitionId,
            now,
            safeWatermark,
            leaderEligibility.getPartitionMutationEpoch(),
            leaderEligibility.getSnapshotEpoch(),
            RepairProgressTable.SnapshotState.READY,
            replicaObservationToken);
        continue;
      }

      String mismatchScopeRef = LogicalMismatchScope.serialize(unionScopes);
      String replicaObservationToken = buildReplicaObservationToken(partitionId);
      repairProgressTable.markMismatch(
          partitionId,
          now,
          safeWatermark,
          leaderEligibility.getPartitionMutationEpoch(),
          leaderEligibility.getSnapshotEpoch(),
          RepairProgressTable.SnapshotState.READY,
          mismatchScopeRef,
          unionScopes.size(),
          repairEpochRef,
          replicaObservationToken);

      if (!repairMode) {
        continue;
      }

      String blockingReason = buildBlockingReason(partitionId, unionScopes);
      if (blockingReason != null) {
        partitionContexts.put(
            partitionId,
            new LivePartitionRepairContext(
                partitionId, false, Collections.emptyList(), repairEpochRef, blockingReason));
        repairablePartitions.add(partitionId);
        continue;
      }

      LivePartitionRepairContext context =
          new LivePartitionRepairContext(
              partitionId, false, new ArrayList<>(), repairEpochRef, null);
      Set<RepairLeafKey> scheduledLeaves = new HashSet<>();
      for (TDataNodeLocation followerLocation : followerLocations) {
        TPartitionConsistencyEligibility followerEligibility =
            followerEligibilityByNodeId
                .getOrDefault(followerLocation.getDataNodeId(), Collections.emptyMap())
                .get(partitionId);
        if (followerEligibility == null
            || (sameRootDigest(leaderEligibility, followerEligibility)
                && sameTombstoneDigest(leaderEligibility, followerEligibility))) {
          continue;
        }
        for (LogicalMismatchScope.Scope scope : unionScopes) {
          RepairLeafKey repairLeafKey =
              new RepairLeafKey(
                  followerLocation.getDataNodeId(),
                  scope.getTreeKind(),
                  scope.getLeafId(),
                  scope.toPersistentString());
          if (!scheduledLeaves.add(repairLeafKey)) {
            continue;
          }
          String operationId =
              partitionId
                  + "-"
                  + followerLocation.getDataNodeId()
                  + "-"
                  + scope.getTreeKind()
                  + "-"
                  + scope.getLeafId()
                  + "-"
                  + UUID.randomUUID();
          repairOperationsById.put(
              operationId,
              new RepairOperation(partitionId, followerLocation, scope, repairEpochRef));
          context.repairOperationIds.add(operationId);
        }
      }
      if (!context.repairOperationIds.isEmpty()) {
        partitionContexts.put(partitionId, context);
        repairablePartitions.add(partitionId);
      }
    }

    return repairablePartitions;
  }

  @Override
  public RepairRegionProcedure.PartitionRepairContext getPartitionContext(long partitionId) {
    return partitionContexts.get(partitionId);
  }

  @Override
  public void executeRepairOperation(String operationId) {
    RepairOperation operation = repairOperationsById.get(operationId);
    if (operation == null) {
      throw new IllegalStateException("Unknown logical repair operation " + operationId);
    }
    if (!isRouteBindingStillValid()) {
      throw new IllegalStateException(
          "Repair route drift detected for partition " + operation.partitionId);
    }

    TPartitionConsistencyEligibility leaderEligibility =
        leaderEligibilityByPartition.get(operation.partitionId);
    if (leaderEligibility == null
        || !Objects.equals(
            buildRepairEpoch(operation.partitionId, leaderEligibility), operation.repairEpoch)) {
      throw new IllegalStateException(
          "Repair epoch drift detected for partition " + operation.partitionId);
    }

    TStreamLogicalRepairResp streamResp =
        sendRequestExpect(
            leaderLocation.getInternalEndPoint(),
            new TStreamLogicalRepairReq(
                consensusGroupId,
                operation.partitionId,
                operation.repairEpoch,
                Collections.singletonList(
                    new TLogicalRepairLeafSelector(
                        operation.scope.getTreeKind(), encodeRepairSelector(operation.scope)))),
            CnToDnSyncRequestType.STREAM_LOGICAL_REPAIR,
            TStreamLogicalRepairResp.class);
    requireSuccess(streamResp.getStatus(), "streamLogicalRepair");
    if (Boolean.TRUE.equals(streamResp.isStale())) {
      throw new IllegalStateException("Leader logical repair stream is stale");
    }

    String sessionId = null;
    if (streamResp.isSetBatches()) {
      for (TLogicalRepairBatch batch : streamResp.getBatches()) {
        sessionId = batch.getSessionId();
        TSStatus applyStatus =
            sendRequestExpect(
                operation.followerLocation.getInternalEndPoint(),
                new TApplyLogicalRepairBatchReq(
                    consensusGroupId,
                    operation.partitionId,
                    operation.repairEpoch,
                    batch.getSessionId(),
                    batch.getTreeKind(),
                    batch.getLeafId(),
                    batch.getSeqNo(),
                    batch.getBatchKind(),
                    batch.bufferForPayload()),
                CnToDnSyncRequestType.APPLY_LOGICAL_REPAIR_BATCH,
                TSStatus.class);
        requireSuccess(applyStatus, "applyLogicalRepairBatch");
      }
    }
    if (sessionId != null) {
      TSStatus finishStatus =
          sendRequestExpect(
              operation.followerLocation.getInternalEndPoint(),
              new TFinishLogicalRepairSessionReq(
                  consensusGroupId, operation.partitionId, operation.repairEpoch, sessionId),
              CnToDnSyncRequestType.FINISH_LOGICAL_REPAIR_SESSION,
              TSStatus.class);
      requireSuccess(finishStatus, "finishLogicalRepairSession");
    }
  }

  @Override
  public RepairProgressTable loadRepairProgressTable(String consensusGroupKey) {
    return consistencyProgressManager.loadRepairProgressTable(consensusGroupKey);
  }

  @Override
  public void persistRepairProgressTable(RepairProgressTable repairProgressTable) {
    consistencyProgressManager.persistRepairProgressTable(repairProgressTable);
  }

  @Override
  public void onPartitionCommitted(
      long partitionId, long committedAt, RepairProgressTable repairProgressTable) {
    TPartitionConsistencyEligibility leaderEligibility =
        leaderEligibilityByPartition.get(partitionId);
    if (leaderEligibility != null) {
      RepairProgressTable.PartitionProgress progress =
          repairProgressTable.getOrCreatePartition(partitionId);
      repairProgressTable.markRepairSucceeded(
          partitionId,
          committedAt,
          safeWatermark,
          leaderEligibility.getPartitionMutationEpoch(),
          leaderEligibility.getSnapshotEpoch(),
          RepairProgressTable.SnapshotState.READY,
          progress.getRepairEpoch(),
          progress.getReplicaObservationToken());
    }
  }

  private Set<LogicalMismatchScope.Scope> comparePartition(
      TDataNodeLocation followerLocation, long partitionId, String treeKind) {
    Set<LogicalMismatchScope.Scope> scopes = new LinkedHashSet<>();

    Map<String, TSnapshotSubtreeNode> leaderShards =
        fetchSubtreeNodes(leaderLocation, partitionId, treeKind, Collections.singletonList("root"));
    Map<String, TSnapshotSubtreeNode> followerShards =
        fetchSubtreeNodes(
            followerLocation, partitionId, treeKind, Collections.singletonList("root"));

    Set<String> shardHandles = new LinkedHashSet<>();
    shardHandles.addAll(leaderShards.keySet());
    shardHandles.addAll(followerShards.keySet());
    for (String shardHandle : shardHandles) {
      TSnapshotSubtreeNode leaderShard = leaderShards.get(shardHandle);
      TSnapshotSubtreeNode followerShard = followerShards.get(shardHandle);
      if (sameDigest(leaderShard, followerShard)) {
        continue;
      }
      Map<String, TSnapshotSubtreeNode> leaderLeaves =
          fetchSubtreeNodes(
              leaderLocation, partitionId, treeKind, Collections.singletonList(shardHandle));
      Map<String, TSnapshotSubtreeNode> followerLeaves =
          fetchSubtreeNodes(
              followerLocation, partitionId, treeKind, Collections.singletonList(shardHandle));
      Set<String> leafHandles = new LinkedHashSet<>();
      leafHandles.addAll(leaderLeaves.keySet());
      leafHandles.addAll(followerLeaves.keySet());
      for (String leafHandle : leafHandles) {
        TSnapshotSubtreeNode leaderLeaf = leaderLeaves.get(leafHandle);
        TSnapshotSubtreeNode followerLeaf = followerLeaves.get(leafHandle);
        if (!sameDigest(leaderLeaf, followerLeaf)) {
          scopes.addAll(
              compareLeaf(
                  followerLocation, partitionId, treeKind, leafHandle, leaderLeaf, followerLeaf));
        }
      }
    }
    return scopes;
  }

  private Set<LogicalMismatchScope.Scope> compareLeaf(
      TDataNodeLocation followerLocation,
      long partitionId,
      String treeKind,
      String leafId,
      TSnapshotSubtreeNode leaderLeaf,
      TSnapshotSubtreeNode followerLeaf) {
    long leaderItemCount = leaderLeaf == null ? 0L : leaderLeaf.getItemCount();
    long followerItemCount = followerLeaf == null ? 0L : followerLeaf.getItemCount();
    if (Math.max(leaderItemCount, followerItemCount) > EXACT_DIFF_DECODE_THRESHOLD) {
      return Collections.singleton(
          new LogicalMismatchScope.Scope(
              treeKind,
              leafId,
              chooseKeyRangeStart(leaderLeaf, followerLeaf),
              chooseKeyRangeEnd(leaderLeaf, followerLeaf)));
    }

    TLeafDiffEstimate leaderEstimate =
        fetchLeafEstimate(leaderLocation, partitionId, treeKind, leafId);
    TLeafDiffEstimate followerEstimate =
        fetchLeafEstimate(followerLocation, partitionId, treeKind, leafId);
    if (leaderEstimate == null || followerEstimate == null) {
      return Collections.singleton(
          new LogicalMismatchScope.Scope(
              treeKind,
              leafId,
              chooseKeyRangeStart(leaderLeaf, followerLeaf),
              chooseKeyRangeEnd(leaderLeaf, followerLeaf)));
    }
    if (Math.max(leaderEstimate.getStrataEstimate(), followerEstimate.getStrataEstimate())
        > EXACT_DIFF_DECODE_THRESHOLD) {
      return Collections.singleton(
          new LogicalMismatchScope.Scope(
              treeKind,
              leafId,
              chooseKeyRangeStart(leaderLeaf, followerLeaf),
              chooseKeyRangeEnd(leaderLeaf, followerLeaf)));
    }

    Set<String> leaderKeys = fetchLeafDiffKeys(leaderLocation, partitionId, treeKind, leafId);
    Set<String> followerKeys = fetchLeafDiffKeys(followerLocation, partitionId, treeKind, leafId);
    if (leaderKeys == null || followerKeys == null) {
      return Collections.singleton(
          new LogicalMismatchScope.Scope(
              treeKind,
              leafId,
              chooseKeyRangeStart(leaderLeaf, followerLeaf),
              chooseKeyRangeEnd(leaderLeaf, followerLeaf)));
    }

    Set<String> symmetricDiff = new LinkedHashSet<>(leaderKeys);
    symmetricDiff.addAll(followerKeys);
    Set<String> intersection = new LinkedHashSet<>(leaderKeys);
    intersection.retainAll(followerKeys);
    symmetricDiff.removeAll(intersection);
    if (symmetricDiff.isEmpty()) {
      return Collections.emptySet();
    }

    List<String> sortedDiffKeys = new ArrayList<>(symmetricDiff);
    sortedDiffKeys.sort(String::compareTo);
    if (TREE_KIND_TOMBSTONE.equalsIgnoreCase(treeKind)) {
      Set<String> leaderOnly = new LinkedHashSet<>(leaderKeys);
      leaderOnly.removeAll(followerKeys);
      List<String> sortedLeaderOnlyKeys = new ArrayList<>(leaderOnly);
      sortedLeaderOnlyKeys.sort(String::compareTo);

      Set<String> followerOnly = new LinkedHashSet<>(followerKeys);
      followerOnly.removeAll(leaderKeys);
      if (!followerOnly.isEmpty()) {
        return Collections.singleton(
            new LogicalMismatchScope.Scope(
                treeKind,
                leafId,
                sortedDiffKeys.get(0),
                sortedDiffKeys.get(sortedDiffKeys.size() - 1),
                sortedLeaderOnlyKeys,
                LogicalMismatchScope.RepairDirective.FOLLOWER_EXTRA_TOMBSTONE));
      }

      return Collections.singleton(
          new LogicalMismatchScope.Scope(
              treeKind,
              leafId,
              sortedDiffKeys.get(0),
              sortedDiffKeys.get(sortedDiffKeys.size() - 1),
              sortedLeaderOnlyKeys));
    }
    return Collections.singleton(
        new LogicalMismatchScope.Scope(
            treeKind,
            leafId,
            sortedDiffKeys.get(0),
            sortedDiffKeys.get(sortedDiffKeys.size() - 1),
            sortedDiffKeys));
  }

  private String buildBlockingReason(long partitionId, Set<LogicalMismatchScope.Scope> scopes) {
    List<String> unsupportedScopes =
        scopes.stream()
            .filter(scope -> scope != null && !scope.isRepairable())
            .map(scope -> scope.getTreeKind() + "@" + scope.getLeafId())
            .sorted()
            .collect(Collectors.toList());
    if (unsupportedScopes.isEmpty()) {
      return null;
    }
    return "Partition "
        + partitionId
        + " contains follower-only tombstone mismatches that cannot be rolled back safely yet: "
        + String.join(", ", unsupportedScopes);
  }

  private Map<String, TSnapshotSubtreeNode> fetchSubtreeNodes(
      TDataNodeLocation location, long partitionId, String treeKind, List<String> nodeHandles) {
    TPartitionConsistencyEligibility localEligibility =
        getEligibilityForLocation(location, partitionId);
    if (localEligibility == null) {
      throw new StaleSnapshotCompareException(
          "Missing snapshot eligibility while comparing partition " + partitionId);
    }
    TGetSnapshotSubtreeResp resp =
        sendRequestExpect(
            location.getInternalEndPoint(),
            new TGetSnapshotSubtreeReq(
                consensusGroupId,
                partitionId,
                localEligibility.getSnapshotEpoch(),
                treeKind,
                nodeHandles),
            CnToDnSyncRequestType.GET_SNAPSHOT_SUBTREE,
            TGetSnapshotSubtreeResp.class);
    requireSuccess(resp.getStatus(), "getSnapshotSubtree");
    if (Boolean.TRUE.equals(resp.isStale())) {
      throw new StaleSnapshotCompareException(
          "Snapshot subtree became stale while comparing partition " + partitionId);
    }
    Map<String, TSnapshotSubtreeNode> nodes = new LinkedHashMap<>();
    if (resp.isSetNodes()) {
      for (TSnapshotSubtreeNode node : resp.getNodes()) {
        nodes.put(node.getNodeHandle(), node);
      }
    }
    return nodes;
  }

  private TLeafDiffEstimate fetchLeafEstimate(
      TDataNodeLocation location, long partitionId, String treeKind, String leafId) {
    TPartitionConsistencyEligibility localEligibility =
        getEligibilityForLocation(location, partitionId);
    if (localEligibility == null) {
      return null;
    }
    TEstimateLeafDiffResp resp =
        sendRequestExpect(
            location.getInternalEndPoint(),
            new TEstimateLeafDiffReq(
                consensusGroupId,
                partitionId,
                localEligibility.getSnapshotEpoch(),
                treeKind,
                leafId),
            CnToDnSyncRequestType.ESTIMATE_LEAF_DIFF,
            TEstimateLeafDiffResp.class);
    requireSuccess(resp.getStatus(), "estimateLeafDiff");
    if (Boolean.TRUE.equals(resp.isStale()) || !resp.isSetLeafDiff()) {
      return null;
    }
    return resp.getLeafDiff();
  }

  private Set<String> fetchLeafDiffKeys(
      TDataNodeLocation location, long partitionId, String treeKind, String leafId) {
    TPartitionConsistencyEligibility localEligibility =
        getEligibilityForLocation(location, partitionId);
    if (localEligibility == null) {
      return null;
    }
    TDecodeLeafDiffResp resp =
        sendRequestExpect(
            location.getInternalEndPoint(),
            new TDecodeLeafDiffReq(
                consensusGroupId,
                partitionId,
                localEligibility.getSnapshotEpoch(),
                treeKind,
                leafId),
            CnToDnSyncRequestType.DECODE_LEAF_DIFF,
            TDecodeLeafDiffResp.class);
    requireSuccess(resp.getStatus(), "decodeLeafDiff");
    if (Boolean.TRUE.equals(resp.isStale())) {
      return null;
    }
    Set<String> logicalKeys = new LinkedHashSet<>();
    if (resp.isSetDiffEntries()) {
      for (TLeafDiffEntry entry : resp.getDiffEntries()) {
        logicalKeys.add(entry.getLogicalKey());
      }
    }
    return logicalKeys;
  }

  private EligibilitySnapshot fetchEligibility(
      TDataNodeLocation location, boolean tolerateUnavailable) {
    Object response =
        sendRequest(
            location.getInternalEndPoint(),
            new TGetConsistencyEligibilityReq(consensusGroupId),
            CnToDnSyncRequestType.GET_CONSISTENCY_ELIGIBILITY);
    if (response == null) {
      if (tolerateUnavailable) {
        return EligibilitySnapshot.unavailable();
      }
      throw new IllegalStateException("getConsistencyEligibility failed: null response");
    }
    if (response instanceof TSStatus) {
      TSStatus status = (TSStatus) response;
      if (tolerateUnavailable) {
        return EligibilitySnapshot.unavailable();
      }
      throw new IllegalStateException("getConsistencyEligibility failed: " + status.getMessage());
    }
    if (!(response instanceof TGetConsistencyEligibilityResp)) {
      throw new IllegalStateException(
          "GET_CONSISTENCY_ELIGIBILITY returned unexpected response type "
              + response.getClass().getSimpleName());
    }
    TGetConsistencyEligibilityResp resp = (TGetConsistencyEligibilityResp) response;
    requireSuccess(resp.getStatus(), "getConsistencyEligibility");
    return new EligibilitySnapshot(resp);
  }

  private void refreshLeaderEligibility() {
    if (!isRouteBindingStillValid()) {
      syncLag = Long.MAX_VALUE;
      safeWatermark = Long.MIN_VALUE;
      leaderEligibilityByPartition.clear();
      followerEligibilityByNodeId.clear();
      return;
    }
    EligibilitySnapshot snapshot = fetchEligibility(leaderLocation, !repairMode);
    if (!snapshot.isAvailable()) {
      syncLag = Long.MAX_VALUE;
      safeWatermark = Long.MIN_VALUE;
      leaderEligibilityByPartition.clear();
      followerEligibilityByNodeId.clear();
      return;
    }
    syncLag = snapshot.syncLag;
    safeWatermark = snapshot.safeWatermark;
    leaderEligibilityByPartition.clear();
    leaderEligibilityByPartition.putAll(snapshot.partitionsById);
  }

  private Map<Long, String> buildReplicaObservationTokens() {
    Map<Long, String> tokens = new LinkedHashMap<>();
    for (Long partitionId : leaderEligibilityByPartition.keySet()) {
      tokens.put(partitionId, buildReplicaObservationToken(partitionId));
    }
    return tokens;
  }

  private String buildReplicaObservationToken(long partitionId) {
    StringBuilder builder = new StringBuilder();
    appendEligibilityToken(
        builder, leaderLocation.getDataNodeId(), leaderEligibilityByPartition.get(partitionId));
    for (TDataNodeLocation followerLocation : followerLocations) {
      builder.append('|');
      appendEligibilityToken(
          builder,
          followerLocation.getDataNodeId(),
          followerEligibilityByNodeId
              .getOrDefault(followerLocation.getDataNodeId(), Collections.emptyMap())
              .get(partitionId));
    }
    return builder.toString();
  }

  private void appendEligibilityToken(
      StringBuilder builder, int dataNodeId, TPartitionConsistencyEligibility eligibility) {
    builder.append(dataNodeId).append(':');
    if (eligibility == null) {
      builder.append("MISSING");
      return;
    }
    builder
        .append(eligibility.getPartitionMutationEpoch())
        .append(':')
        .append(eligibility.getSnapshotEpoch())
        .append(':')
        .append(eligibility.getSnapshotState())
        .append(':')
        .append(eligibility.getLiveRootXorHash())
        .append(':')
        .append(eligibility.getLiveRootAddHash())
        .append(':')
        .append(eligibility.getTombstoneRootXorHash())
        .append(':')
        .append(eligibility.getTombstoneRootAddHash());
  }

  private TPartitionConsistencyEligibility getEligibilityForLocation(
      TDataNodeLocation location, long partitionId) {
    if (location == null) {
      return null;
    }
    if (location.getDataNodeId() == leaderLocation.getDataNodeId()) {
      return leaderEligibilityByPartition.get(partitionId);
    }
    return followerEligibilityByNodeId
        .getOrDefault(location.getDataNodeId(), Collections.emptyMap())
        .get(partitionId);
  }

  private boolean isSnapshotComparable(TPartitionConsistencyEligibility eligibility) {
    return eligibility != null
        && "READY".equalsIgnoreCase(eligibility.getSnapshotState())
        && eligibility.getSnapshotEpoch() == eligibility.getPartitionMutationEpoch();
  }

  private boolean sameRootDigest(
      TPartitionConsistencyEligibility left, TPartitionConsistencyEligibility right) {
    return left.getLiveRootXorHash() == right.getLiveRootXorHash()
        && left.getLiveRootAddHash() == right.getLiveRootAddHash();
  }

  private boolean sameTombstoneDigest(
      TPartitionConsistencyEligibility left, TPartitionConsistencyEligibility right) {
    return left.getTombstoneRootXorHash() == right.getTombstoneRootXorHash()
        && left.getTombstoneRootAddHash() == right.getTombstoneRootAddHash();
  }

  private boolean sameDigest(TSnapshotSubtreeNode left, TSnapshotSubtreeNode right) {
    if (left == null || right == null) {
      return false;
    }
    return left.getXorHash() == right.getXorHash() && left.getAddHash() == right.getAddHash();
  }

  private String buildRepairEpoch(
      long partitionId, TPartitionConsistencyEligibility leaderEligibility) {
    return leaderLocation.getDataNodeId()
        + ":"
        + partitionId
        + ":"
        + safeWatermark
        + ":"
        + leaderEligibility.getSnapshotEpoch()
        + ":"
        + leaderEligibility.getPartitionMutationEpoch()
        + ":"
        + routeVersionToken;
  }

  private boolean isRouteBindingStillValid() {
    int currentLeaderId =
        configManager.getLoadManager().getRegionLeaderMap().getOrDefault(consensusGroupId, -1);
    return currentLeaderId == leaderLocation.getDataNodeId()
        && Objects.equals(routeVersionToken, buildRouteBindingToken(currentLeaderId));
  }

  private String buildRouteBindingToken(int leaderId) {
    TRegionReplicaSet routeReplicaSet =
        configManager.getLoadManager().getRegionPriorityMap().get(consensusGroupId);
    if (routeReplicaSet == null || routeReplicaSet.getDataNodeLocations() == null) {
      routeReplicaSet =
          configManager
              .getPartitionManager()
              .getAllReplicaSetsMap(TConsensusGroupType.DataRegion)
              .get(consensusGroupId);
    }
    List<Integer> routeNodeIds =
        routeReplicaSet == null || routeReplicaSet.getDataNodeLocations() == null
            ? Collections.emptyList()
            : routeReplicaSet.getDataNodeLocations().stream()
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toList());
    return leaderId
        + "-"
        + routeNodeIds.stream().map(String::valueOf).collect(Collectors.joining("_"));
  }

  private String encodeRepairSelector(LogicalMismatchScope.Scope scope) {
    if ((scope.getExactKeys() == null || scope.getExactKeys().isEmpty())
        && scope.getKeyRangeStart() == null
        && scope.getKeyRangeEnd() == null) {
      return scope.getLeafId();
    }
    List<String> exactKeys =
        scope.getExactKeys() == null ? Collections.emptyList() : scope.getExactKeys();
    return scope.getLeafId()
        + "@"
        + encodeNullable(scope.getKeyRangeStart())
        + "@"
        + encodeNullable(scope.getKeyRangeEnd())
        + "@"
        + encodeNullable(String.join("\n", exactKeys));
  }

  private String encodeNullable(String value) {
    if (value == null) {
      return "";
    }
    return java.util.Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private String chooseKeyRangeStart(
      TSnapshotSubtreeNode leaderLeaf, TSnapshotSubtreeNode followerLeaf) {
    if (leaderLeaf != null && leaderLeaf.isSetKeyRangeStart()) {
      return leaderLeaf.getKeyRangeStart();
    }
    return followerLeaf != null && followerLeaf.isSetKeyRangeStart()
        ? followerLeaf.getKeyRangeStart()
        : null;
  }

  private String chooseKeyRangeEnd(
      TSnapshotSubtreeNode leaderLeaf, TSnapshotSubtreeNode followerLeaf) {
    if (leaderLeaf != null && leaderLeaf.isSetKeyRangeEnd()) {
      return leaderLeaf.getKeyRangeEnd();
    }
    return followerLeaf != null && followerLeaf.isSetKeyRangeEnd()
        ? followerLeaf.getKeyRangeEnd()
        : null;
  }

  private Object sendRequest(TEndPoint endPoint, Object req, CnToDnSyncRequestType requestType) {
    return SyncDataNodeClientPool.getInstance()
        .sendSyncRequestToDataNodeWithRetry(endPoint, req, requestType);
  }

  private <T> T sendRequestExpect(
      TEndPoint endPoint, Object req, CnToDnSyncRequestType requestType, Class<T> responseType) {
    Object response = sendRequest(endPoint, req, requestType);
    if (!responseType.isInstance(response)) {
      throw new IllegalStateException(
          requestType
              + " returned unexpected response type "
              + response.getClass().getSimpleName());
    }
    return responseType.cast(response);
  }

  private void requireSuccess(TSStatus status, String action) {
    if (status == null || status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IllegalStateException(
          action + " failed: " + (status == null ? "null status" : status.getMessage()));
    }
  }

  private static TConsensusGroupId requireDataRegion(TConsensusGroupId consensusGroupId) {
    if (consensusGroupId == null || consensusGroupId.getType() != TConsensusGroupType.DataRegion) {
      throw new IllegalArgumentException("Only DataRegion consistency repair is supported");
    }
    return consensusGroupId;
  }

  private static class EligibilitySnapshot {
    private final boolean available;
    private final long syncLag;
    private final long safeWatermark;
    private final Map<Long, TPartitionConsistencyEligibility> partitionsById =
        new LinkedHashMap<>();

    private EligibilitySnapshot() {
      this.available = false;
      this.syncLag = Long.MAX_VALUE;
      this.safeWatermark = Long.MIN_VALUE;
    }

    private EligibilitySnapshot(TGetConsistencyEligibilityResp resp) {
      this.available = true;
      this.syncLag = resp.getSyncLag();
      this.safeWatermark = resp.getSafeWatermark();
      if (resp.isSetPartitions()) {
        for (TPartitionConsistencyEligibility partition : resp.getPartitions()) {
          partitionsById.put(partition.getTimePartitionId(), partition);
        }
      }
    }

    private static EligibilitySnapshot unavailable() {
      return new EligibilitySnapshot();
    }

    private boolean isAvailable() {
      return available;
    }

    private Map<Long, TPartitionConsistencyEligibility> getPartitionsMap() {
      return partitionsById;
    }
  }

  private static final class StaleSnapshotCompareException extends IllegalStateException {
    private StaleSnapshotCompareException(String message) {
      super(message);
    }
  }

  private static class RepairLeafKey {
    private final int followerDataNodeId;
    private final String treeKind;
    private final String leafId;
    private final String scopeRef;

    private RepairLeafKey(int followerDataNodeId, String treeKind, String leafId, String scopeRef) {
      this.followerDataNodeId = followerDataNodeId;
      this.treeKind = treeKind;
      this.leafId = leafId;
      this.scopeRef = scopeRef;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RepairLeafKey)) {
        return false;
      }
      RepairLeafKey that = (RepairLeafKey) obj;
      return followerDataNodeId == that.followerDataNodeId
          && Objects.equals(treeKind, that.treeKind)
          && Objects.equals(leafId, that.leafId)
          && Objects.equals(scopeRef, that.scopeRef);
    }

    @Override
    public int hashCode() {
      return Objects.hash(followerDataNodeId, treeKind, leafId, scopeRef);
    }
  }

  private static class RepairOperation {
    private final long partitionId;
    private final TDataNodeLocation followerLocation;
    private final LogicalMismatchScope.Scope scope;
    private final String repairEpoch;

    private RepairOperation(
        long partitionId,
        TDataNodeLocation followerLocation,
        LogicalMismatchScope.Scope scope,
        String repairEpoch) {
      this.partitionId = partitionId;
      this.followerLocation = followerLocation;
      this.scope = scope;
      this.repairEpoch = repairEpoch;
    }
  }

  private final class LivePartitionRepairContext
      implements RepairRegionProcedure.PartitionRepairContext {
    private final long partitionId;
    private final boolean rootHashMatched;
    private final List<String> repairOperationIds;
    private final String repairEpoch;
    private final String blockingReason;

    private LivePartitionRepairContext(
        long partitionId,
        boolean rootHashMatched,
        List<String> repairOperationIds,
        String repairEpoch,
        String blockingReason) {
      this.partitionId = partitionId;
      this.rootHashMatched = rootHashMatched;
      this.repairOperationIds = repairOperationIds;
      this.repairEpoch = repairEpoch;
      this.blockingReason = blockingReason;
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
    public List<String> getRepairOperationIds() {
      return repairOperationIds;
    }

    @Override
    public String getRepairEpoch() {
      return repairEpoch;
    }

    @Override
    public String getBlockingReason() {
      return blockingReason;
    }

    @Override
    public boolean verify() {
      if (!isRouteBindingStillValid()) {
        return false;
      }
      refreshLeaderEligibility();
      TPartitionConsistencyEligibility leaderEligibility =
          leaderEligibilityByPartition.get(partitionId);
      if (!isSnapshotComparable(leaderEligibility)
          || !Objects.equals(repairEpoch, buildRepairEpoch(partitionId, leaderEligibility))) {
        return false;
      }
      for (TDataNodeLocation followerLocation : followerLocations) {
        TPartitionConsistencyEligibility followerEligibility =
            fetchEligibility(followerLocation, false).getPartitionsMap().get(partitionId);
        if (!isSnapshotComparable(followerEligibility)
            || !sameRootDigest(leaderEligibility, followerEligibility)
            || !sameTombstoneDigest(leaderEligibility, followerEligibility)) {
          return false;
        }
      }
      return true;
    }
  }
}
