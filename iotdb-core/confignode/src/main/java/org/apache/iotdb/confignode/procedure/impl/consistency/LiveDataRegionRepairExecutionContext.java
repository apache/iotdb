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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DiffEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.RowRefIndex;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairConflictResolver;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairRecord;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.mpp.rpc.thrift.TConsistencyMerkleFile;
import org.apache.iotdb.mpp.rpc.thrift.TDataRegionConsistencySnapshotReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataRegionConsistencySnapshotResp;
import org.apache.iotdb.mpp.rpc.thrift.TRepairTransferTsFileReq;
import org.apache.iotdb.mpp.rpc.thrift.TTimePartitionConsistencyView;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Live runtime bridge for {@link RepairRegionProcedure}. The first production path only supports
 * leader-authoritative repair when one or more followers are missing sealed TsFiles that still
 * exist on the leader.
 */
public class LiveDataRegionRepairExecutionContext
    implements RepairRegionProcedure.RepairExecutionContext {

  private final TConsensusGroupId consensusGroupId;
  private final TDataNodeLocation leaderLocation;
  private final List<TDataNodeLocation> followerLocations;
  private final Map<Long, LivePartitionRepairContext> partitionContexts;
  private final Map<String, List<TDataNodeLocation>> transferTargetsByTsFile;

  public LiveDataRegionRepairExecutionContext(
      ConfigManager configManager, TConsensusGroupId consensusGroupId) {
    this.consensusGroupId = requireDataRegion(consensusGroupId);

    TRegionReplicaSet replicaSet =
        configManager
            .getPartitionManager()
            .getAllReplicaSetsMap(TConsensusGroupType.DataRegion)
            .get(this.consensusGroupId);
    if (replicaSet == null) {
      throw new IllegalStateException("DataRegion " + consensusGroupId + " does not exist");
    }
    if (replicaSet.getDataNodeLocations() == null || replicaSet.getDataNodeLocations().size() < 2) {
      throw new IllegalStateException(
          "DataRegion " + consensusGroupId + " has fewer than 2 replicas, no repair target exists");
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

    Map<Long, List<SnapshotFile>> leaderSnapshot = fetchSnapshot(leaderLocation);
    Map<Integer, Map<Long, List<SnapshotFile>>> followerSnapshots = new LinkedHashMap<>();
    for (TDataNodeLocation followerLocation : followerLocations) {
      followerSnapshots.put(followerLocation.getDataNodeId(), fetchSnapshot(followerLocation));
    }

    this.partitionContexts = new TreeMap<>();
    this.transferTargetsByTsFile = new LinkedHashMap<>();
    initializePartitionContexts(leaderSnapshot, followerSnapshots);
  }

  @Override
  public boolean isReplicationComplete() {
    return true;
  }

  @Override
  public long computeSafeWatermark() {
    return Long.MAX_VALUE;
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
    return null;
  }

  @Override
  public void transferTsFile(String tsFilePath) {
    List<TDataNodeLocation> targetDataNodes = transferTargetsByTsFile.get(tsFilePath);
    if (targetDataNodes == null || targetDataNodes.isEmpty()) {
      throw new IllegalStateException("No repair target found for TsFile " + tsFilePath);
    }

    TRepairTransferTsFileReq request =
        new TRepairTransferTsFileReq(consensusGroupId, tsFilePath, targetDataNodes);
    Object response =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                leaderLocation.getInternalEndPoint(),
                request,
                CnToDnSyncRequestType.REPAIR_TRANSFER_TSFILE);

    TSStatus status =
        response instanceof TSStatus
            ? (TSStatus) response
            : new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                .setMessage("Unexpected response type for repair TsFile transfer: " + response);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IllegalStateException(
          "Repair transfer of TsFile " + tsFilePath + " failed: " + status.getMessage());
    }
  }

  private void initializePartitionContexts(
      Map<Long, List<SnapshotFile>> leaderSnapshot,
      Map<Integer, Map<Long, List<SnapshotFile>>> followerSnapshots) {
    Set<Long> partitionIds = new LinkedHashSet<>(leaderSnapshot.keySet());
    followerSnapshots.values().forEach(snapshot -> partitionIds.addAll(snapshot.keySet()));

    for (Long partitionId : partitionIds.stream().sorted().collect(Collectors.toList())) {
      List<SnapshotFile> leaderFiles =
          leaderSnapshot.getOrDefault(partitionId, Collections.emptyList());
      if (leaderFiles.isEmpty()) {
        for (TDataNodeLocation followerLocation : followerLocations) {
          List<SnapshotFile> followerFiles =
              followerSnapshots
                  .getOrDefault(followerLocation.getDataNodeId(), Collections.emptyMap())
                  .getOrDefault(partitionId, Collections.emptyList());
          if (!followerFiles.isEmpty()) {
            throw new IllegalStateException(
                String.format(
                    "Partition %d of region %s has follower-only data on DataNode %d",
                    partitionId, consensusGroupId, followerLocation.getDataNodeId()));
          }
        }
        continue;
      }

      Map<DigestKey, Integer> leaderCounts = buildDigestCounts(leaderFiles);
      Map<DigestKey, List<SnapshotFile>> leaderFilesByDigest = groupFilesByDigest(leaderFiles);
      Map<String, List<TDataNodeLocation>> transferTargets = new LinkedHashMap<>();
      boolean matched = true;

      for (TDataNodeLocation followerLocation : followerLocations) {
        List<SnapshotFile> followerFiles =
            followerSnapshots
                .getOrDefault(followerLocation.getDataNodeId(), Collections.emptyMap())
                .getOrDefault(partitionId, Collections.emptyList());
        Map<DigestKey, Integer> followerCounts = buildDigestCounts(followerFiles);

        for (Map.Entry<DigestKey, Integer> followerEntry : followerCounts.entrySet()) {
          int leaderCount = leaderCounts.getOrDefault(followerEntry.getKey(), 0);
          if (followerEntry.getValue() > leaderCount) {
            throw new IllegalStateException(
                String.format(
                    "Partition %d of region %s has follower-only digest %s on DataNode %d",
                    partitionId,
                    consensusGroupId,
                    followerEntry.getKey(),
                    followerLocation.getDataNodeId()));
          }
        }

        for (Map.Entry<DigestKey, Integer> leaderEntry : leaderCounts.entrySet()) {
          int followerCount = followerCounts.getOrDefault(leaderEntry.getKey(), 0);
          int missingCount = leaderEntry.getValue() - followerCount;
          if (missingCount <= 0) {
            continue;
          }

          matched = false;
          List<SnapshotFile> candidateFiles = leaderFilesByDigest.get(leaderEntry.getKey());
          for (int i = 0; i < missingCount; i++) {
            SnapshotFile snapshotFile = candidateFiles.get(i);
            transferTargets
                .computeIfAbsent(snapshotFile.getSourceTsFilePath(), ignored -> new ArrayList<>())
                .add(followerLocation);
          }
        }
      }

      if (matched) {
        continue;
      }

      LivePartitionRepairContext partitionContext =
          new LivePartitionRepairContext(partitionId, leaderFiles, transferTargets);
      partitionContexts.put(partitionId, partitionContext);
      transferTargets.forEach(
          (tsFilePath, targets) ->
              transferTargetsByTsFile
                  .computeIfAbsent(tsFilePath, ignored -> new ArrayList<>())
                  .addAll(targets));
    }
  }

  private Map<Long, List<SnapshotFile>> fetchSnapshot(TDataNodeLocation dataNodeLocation) {
    Object response =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                dataNodeLocation.getInternalEndPoint(),
                new TDataRegionConsistencySnapshotReq(consensusGroupId),
                CnToDnSyncRequestType.GET_DATA_REGION_CONSISTENCY_SNAPSHOT);

    if (response instanceof TSStatus) {
      TSStatus status = (TSStatus) response;
      throw new IllegalStateException(
          "Failed to fetch consistency snapshot from DataNode "
              + dataNodeLocation.getDataNodeId()
              + ": "
              + status.getMessage());
    }
    if (!(response instanceof TDataRegionConsistencySnapshotResp)) {
      throw new IllegalStateException(
          "Unexpected snapshot response type from DataNode "
              + dataNodeLocation.getDataNodeId()
              + ": "
              + response);
    }

    TDataRegionConsistencySnapshotResp snapshotResp = (TDataRegionConsistencySnapshotResp) response;
    if (snapshotResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IllegalStateException(
          "Failed to fetch consistency snapshot from DataNode "
              + dataNodeLocation.getDataNodeId()
              + ": "
              + snapshotResp.getStatus().getMessage());
    }

    Map<Long, List<SnapshotFile>> snapshot = new TreeMap<>();
    if (!snapshotResp.isSetTimePartitionViews()) {
      return snapshot;
    }

    for (TTimePartitionConsistencyView partitionView : snapshotResp.getTimePartitionViews()) {
      List<SnapshotFile> files = new ArrayList<>();
      if (partitionView.isSetMerkleFiles()) {
        for (TConsistencyMerkleFile merkleFile : partitionView.getMerkleFiles()) {
          files.add(
              new SnapshotFile(
                  merkleFile.getSourceTsFilePath(),
                  merkleFile.getTsFileSize(),
                  new DigestKey(merkleFile.getFileXorHash(), merkleFile.getFileAddHash())));
        }
      }
      files.sort(Comparator.comparing(SnapshotFile::getSourceTsFilePath));
      snapshot.put(partitionView.getTimePartitionId(), files);
    }
    return snapshot;
  }

  private boolean verifyPartition(long partitionId) {
    Map<Long, List<SnapshotFile>> leaderSnapshot = fetchSnapshot(leaderLocation);
    Map<DigestKey, Integer> leaderCounts =
        buildDigestCounts(leaderSnapshot.getOrDefault(partitionId, Collections.emptyList()));

    for (TDataNodeLocation followerLocation : followerLocations) {
      Map<Long, List<SnapshotFile>> followerSnapshot = fetchSnapshot(followerLocation);
      Map<DigestKey, Integer> followerCounts =
          buildDigestCounts(followerSnapshot.getOrDefault(partitionId, Collections.emptyList()));
      if (!leaderCounts.equals(followerCounts)) {
        return false;
      }
    }
    return true;
  }

  private static TConsensusGroupId requireDataRegion(TConsensusGroupId consensusGroupId) {
    if (consensusGroupId == null
        || consensusGroupId.getType() == null
        || consensusGroupId.getType() != TConsensusGroupType.DataRegion) {
      throw new IllegalArgumentException(
          "Replica consistency repair currently only supports DataRegion");
    }
    return consensusGroupId;
  }

  private static Map<DigestKey, Integer> buildDigestCounts(Collection<SnapshotFile> files) {
    Map<DigestKey, Integer> counts = new HashMap<>();
    for (SnapshotFile file : files) {
      counts.merge(file.getDigestKey(), 1, Integer::sum);
    }
    return counts;
  }

  private static Map<DigestKey, List<SnapshotFile>> groupFilesByDigest(List<SnapshotFile> files) {
    Map<DigestKey, List<SnapshotFile>> grouped = new LinkedHashMap<>();
    for (SnapshotFile file : files) {
      grouped.computeIfAbsent(file.getDigestKey(), ignored -> new ArrayList<>()).add(file);
    }
    return grouped;
  }

  private final class LivePartitionRepairContext
      implements RepairRegionProcedure.PartitionRepairContext {

    private final long partitionId;
    private final List<MerkleFileContent> leaderMerkleFiles;
    private final Map<String, MerkleFileContent> leaderMerkleFileByPath;
    private final Map<String, Long> leaderTsFileSizeByPath;
    private final Map<String, List<TDataNodeLocation>> transferTargets;

    private LivePartitionRepairContext(
        long partitionId,
        List<SnapshotFile> leaderFiles,
        Map<String, List<TDataNodeLocation>> transferTargets) {
      this.partitionId = partitionId;
      this.leaderMerkleFiles =
          leaderFiles.stream()
              .map(
                  file ->
                      new MerkleFileContent(
                          file.getDigestKey().getFileXorHash(),
                          file.getDigestKey().getFileAddHash(),
                          Collections.emptyList(),
                          file.getSourceTsFilePath()))
              .collect(Collectors.toList());
      this.leaderMerkleFileByPath =
          this.leaderMerkleFiles.stream()
              .collect(
                  Collectors.toMap(
                      MerkleFileContent::getSourceTsFilePath,
                      file -> file,
                      (left, right) -> left,
                      LinkedHashMap::new));
      this.leaderTsFileSizeByPath =
          leaderFiles.stream()
              .collect(
                  Collectors.toMap(
                      SnapshotFile::getSourceTsFilePath,
                      SnapshotFile::getTsFileSize,
                      (left, right) -> left,
                      LinkedHashMap::new));
      this.transferTargets =
          transferTargets.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      entry -> Collections.unmodifiableList(new ArrayList<>(entry.getValue())),
                      (left, right) -> left,
                      LinkedHashMap::new));
    }

    @Override
    public long getPartitionId() {
      return partitionId;
    }

    @Override
    public boolean isRootHashMatched() {
      return false;
    }

    @Override
    public List<MerkleFileContent> getLeaderMerkleFiles() {
      return leaderMerkleFiles;
    }

    @Override
    public List<MerkleFileContent> getMismatchedLeaderMerkleFiles() {
      return transferTargets.keySet().stream()
          .map(leaderMerkleFileByPath::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }

    @Override
    public RowRefIndex getRowRefIndex() {
      return null;
    }

    @Override
    public List<DiffEntry> decodeDiffs() {
      return Collections.emptyList();
    }

    @Override
    public boolean isDiffDecodeSuccessful() {
      return false;
    }

    @Override
    public long estimateDiffCount() {
      return 0L;
    }

    @Override
    public List<String> getFallbackTsFiles() {
      return new ArrayList<>(transferTargets.keySet());
    }

    @Override
    public long getTsFileSize(String tsFilePath) {
      Long tsFileSize = leaderTsFileSizeByPath.get(tsFilePath);
      if (tsFileSize == null) {
        throw new IllegalArgumentException("Unknown leader TsFile " + tsFilePath);
      }
      return tsFileSize;
    }

    @Override
    public int getTotalPointCount(String tsFilePath) {
      return 0;
    }

    @Override
    public boolean shouldForceDirectTsFileTransfer() {
      return true;
    }

    @Override
    public boolean verify(
        Map<String, org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairPlan>
            repairPlans,
        boolean rootHashMatched) {
      return verifyPartition(partitionId);
    }
  }

  private static final class SnapshotFile {
    private final String sourceTsFilePath;
    private final long tsFileSize;
    private final DigestKey digestKey;

    private SnapshotFile(String sourceTsFilePath, long tsFileSize, DigestKey digestKey) {
      this.sourceTsFilePath = sourceTsFilePath;
      this.tsFileSize = tsFileSize;
      this.digestKey = digestKey;
    }

    private String getSourceTsFilePath() {
      return sourceTsFilePath;
    }

    private long getTsFileSize() {
      return tsFileSize;
    }

    private DigestKey getDigestKey() {
      return digestKey;
    }
  }

  private static final class DigestKey {
    private final long fileXorHash;
    private final long fileAddHash;

    private DigestKey(long fileXorHash, long fileAddHash) {
      this.fileXorHash = fileXorHash;
      this.fileAddHash = fileAddHash;
    }

    private long getFileXorHash() {
      return fileXorHash;
    }

    private long getFileAddHash() {
      return fileAddHash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DigestKey)) {
        return false;
      }
      DigestKey digestKey = (DigestKey) o;
      return fileXorHash == digestKey.fileXorHash && fileAddHash == digestKey.fileAddHash;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileXorHash, fileAddHash);
    }

    @Override
    public String toString() {
      return "DigestKey{" + "xor=" + fileXorHash + ", add=" + fileAddHash + '}';
    }
  }
}
