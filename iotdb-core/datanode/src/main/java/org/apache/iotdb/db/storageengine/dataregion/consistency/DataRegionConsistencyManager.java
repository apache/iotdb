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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.iotv2.consistency.DualDigest;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.control.QueryResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.mpp.rpc.thrift.TConsistencyDeletionSummary;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DataNode-side logical snapshot manager. The snapshot is built from the merged logical visible
 * view instead of TsFile correspondence, so physical file layout heterogeneity does not affect
 * consistency results.
 */
public class DataRegionConsistencyManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataRegionConsistencyManager.class);
  private static final DataRegionConsistencyManager INSTANCE = new DataRegionConsistencyManager();

  private static final int DEVICE_SHARD_COUNT = 256;
  private static final long LEAF_TIME_BUCKET_MS = 3_600_000L;
  private static final String TREE_KIND_LIVE = "LIVE";
  private static final String TREE_KIND_TOMBSTONE = "TOMBSTONE";
  private static final String BATCH_KIND_RESET_LEAF = "RESET_LEAF";
  private static final String BATCH_KIND_RESET_SCOPE = "RESET_SCOPE";
  private static final String BATCH_KIND_INSERT_ROWS = "INSERT_ROWS";
  private static final String BATCH_KIND_DELETE_DATA = "DELETE_DATA";
  private static final int SNAPSHOT_REBUILD_MAX_ATTEMPTS = 2;

  private final ConcurrentHashMap<String, RegionState> regionStates = new ConcurrentHashMap<>();
  private final ThreadLocal<RepairMutationContext> repairMutationContext = new ThreadLocal<>();
  private final LogicalConsistencyPartitionStateStore partitionStateStore;

  DataRegionConsistencyManager() {
    this(new LogicalConsistencyPartitionStateStore());
  }

  DataRegionConsistencyManager(LogicalConsistencyPartitionStateStore partitionStateStore) {
    this.partitionStateStore = partitionStateStore;
  }

  public static DataRegionConsistencyManager getInstance() {
    return INSTANCE;
  }

  public PartitionInspection inspectPartition(
      TConsensusGroupId consensusGroupId,
      DataRegion dataRegion,
      long partitionId,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries) {
    String consensusGroupKey = consensusGroupId.toString();
    RegionState regionState = getOrCreateRegionState(consensusGroupId);
    PartitionState partitionState =
        regionState.partitions.computeIfAbsent(partitionId, PartitionState::new);
    PartitionInspection inspection;
    synchronized (partitionState) {
      if (partitionState.snapshotState != RepairProgressTable.SnapshotState.READY
          || partitionState.snapshotEpoch != partitionState.partitionMutationEpoch) {
        try {
          rebuildSnapshot(partitionState, dataRegion, partitionId, partitionDeletionSummaries);
        } catch (Exception e) {
          partitionState.snapshotState = RepairProgressTable.SnapshotState.FAILED;
          partitionState.lastError = e.getMessage();
          LOGGER.warn(
              "Failed to build logical consistency snapshot for region {} partition {}",
              consensusGroupId,
              partitionId,
              e);
        }
      }
      inspection = partitionState.toInspection(partitionId);
    }
    persistKnownPartitionsIfNeeded(consensusGroupKey, regionState);
    return inspection;
  }

  public List<Long> getKnownPartitions(TConsensusGroupId consensusGroupId) {
    List<Long> partitions =
        new ArrayList<>(getOrCreateRegionState(consensusGroupId).partitions.keySet());
    partitions.sort(Long::compareTo);
    return partitions;
  }

  public SnapshotSubtreeResult getSnapshotSubtree(
      TConsensusGroupId consensusGroupId,
      DataRegion dataRegion,
      long partitionId,
      long snapshotEpoch,
      String treeKind,
      List<String> nodeHandles,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries) {
    PartitionState partitionState =
        getOrCreateRegionState(consensusGroupId)
            .partitions
            .computeIfAbsent(partitionId, PartitionState::new);
    synchronized (partitionState) {
      if (!ensureReadySnapshot(
          partitionState, dataRegion, partitionId, snapshotEpoch, partitionDeletionSummaries)) {
        return SnapshotSubtreeResult.stale(snapshotEpoch);
      }
      SnapshotTree tree = partitionState.getTree(treeKind);
      List<SnapshotNode> resultNodes = new ArrayList<>();
      List<String> requestedHandles =
          nodeHandles == null || nodeHandles.isEmpty()
              ? Collections.singletonList(SnapshotTree.ROOT_HANDLE)
              : nodeHandles;
      for (String handle : requestedHandles) {
        SnapshotNode node = tree.nodesByHandle.get(handle);
        if (node == null) {
          continue;
        }
        if (node.leaf) {
          resultNodes.add(node);
          continue;
        }
        for (String childHandle : node.childrenHandles) {
          SnapshotNode child = tree.nodesByHandle.get(childHandle);
          if (child != null) {
            resultNodes.add(child);
          }
        }
      }
      resultNodes.sort(Comparator.comparing(SnapshotNode::getNodeHandle));
      return SnapshotSubtreeResult.ready(snapshotEpoch, resultNodes);
    }
  }

  public LeafEstimate estimateLeaf(
      TConsensusGroupId consensusGroupId,
      DataRegion dataRegion,
      long partitionId,
      long snapshotEpoch,
      String treeKind,
      String leafId,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries) {
    PartitionState partitionState =
        getOrCreateRegionState(consensusGroupId)
            .partitions
            .computeIfAbsent(partitionId, PartitionState::new);
    synchronized (partitionState) {
      if (!ensureReadySnapshot(
          partitionState, dataRegion, partitionId, snapshotEpoch, partitionDeletionSummaries)) {
        return null;
      }
      SnapshotNode leaf = partitionState.getTree(treeKind).nodesByHandle.get(leafId);
      if (leaf == null || !leaf.leaf) {
        return null;
      }
      return new LeafEstimate(
          partitionId,
          snapshotEpoch,
          treeKind,
          leafId,
          leaf.itemCount,
          TREE_KIND_TOMBSTONE.equalsIgnoreCase(treeKind) ? leaf.itemCount : 0L,
          leaf.itemCount,
          leaf.keyRangeStart,
          leaf.keyRangeEnd);
    }
  }

  public List<LeafDiffEntry> decodeLeaf(
      TConsensusGroupId consensusGroupId,
      DataRegion dataRegion,
      long partitionId,
      long snapshotEpoch,
      String treeKind,
      String leafId,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries)
      throws Exception {
    PartitionState partitionState =
        getOrCreateRegionState(consensusGroupId)
            .partitions
            .computeIfAbsent(partitionId, PartitionState::new);
    synchronized (partitionState) {
      if (!ensureReadySnapshot(
          partitionState, dataRegion, partitionId, snapshotEpoch, partitionDeletionSummaries)) {
        return null;
      }
    }

    LogicalLeafSelector selector = LogicalLeafSelector.parse(leafId);
    List<LeafDiffEntry> entries = new ArrayList<>();
    if (TREE_KIND_TOMBSTONE.equalsIgnoreCase(treeKind)) {
      for (TConsistencyDeletionSummary summary : partitionDeletionSummaries) {
        if (selector.matches(summary)) {
          entries.add(new LeafDiffEntry(encodeDeletionKey(summary), "LOCAL"));
        }
      }
    } else {
      scanLiveCells(
          dataRegion,
          partitionId,
          (deviceId, measurement, type, time, value, aligned) -> {
            if (selector.matches(deviceId, time)) {
              entries.add(
                  new LeafDiffEntry(
                      encodeLogicalCell(deviceId, measurement, time, type, value), "LOCAL"));
            }
          });
    }
    entries.sort(Comparator.comparing(LeafDiffEntry::getLogicalKey));
    return entries;
  }

  public List<LogicalRepairBatch> streamLogicalRepair(
      TConsensusGroupId consensusGroupId,
      DataRegion dataRegion,
      long partitionId,
      String repairEpoch,
      List<LeafSelector> leafSelectors,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries)
      throws Exception {
    PartitionState partitionState =
        getOrCreateRegionState(consensusGroupId)
            .partitions
            .computeIfAbsent(partitionId, PartitionState::new);
    synchronized (partitionState) {
      ensureRepairEpochReadySnapshot(
          partitionState, dataRegion, partitionId, repairEpoch, partitionDeletionSummaries);
    }

    String sessionId = buildRepairSessionId(repairEpoch, leafSelectors);
    List<LogicalRepairBatch> batches = new ArrayList<>();
    final int[] seqNo = {0};
    for (LeafSelector selector : leafSelectors) {
      if (TREE_KIND_TOMBSTONE.equalsIgnoreCase(selector.treeKind)) {
        for (TConsistencyDeletionSummary summary : partitionDeletionSummaries) {
          if (!selector.selector.matches(summary)) {
            continue;
          }
          DeleteDataNode deleteDataNode = buildDeleteDataNode(summary);
          batches.add(
              new LogicalRepairBatch(
                  sessionId,
                  selector.treeKind,
                  selector.selector.leafId,
                  seqNo[0]++,
                  BATCH_KIND_DELETE_DATA,
                  deleteDataNode.serializeToByteBuffer()));
        }
        continue;
      }

      batches.add(
          new LogicalRepairBatch(
              sessionId,
              selector.treeKind,
              selector.selector.leafId,
              seqNo[0]++,
              selector.selector.requiresScopedReset()
                  ? BATCH_KIND_RESET_SCOPE
                  : BATCH_KIND_RESET_LEAF,
              selector.selector.requiresScopedReset()
                  ? selector.selector.serialize()
                  : ByteBuffer.allocate(0)));

      List<InsertRowNode> bufferedRows = new ArrayList<>();
      scanLiveCells(
          dataRegion,
          partitionId,
          (deviceId, measurement, type, time, value, aligned) -> {
            if (!selector.selector.matchesLiveCell(deviceId, measurement, type, time, value)) {
              return;
            }
            bufferedRows.add(buildInsertRow(deviceId, measurement, type, time, value, aligned));
            if (bufferedRows.size() >= 256) {
              batches.add(
                  new LogicalRepairBatch(
                      sessionId,
                      selector.treeKind,
                      selector.selector.leafId,
                      seqNo[0]++,
                      BATCH_KIND_INSERT_ROWS,
                      toInsertRowsNode(bufferedRows).serializeToByteBuffer()));
              bufferedRows.clear();
            }
          });
      if (!bufferedRows.isEmpty()) {
        batches.add(
            new LogicalRepairBatch(
                sessionId,
                selector.treeKind,
                selector.selector.leafId,
                seqNo[0]++,
                BATCH_KIND_INSERT_ROWS,
                toInsertRowsNode(bufferedRows).serializeToByteBuffer()));
      }
    }
    return batches;
  }

  public <T> T runWithLogicalRepairMutation(
      TConsensusGroupId consensusGroupId, long partitionId, String repairEpoch, Callable<T> action)
      throws Exception {
    RepairMutationContext previousContext = repairMutationContext.get();
    repairMutationContext.set(
        new RepairMutationContext(consensusGroupId.toString(), partitionId, repairEpoch));
    try {
      return action.call();
    } finally {
      if (previousContext == null) {
        repairMutationContext.remove();
      } else {
        repairMutationContext.set(previousContext);
      }
    }
  }

  public void resetLiveLeaf(DataRegion dataRegion, long partitionId, String leafId)
      throws Exception {
    LogicalLeafSelector selector = LogicalLeafSelector.parse(leafId);
    resetLiveBySelector(dataRegion, partitionId, selector);
  }

  public void resetLiveScope(DataRegion dataRegion, long partitionId, ByteBuffer selectorPayload)
      throws Exception {
    LogicalLeafSelector selector = LogicalLeafSelector.deserialize(selectorPayload);
    resetLiveBySelector(dataRegion, partitionId, selector);
  }

  private void resetLiveBySelector(
      DataRegion dataRegion, long partitionId, LogicalLeafSelector selector) throws Exception {
    if (selector.requiresScopedReset()) {
      resetLiveScopedCells(dataRegion, partitionId, selector);
      return;
    }
    Set<String> fullPaths = new LinkedHashSet<>();
    scanLiveCells(
        dataRegion,
        partitionId,
        (deviceId, measurement, type, time, value, aligned) -> {
          if (selector.matches(deviceId, time)) {
            fullPaths.add(deviceId + "." + measurement);
          }
        });
    if (fullPaths.isEmpty()) {
      return;
    }
    List<MeasurementPath> paths = new ArrayList<>(fullPaths.size());
    for (String fullPath : fullPaths) {
      paths.add(new MeasurementPath(fullPath));
    }
    long bucketStart = selector.bucket * LEAF_TIME_BUCKET_MS;
    long bucketEnd = bucketStart + LEAF_TIME_BUCKET_MS - 1;
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("logical-reset-leaf"), paths, bucketStart, bucketEnd);
    for (MeasurementPath path : paths) {
      dataRegion.deleteByDevice(path, deleteDataNode);
    }
  }

  private void resetLiveScopedCells(
      DataRegion dataRegion, long partitionId, LogicalLeafSelector selector) throws Exception {
    Map<String, Set<Long>> timesByPath = new HashMap<>();
    scanLiveCells(
        dataRegion,
        partitionId,
        (deviceId, measurement, type, time, value, aligned) -> {
          if (!selector.matchesLiveCell(deviceId, measurement, type, time, value)) {
            return;
          }
          timesByPath
              .computeIfAbsent(deviceId + "." + measurement, ignored -> new LinkedHashSet<>())
              .add(time);
        });
    for (Map.Entry<String, Set<Long>> entry : timesByPath.entrySet()) {
      MeasurementPath path = new MeasurementPath(entry.getKey());
      for (Long time : entry.getValue()) {
        DeleteDataNode deleteDataNode =
            new DeleteDataNode(
                new PlanNodeId("logical-reset-scope"), Collections.singletonList(path), time, time);
        dataRegion.deleteByDevice(path, deleteDataNode);
      }
    }
  }

  public void onTsFileClosed(TConsensusGroupId consensusGroupId, TsFileResource tsFileResource) {
    // Logical snapshots are driven by mutation epoch, not close-file events.
  }

  public void onCompaction(
      TConsensusGroupId consensusGroupId,
      List<TsFileResource> seqSourceFiles,
      List<TsFileResource> unseqSourceFiles,
      List<TsFileResource> targetFiles,
      long timePartition) {
    // Compaction only changes physical layout and must not dirty logical snapshots.
  }

  public void onDeletion(TConsensusGroupId consensusGroupId, List<TsFileResource> affectedTsFiles) {
    if (affectedTsFiles == null) {
      return;
    }
    for (TsFileResource resource : affectedTsFiles) {
      onPartitionMutation(consensusGroupId, resource.getTimePartition());
    }
  }

  public void onDeletion(TConsensusGroupId consensusGroupId, long startTime, long endTime) {
    long partitionInterval =
        org.apache.iotdb.commons.conf.CommonDescriptor.getInstance()
            .getConfig()
            .getTimePartitionInterval();
    if (partitionInterval <= 0) {
      onPartitionMutation(consensusGroupId, 0L);
      return;
    }
    long startPartition = Math.floorDiv(startTime, partitionInterval);
    long endPartition = Math.floorDiv(endTime, partitionInterval);
    for (long partitionId = startPartition; partitionId <= endPartition; partitionId++) {
      onPartitionMutation(consensusGroupId, partitionId);
    }
  }

  public void onPartitionMutation(TConsensusGroupId consensusGroupId, long partitionId) {
    String consensusGroupKey = consensusGroupId.toString();
    RegionState regionState = getOrCreateRegionState(consensusGroupId);
    PartitionState newState = new PartitionState(partitionId);
    PartitionState existingState = regionState.partitions.putIfAbsent(partitionId, newState);
    PartitionState state = existingState == null ? newState : existingState;
    synchronized (state) {
      if (isRepairMutation(consensusGroupId, partitionId)) {
        state.snapshotState = RepairProgressTable.SnapshotState.DIRTY;
        state.liveTree = SnapshotTree.empty();
        state.tombstoneTree = SnapshotTree.empty();
        state.lastError = null;
        return;
      }
      state.partitionMutationEpoch++;
      state.snapshotState = RepairProgressTable.SnapshotState.DIRTY;
      state.liveTree = SnapshotTree.empty();
      state.tombstoneTree = SnapshotTree.empty();
      state.lastError = null;
    }
    if (existingState == null) {
      persistKnownPartitionsIfNeeded(consensusGroupKey, regionState);
    }
  }

  private RegionState getOrCreateRegionState(TConsensusGroupId consensusGroupId) {
    return regionStates.computeIfAbsent(
        consensusGroupId.toString(), ignored -> loadRegionState(consensusGroupId.toString()));
  }

  private RegionState loadRegionState(String consensusGroupKey) {
    RegionState regionState = new RegionState();
    try {
      Map<Long, Long> persistedMutationEpochs = partitionStateStore.load(consensusGroupKey);
      for (Map.Entry<Long, Long> entry : persistedMutationEpochs.entrySet()) {
        PartitionState partitionState = new PartitionState(entry.getKey());
        partitionState.partitionMutationEpoch = entry.getValue();
        regionState.partitions.put(entry.getKey(), partitionState);
      }
      regionState.lastPersistedMutationEpochs = new TreeMap<>(persistedMutationEpochs);
    } catch (IOException e) {
      LOGGER.warn(
          "Failed to restore logical consistency partition state for region {}",
          consensusGroupKey,
          e);
    }
    return regionState;
  }

  private void persistKnownPartitionsIfNeeded(String consensusGroupKey, RegionState regionState) {
    Map<Long, Long> snapshotToPersist = snapshotPartitionMutationEpochs(regionState);
    synchronized (regionState.persistMonitor) {
      if (snapshotToPersist.equals(regionState.lastPersistedMutationEpochs)
          || snapshotToPersist.equals(regionState.pendingPersistMutationEpochs)) {
        return;
      }
      if (regionState.persistInFlight) {
        regionState.pendingPersistMutationEpochs = snapshotToPersist;
        return;
      }
      regionState.persistInFlight = true;
    }

    while (true) {
      boolean persistSucceeded = false;
      try {
        partitionStateStore.persist(consensusGroupKey, snapshotToPersist);
        persistSucceeded = true;
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to persist logical consistency partition state for region {}",
            consensusGroupKey,
            e);
      }

      synchronized (regionState.persistMonitor) {
        if (persistSucceeded) {
          regionState.lastPersistedMutationEpochs = snapshotToPersist;
        }
        if (regionState.pendingPersistMutationEpochs == null
            || regionState.pendingPersistMutationEpochs.equals(
                regionState.lastPersistedMutationEpochs)
            || (!persistSucceeded
                && regionState.pendingPersistMutationEpochs.equals(snapshotToPersist))) {
          regionState.pendingPersistMutationEpochs = null;
          regionState.persistInFlight = false;
          return;
        }
        snapshotToPersist = regionState.pendingPersistMutationEpochs;
        regionState.pendingPersistMutationEpochs = null;
      }
    }
  }

  private Map<Long, Long> snapshotPartitionMutationEpochs(RegionState regionState) {
    Map<Long, Long> mutationEpochs = new TreeMap<>();
    for (Map.Entry<Long, PartitionState> entry : regionState.partitions.entrySet()) {
      synchronized (entry.getValue()) {
        mutationEpochs.put(entry.getKey(), entry.getValue().partitionMutationEpoch);
      }
    }
    return mutationEpochs;
  }

  private boolean ensureReadySnapshot(
      PartitionState partitionState,
      DataRegion dataRegion,
      long partitionId,
      long snapshotEpoch,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries) {
    if (partitionState.snapshotEpoch == snapshotEpoch
        && partitionState.snapshotState == RepairProgressTable.SnapshotState.READY) {
      return true;
    }
    if (partitionState.partitionMutationEpoch != snapshotEpoch) {
      return false;
    }
    try {
      rebuildSnapshot(partitionState, dataRegion, partitionId, partitionDeletionSummaries);
      return partitionState.snapshotEpoch == snapshotEpoch
          && partitionState.snapshotState == RepairProgressTable.SnapshotState.READY;
    } catch (Exception e) {
      partitionState.snapshotState = RepairProgressTable.SnapshotState.FAILED;
      partitionState.lastError = e.getMessage();
      return false;
    }
  }

  private void rebuildSnapshot(
      PartitionState partitionState,
      DataRegion dataRegion,
      long partitionId,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries)
      throws Exception {
    long expectedMutationEpoch = partitionState.partitionMutationEpoch;
    partitionState.snapshotState = RepairProgressTable.SnapshotState.BUILDING;

    for (int attempt = 1; attempt <= SNAPSHOT_REBUILD_MAX_ATTEMPTS; attempt++) {
      SnapshotTree liveTree = SnapshotTree.empty();
      SnapshotTree tombstoneTree = SnapshotTree.empty();

      try {
        scanLiveCells(
            dataRegion,
            partitionId,
            (deviceId, measurement, type, time, value, aligned) -> {
              int shard = computeDeviceShard(deviceId);
              long bucket = Math.floorDiv(time, LEAF_TIME_BUCKET_MS);
              String leafId = LogicalLeafSelector.leafId(shard, bucket);
              String logicalKey = encodeLogicalCell(deviceId, measurement, time, type, value);
              long hash = hashLogicalCell(deviceId, measurement, time, type, value);
              liveTree.addLeafEntry(leafId, shard, logicalKey, hash, 1L);
            });
      } catch (Exception e) {
        if (attempt >= SNAPSHOT_REBUILD_MAX_ATTEMPTS || !isRetryableSnapshotReadFailure(e)) {
          throw e;
        }
        invalidateSnapshotReaders(dataRegion, partitionId);
        LOGGER.info(
            "Retrying logical consistency snapshot build for partition {} after refreshing stale readers",
            partitionId);
        continue;
      }

      for (TConsistencyDeletionSummary summary : partitionDeletionSummaries) {
        int shard = computeDeviceShard(summary.getPathPattern());
        long startBucket = Math.floorDiv(summary.getTimeRangeStart(), LEAF_TIME_BUCKET_MS);
        long endBucket = Math.floorDiv(summary.getTimeRangeEnd(), LEAF_TIME_BUCKET_MS);
        String deletionKey = encodeDeletionKey(summary);
        for (long bucket = startBucket; bucket <= endBucket; bucket++) {
          String leafId = LogicalLeafSelector.leafId(shard, bucket);
          tombstoneTree.addLeafEntry(leafId, shard, deletionKey, hashDeletion(summary), 1L);
        }
      }

      liveTree.finalizeTree();
      tombstoneTree.finalizeTree();

      if (expectedMutationEpoch != partitionState.partitionMutationEpoch) {
        partitionState.snapshotState = RepairProgressTable.SnapshotState.DIRTY;
        return;
      }
      partitionState.snapshotEpoch = expectedMutationEpoch;
      partitionState.snapshotState = RepairProgressTable.SnapshotState.READY;
      partitionState.liveTree = liveTree;
      partitionState.tombstoneTree = tombstoneTree;
      partitionState.lastError = null;
      return;
    }

    throw new IllegalStateException(
        "Logical consistency snapshot rebuild finished without a terminal result");
  }

  private void ensureRepairEpochReadySnapshot(
      PartitionState partitionState,
      DataRegion dataRegion,
      long partitionId,
      String repairEpoch,
      List<TConsistencyDeletionSummary> partitionDeletionSummaries)
      throws Exception {
    RepairEpochRef repairEpochRef = RepairEpochRef.parse(repairEpoch);
    if (repairEpochRef.partitionId != partitionId) {
      throw new IllegalStateException(
          "Repair epoch partition "
              + repairEpochRef.partitionId
              + " does not match requested partition "
              + partitionId);
    }
    if (partitionState.partitionMutationEpoch != repairEpochRef.partitionMutationEpoch) {
      throw new IllegalStateException(
          "Repair epoch drift detected for partition "
              + partitionId
              + ": expected mutation epoch "
              + repairEpochRef.partitionMutationEpoch
              + ", actual "
              + partitionState.partitionMutationEpoch);
    }
    if (partitionState.snapshotState != RepairProgressTable.SnapshotState.READY
        || partitionState.snapshotEpoch != repairEpochRef.snapshotEpoch) {
      rebuildSnapshot(partitionState, dataRegion, partitionId, partitionDeletionSummaries);
    }
    if (partitionState.snapshotState != RepairProgressTable.SnapshotState.READY
        || partitionState.snapshotEpoch != repairEpochRef.snapshotEpoch
        || partitionState.partitionMutationEpoch != repairEpochRef.partitionMutationEpoch) {
      throw new IllegalStateException(
          "Repair epoch drift detected after snapshot rebuild for partition " + partitionId);
    }
  }

  private boolean isRepairMutation(TConsensusGroupId consensusGroupId, long partitionId) {
    RepairMutationContext mutationContext = repairMutationContext.get();
    return mutationContext != null
        && mutationContext.partitionId == partitionId
        && mutationContext.consensusGroupKey.equals(consensusGroupId.toString());
  }

  private void scanLiveCells(DataRegion dataRegion, long partitionId, LiveCellConsumer consumer)
      throws Exception {
    Map<String, DeviceSeriesContext> deviceSeriesContexts =
        collectLogicalSeriesContexts(dataRegion, partitionId);
    if (deviceSeriesContexts.isEmpty()) {
      return;
    }

    long queryId = QueryResourceManager.getInstance().assignInternalQueryId();
    FragmentInstanceContext context =
        FragmentInstanceContext.createFragmentInstanceContextForCompaction(queryId);

    try {
      for (DeviceSeriesContext deviceSeriesContext : deviceSeriesContexts.values()) {
        List<IFullPath> paths = deviceSeriesContext.buildPaths();
        if (paths.isEmpty()) {
          continue;
        }

        QueryDataSource dataSource =
            dataRegion.query(
                paths,
                deviceSeriesContext.deviceId,
                context,
                null,
                Collections.singletonList(partitionId));
        if (dataSource == null || dataSource.isEmpty()) {
          continue;
        }

        QueryResourceManager.getInstance()
            .getQueryFileManager()
            .addUsedFilesForQuery(queryId, dataSource);
        dataSource.fillOrderIndexes(deviceSeriesContext.deviceId, true);
        String deviceId = String.valueOf(deviceSeriesContext.deviceId);
        if (deviceSeriesContext.aligned) {
          QueryDataSource deviceDataSource = copyQueryDataSourceForDevice(dataSource);
          IDataBlockReader reader =
              ReadPointCompactionPerformer.constructReader(
                  deviceSeriesContext.deviceId,
                  deviceSeriesContext.getMeasurementNames(),
                  deviceSeriesContext.getMeasurementSchemas(),
                  deviceSeriesContext.getMeasurementNames(),
                  context,
                  deviceDataSource,
                  true);
          consumeAligned(
              reader,
              deviceId,
              deviceSeriesContext.getMeasurementNames(),
              deviceSeriesContext.measurementSchemas,
              consumer);
        } else {
          for (String measurementName : deviceSeriesContext.getMeasurementNames()) {
            IMeasurementSchema schema = deviceSeriesContext.measurementSchemas.get(measurementName);
            QueryDataSource deviceDataSource = copyQueryDataSourceForDevice(dataSource);
            IDataBlockReader reader =
                ReadPointCompactionPerformer.constructReader(
                    deviceSeriesContext.deviceId,
                    Collections.singletonList(measurementName),
                    Collections.singletonList(schema),
                    deviceSeriesContext.getMeasurementNames(),
                    context,
                    deviceDataSource,
                    false);
            consumeNonAligned(reader, deviceId, measurementName, schema, consumer);
          }
        }
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
    }
  }

  private void consumeAligned(
      IDataBlockReader reader,
      String deviceId,
      List<String> measurementNames,
      Map<String, IMeasurementSchema> schemaMap,
      LiveCellConsumer consumer)
      throws Exception {
    while (reader.hasNextBatch()) {
      TsBlock tsBlock = reader.nextBatch();
      for (int row = 0; row < tsBlock.getPositionCount(); row++) {
        long time = tsBlock.getTimeByIndex(row);
        for (int columnIndex = 0; columnIndex < tsBlock.getValueColumnCount(); columnIndex++) {
          Column column = tsBlock.getColumn(columnIndex);
          if (column.isNull(row)) {
            continue;
          }
          String measurement = measurementNames.get(columnIndex);
          IMeasurementSchema schema = schemaMap.get(measurement);
          consumer.accept(
              deviceId,
              measurement,
              schema.getType(),
              time,
              extractValue(column, row, schema.getType()),
              true);
        }
      }
    }
  }

  private void consumeNonAligned(
      IDataBlockReader reader,
      String deviceId,
      String measurement,
      IMeasurementSchema schema,
      LiveCellConsumer consumer)
      throws Exception {
    while (reader.hasNextBatch()) {
      TsBlock tsBlock = reader.nextBatch();
      Column column = tsBlock.getColumn(0);
      for (int row = 0; row < tsBlock.getPositionCount(); row++) {
        if (column.isNull(row)) {
          continue;
        }
        consumer.accept(
            deviceId,
            measurement,
            schema.getType(),
            tsBlock.getTimeByIndex(row),
            extractValue(column, row, schema.getType()),
            false);
      }
    }
  }

  private Object extractValue(Column column, int rowIndex, TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return column.getBoolean(rowIndex);
      case INT32:
      case DATE:
        return column.getInt(rowIndex);
      case INT64:
      case TIMESTAMP:
        return column.getLong(rowIndex);
      case FLOAT:
        return column.getFloat(rowIndex);
      case DOUBLE:
        return column.getDouble(rowIndex);
      case TEXT:
      case STRING:
      case BLOB:
        return column.getBinary(rowIndex);
      default:
        return String.valueOf(column.getObject(rowIndex));
    }
  }

  private Map<String, DeviceSeriesContext> collectLogicalSeriesContexts(
      DataRegion dataRegion, long partitionId) throws Exception {
    Map<String, DeviceSeriesContext> deviceSeriesContexts = new TreeMap<>();

    List<TsFileResource> seqResources =
        new ArrayList<>(dataRegion.getTsFileManager().getTsFileListSnapshot(partitionId, true));
    List<TsFileResource> unseqResources =
        new ArrayList<>(dataRegion.getTsFileManager().getTsFileListSnapshot(partitionId, false));
    pruneClosedResources(seqResources);
    pruneClosedResources(unseqResources);

    if (!seqResources.isEmpty() || !unseqResources.isEmpty()) {
      // Use dedicated metadata readers here instead of the shared FileReaderManager-backed read
      // point iterator. The snapshot rebuild later performs a real logical scan through the query
      // engine, so closing the schema-discovery iterator must never poison the shared reader cache.
      try (MultiTsFileDeviceIterator deviceIterator =
          new MultiTsFileDeviceIterator(seqResources, unseqResources, new HashMap<>())) {
        while (deviceIterator.hasNextDevice()) {
          org.apache.tsfile.utils.Pair<IDeviceID, Boolean> deviceInfo = deviceIterator.nextDevice();
          DeviceSeriesContext deviceSeriesContext =
              deviceSeriesContexts.computeIfAbsent(
                  String.valueOf(deviceInfo.left),
                  ignored -> new DeviceSeriesContext(deviceInfo.left, deviceInfo.right));
          deviceSeriesContext.mergeAlignment(deviceInfo.right);

          Map<String, MeasurementSchema> rawSchemaMap =
              deviceIterator.getAllSchemasOfCurrentDevice();
          rawSchemaMap.remove("time");
          for (MeasurementSchema schema : rawSchemaMap.values()) {
            deviceSeriesContext.mergeMeasurement(schema);
          }
        }
      }
    }

    collectOpenProcessorSeriesContexts(
        dataRegion.getWorkSequenceTsFileProcessors(), partitionId, deviceSeriesContexts);
    collectOpenProcessorSeriesContexts(
        dataRegion.getWorkUnsequenceTsFileProcessors(), partitionId, deviceSeriesContexts);
    return deviceSeriesContexts;
  }

  private void collectOpenProcessorSeriesContexts(
      Iterable<TsFileProcessor> processors,
      long partitionId,
      Map<String, DeviceSeriesContext> deviceSeriesContexts)
      throws IOException, InterruptedException {
    for (TsFileProcessor processor : processors) {
      if (processor == null || processor.getTimeRangeId() != partitionId) {
        continue;
      }

      if (!processor.tryReadLock(1_000L)) {
        throw new IOException(
            "Failed to acquire logical snapshot read lock for time partition " + partitionId);
      }
      try {
        collectMemTableSeriesContexts(processor.getWorkMemTable(), deviceSeriesContexts);
        for (IMemTable flushingMemTable : processor.getFlushingMemTable()) {
          collectMemTableSeriesContexts(flushingMemTable, deviceSeriesContexts);
        }
      } finally {
        processor.readUnLock();
      }
    }
  }

  private void collectMemTableSeriesContexts(
      IMemTable memTable, Map<String, DeviceSeriesContext> deviceSeriesContexts) {
    if (memTable == null || memTable.getMemTableMap().isEmpty()) {
      return;
    }

    for (Map.Entry<IDeviceID, IWritableMemChunkGroup> entry :
        memTable.getMemTableMap().entrySet()) {
      IWritableMemChunkGroup memChunkGroup = entry.getValue();
      if (memChunkGroup == null || memChunkGroup.isEmpty()) {
        continue;
      }

      boolean aligned = memChunkGroup instanceof AlignedWritableMemChunkGroup;
      DeviceSeriesContext deviceSeriesContext =
          deviceSeriesContexts.computeIfAbsent(
              String.valueOf(entry.getKey()),
              ignored -> new DeviceSeriesContext(entry.getKey(), aligned));
      deviceSeriesContext.mergeAlignment(aligned);

      if (aligned) {
        for (IMeasurementSchema schema :
            ((AlignedWritableMemChunkGroup) memChunkGroup).getAlignedMemChunk().getSchemaList()) {
          deviceSeriesContext.mergeMeasurement(schema);
        }
        continue;
      }

      for (IWritableMemChunk memChunk : memChunkGroup.getMemChunkMap().values()) {
        if (memChunk == null) {
          continue;
        }
        deviceSeriesContext.mergeMeasurement(memChunk.getSchema());
      }
    }
  }

  private void pruneClosedResources(List<TsFileResource> resources) {
    resources.removeIf(
        resource -> !resource.isClosed() || resource.isDeleted() || !resource.getTsFile().exists());
    resources.sort(TsFileResource::compareFileName);
  }

  private QueryDataSource copyQueryDataSourceForDevice(QueryDataSource dataSource) {
    QueryDataSource deviceDataSource = new QueryDataSource(dataSource);
    deviceDataSource.setSingleDevice(true);
    return deviceDataSource;
  }

  private boolean isRetryableSnapshotReadFailure(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof ClosedChannelException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void invalidateSnapshotReaders(DataRegion dataRegion, long partitionId) {
    DataRegion.operateClearCache();
    closeClosedSnapshotReaders(
        dataRegion.getTsFileManager().getTsFileListSnapshot(partitionId, true));
    closeClosedSnapshotReaders(
        dataRegion.getTsFileManager().getTsFileListSnapshot(partitionId, false));
  }

  private void closeClosedSnapshotReaders(List<TsFileResource> resources) {
    if (resources == null) {
      return;
    }
    for (TsFileResource resource : resources) {
      if (resource == null || !resource.isClosed()) {
        continue;
      }
      try {
        FileReaderManager.getInstance().closeFileAndRemoveReader(resource.getTsFileID());
      } catch (IOException e) {
        LOGGER.debug(
            "Failed to invalidate cached reader for logical snapshot file {}",
            resource.getTsFilePath(),
            e);
      }
    }
  }

  private static final class DeviceSeriesContext {
    private final IDeviceID deviceId;
    private boolean aligned;
    private final Map<String, IMeasurementSchema> measurementSchemas = new TreeMap<>();

    private DeviceSeriesContext(IDeviceID deviceId, boolean aligned) {
      this.deviceId = deviceId;
      this.aligned = aligned;
    }

    private void mergeAlignment(boolean newAligned) {
      aligned = aligned || newAligned;
    }

    private void mergeMeasurement(IMeasurementSchema schema) {
      if (schema == null
          || schema.getMeasurementName() == null
          || schema.getMeasurementName().isEmpty()) {
        return;
      }
      measurementSchemas.putIfAbsent(schema.getMeasurementName(), schema);
    }

    private List<String> getMeasurementNames() {
      return new ArrayList<>(measurementSchemas.keySet());
    }

    private List<IMeasurementSchema> getMeasurementSchemas() {
      return new ArrayList<>(measurementSchemas.values());
    }

    private List<IFullPath> buildPaths() {
      if (measurementSchemas.isEmpty()) {
        return Collections.emptyList();
      }
      if (aligned) {
        return Collections.singletonList(
            new AlignedFullPath(deviceId, getMeasurementNames(), getMeasurementSchemas()));
      }

      List<IFullPath> paths = new ArrayList<>(measurementSchemas.size());
      for (IMeasurementSchema schema : measurementSchemas.values()) {
        paths.add(new NonAlignedFullPath(deviceId, schema));
      }
      return paths;
    }
  }

  private InsertRowNode buildInsertRow(
      String deviceId,
      String measurement,
      TSDataType type,
      long time,
      Object value,
      boolean aligned) {
    try {
      MeasurementSchema[] schemas = {new MeasurementSchema(measurement, type)};
      return new InsertRowNode(
          new PlanNodeId("logical-repair-row"),
          new PartialPath(deviceId),
          aligned,
          new String[] {measurement},
          new TSDataType[] {type},
          schemas,
          time,
          new Object[] {value},
          false);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to create logical repair row for " + deviceId + "." + measurement, e);
    }
  }

  private InsertRowsNode toInsertRowsNode(List<InsertRowNode> rows) {
    List<InsertRowNode> clonedRows = new ArrayList<>(rows);
    List<Integer> indexes = new ArrayList<>(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      indexes.add(i);
    }
    return new InsertRowsNode(new PlanNodeId("logical-repair-rows"), indexes, clonedRows);
  }

  private DeleteDataNode buildDeleteDataNode(TConsistencyDeletionSummary summary) {
    try {
      return new DeleteDataNode(
          new PlanNodeId("logical-repair-delete"),
          Collections.singletonList(new MeasurementPath(summary.getPathPattern())),
          summary.getTimeRangeStart(),
          summary.getTimeRangeEnd());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build logical repair delete for " + summary.getPathPattern(), e);
    }
  }

  private static int computeDeviceShard(String deviceId) {
    return Math.floorMod(deviceId.hashCode(), DEVICE_SHARD_COUNT);
  }

  private String buildRepairSessionId(String repairEpoch, List<LeafSelector> leafSelectors) {
    String selectorFingerprint =
        leafSelectors.stream()
            .map(selector -> selector.treeKind + '@' + selector.selector.toSelectorToken())
            .sorted()
            .collect(Collectors.joining(","));
    return java.util
        .UUID
        .nameUUIDFromBytes(
            (repairEpoch + "|" + selectorFingerprint).getBytes(StandardCharsets.UTF_8))
        .toString();
  }

  private long hashLogicalCell(
      String deviceId, String measurement, long time, TSDataType dataType, Object value) {
    long hash = 17L;
    hash = 31L * hash + deviceId.hashCode();
    hash = 31L * hash + measurement.hashCode();
    hash = 31L * hash + Long.hashCode(time);
    hash = 31L * hash + dataType.ordinal();
    hash = 31L * hash + valueHash(value);
    return hash;
  }

  private int valueHash(Object value) {
    if (value == null) {
      return 0;
    }
    if (value instanceof Binary) {
      return value.toString().hashCode();
    }
    return value.hashCode();
  }

  private String encodeLogicalCell(
      String deviceId, String measurement, long time, TSDataType dataType, Object value) {
    return deviceId
        + '|'
        + measurement
        + '|'
        + time
        + '|'
        + dataType.name()
        + '|'
        + valueHash(value);
  }

  private long hashDeletion(TConsistencyDeletionSummary summary) {
    long hash = 19L;
    hash = 31L * hash + summary.getPathPattern().hashCode();
    hash = 31L * hash + Long.hashCode(summary.getTimeRangeStart());
    hash = 31L * hash + Long.hashCode(summary.getTimeRangeEnd());
    return hash;
  }

  private String encodeDeletionKey(TConsistencyDeletionSummary summary) {
    return encodeDeletionKeyStatic(summary);
  }

  public static class PartitionInspection {
    private final long partitionId;
    private final long partitionMutationEpoch;
    private final long snapshotEpoch;
    private final RepairProgressTable.SnapshotState snapshotState;
    private final DualDigest liveRootDigest;
    private final DualDigest tombstoneRootDigest;
    private final String lastError;

    private PartitionInspection(
        long partitionId,
        long partitionMutationEpoch,
        long snapshotEpoch,
        RepairProgressTable.SnapshotState snapshotState,
        DualDigest liveRootDigest,
        DualDigest tombstoneRootDigest,
        String lastError) {
      this.partitionId = partitionId;
      this.partitionMutationEpoch = partitionMutationEpoch;
      this.snapshotEpoch = snapshotEpoch;
      this.snapshotState = snapshotState;
      this.liveRootDigest = liveRootDigest;
      this.tombstoneRootDigest = tombstoneRootDigest;
      this.lastError = lastError;
    }

    public long getPartitionId() {
      return partitionId;
    }

    public long getPartitionMutationEpoch() {
      return partitionMutationEpoch;
    }

    public long getSnapshotEpoch() {
      return snapshotEpoch;
    }

    public RepairProgressTable.SnapshotState getSnapshotState() {
      return snapshotState;
    }

    public DualDigest getLiveRootDigest() {
      return liveRootDigest;
    }

    public DualDigest getTombstoneRootDigest() {
      return tombstoneRootDigest;
    }

    public String getLastError() {
      return lastError;
    }
  }

  public static class SnapshotSubtreeResult {
    private final long snapshotEpoch;
    private final boolean stale;
    private final List<SnapshotNode> nodes;

    private SnapshotSubtreeResult(long snapshotEpoch, boolean stale, List<SnapshotNode> nodes) {
      this.snapshotEpoch = snapshotEpoch;
      this.stale = stale;
      this.nodes = nodes;
    }

    public static SnapshotSubtreeResult ready(long snapshotEpoch, List<SnapshotNode> nodes) {
      return new SnapshotSubtreeResult(snapshotEpoch, false, nodes);
    }

    public static SnapshotSubtreeResult stale(long snapshotEpoch) {
      return new SnapshotSubtreeResult(snapshotEpoch, true, Collections.emptyList());
    }

    public long getSnapshotEpoch() {
      return snapshotEpoch;
    }

    public boolean isStale() {
      return stale;
    }

    public List<SnapshotNode> getNodes() {
      return nodes;
    }
  }

  public static class LeafEstimate {
    private final long partitionId;
    private final long snapshotEpoch;
    private final String treeKind;
    private final String leafId;
    private final long rowCount;
    private final long tombstoneCount;
    private final long strataEstimate;
    private final String keyRangeStart;
    private final String keyRangeEnd;

    private LeafEstimate(
        long partitionId,
        long snapshotEpoch,
        String treeKind,
        String leafId,
        long rowCount,
        long tombstoneCount,
        long strataEstimate,
        String keyRangeStart,
        String keyRangeEnd) {
      this.partitionId = partitionId;
      this.snapshotEpoch = snapshotEpoch;
      this.treeKind = treeKind;
      this.leafId = leafId;
      this.rowCount = rowCount;
      this.tombstoneCount = tombstoneCount;
      this.strataEstimate = strataEstimate;
      this.keyRangeStart = keyRangeStart;
      this.keyRangeEnd = keyRangeEnd;
    }

    public long getPartitionId() {
      return partitionId;
    }

    public long getSnapshotEpoch() {
      return snapshotEpoch;
    }

    public String getTreeKind() {
      return treeKind;
    }

    public String getLeafId() {
      return leafId;
    }

    public long getRowCount() {
      return rowCount;
    }

    public long getTombstoneCount() {
      return tombstoneCount;
    }

    public long getStrataEstimate() {
      return strataEstimate;
    }

    public String getKeyRangeStart() {
      return keyRangeStart;
    }

    public String getKeyRangeEnd() {
      return keyRangeEnd;
    }
  }

  public static class LeafDiffEntry {
    private final String logicalKey;
    private final String diffType;

    private LeafDiffEntry(String logicalKey, String diffType) {
      this.logicalKey = logicalKey;
      this.diffType = diffType;
    }

    public String getLogicalKey() {
      return logicalKey;
    }

    public String getDiffType() {
      return diffType;
    }
  }

  public static class LogicalRepairBatch {
    private final String sessionId;
    private final String treeKind;
    private final String leafId;
    private final int seqNo;
    private final String batchKind;
    private final ByteBuffer payload;

    private LogicalRepairBatch(
        String sessionId,
        String treeKind,
        String leafId,
        int seqNo,
        String batchKind,
        ByteBuffer payload) {
      this.sessionId = sessionId;
      this.treeKind = treeKind;
      this.leafId = leafId;
      this.seqNo = seqNo;
      this.batchKind = batchKind;
      this.payload = payload;
    }

    public String getSessionId() {
      return sessionId;
    }

    public String getTreeKind() {
      return treeKind;
    }

    public String getLeafId() {
      return leafId;
    }

    public int getSeqNo() {
      return seqNo;
    }

    public String getBatchKind() {
      return batchKind;
    }

    public ByteBuffer getPayload() {
      return payload;
    }
  }

  public static class LeafSelector {
    private final String treeKind;
    private final LogicalLeafSelector selector;

    public LeafSelector(String treeKind, String leafId) {
      this.treeKind = treeKind;
      this.selector = LogicalLeafSelector.parse(leafId);
    }
  }

  public static class SnapshotNode {
    private final String parentNodeHandle;
    private final String nodeHandle;
    private final int depth;
    private final boolean leaf;
    private final DualDigest digest;
    private final long itemCount;
    private final String leafId;
    private final String keyRangeStart;
    private final String keyRangeEnd;
    private final List<String> childrenHandles = new ArrayList<>();

    private SnapshotNode(
        String parentNodeHandle,
        String nodeHandle,
        int depth,
        boolean leaf,
        DualDigest digest,
        long itemCount,
        String leafId,
        String keyRangeStart,
        String keyRangeEnd) {
      this.parentNodeHandle = parentNodeHandle;
      this.nodeHandle = nodeHandle;
      this.depth = depth;
      this.leaf = leaf;
      this.digest = digest;
      this.itemCount = itemCount;
      this.leafId = leafId;
      this.keyRangeStart = keyRangeStart;
      this.keyRangeEnd = keyRangeEnd;
    }

    public String getParentNodeHandle() {
      return parentNodeHandle;
    }

    public String getNodeHandle() {
      return nodeHandle;
    }

    public int getDepth() {
      return depth;
    }

    public boolean isLeaf() {
      return leaf;
    }

    public DualDigest getDigest() {
      return digest;
    }

    public long getItemCount() {
      return itemCount;
    }

    public String getLeafId() {
      return leafId;
    }

    public String getKeyRangeStart() {
      return keyRangeStart;
    }

    public String getKeyRangeEnd() {
      return keyRangeEnd;
    }
  }

  private interface LiveCellConsumer {
    void accept(
        String deviceId,
        String measurement,
        TSDataType type,
        long time,
        Object value,
        boolean aligned)
        throws Exception;
  }

  private static class RegionState {
    private final ConcurrentHashMap<Long, PartitionState> partitions = new ConcurrentHashMap<>();
    private final Object persistMonitor = new Object();
    private Map<Long, Long> lastPersistedMutationEpochs = Collections.emptyMap();
    private Map<Long, Long> pendingPersistMutationEpochs = null;
    private boolean persistInFlight = false;
  }

  private static class RepairMutationContext {
    private final String consensusGroupKey;
    private final long partitionId;
    private final String repairEpoch;

    private RepairMutationContext(String consensusGroupKey, long partitionId, String repairEpoch) {
      this.consensusGroupKey = consensusGroupKey;
      this.partitionId = partitionId;
      this.repairEpoch = repairEpoch;
    }
  }

  private static class RepairEpochRef {
    private final long partitionId;
    private final long snapshotEpoch;
    private final long partitionMutationEpoch;

    private RepairEpochRef(long partitionId, long snapshotEpoch, long partitionMutationEpoch) {
      this.partitionId = partitionId;
      this.snapshotEpoch = snapshotEpoch;
      this.partitionMutationEpoch = partitionMutationEpoch;
    }

    private static RepairEpochRef parse(String repairEpoch) {
      if (repairEpoch == null || repairEpoch.isEmpty()) {
        throw new IllegalStateException("Repair epoch is missing");
      }
      String[] parts = repairEpoch.split(":");
      if (parts.length < 5) {
        throw new IllegalStateException("Invalid repair epoch: " + repairEpoch);
      }
      try {
        return new RepairEpochRef(
            Long.parseLong(parts[1]), Long.parseLong(parts[3]), Long.parseLong(parts[4]));
      } catch (NumberFormatException e) {
        throw new IllegalStateException("Invalid repair epoch: " + repairEpoch, e);
      }
    }
  }

  private static class PartitionState {
    private final long partitionId;
    private long partitionMutationEpoch;
    private long snapshotEpoch = Long.MIN_VALUE;
    private RepairProgressTable.SnapshotState snapshotState =
        RepairProgressTable.SnapshotState.DIRTY;
    private SnapshotTree liveTree = SnapshotTree.empty();
    private SnapshotTree tombstoneTree = SnapshotTree.empty();
    private String lastError;

    private PartitionState(long partitionId) {
      this.partitionId = partitionId;
    }

    private PartitionInspection toInspection(long partitionId) {
      return new PartitionInspection(
          partitionId,
          partitionMutationEpoch,
          snapshotEpoch,
          snapshotState,
          liveTree.rootDigest,
          tombstoneTree.rootDigest,
          lastError);
    }

    private SnapshotTree getTree(String treeKind) {
      return TREE_KIND_TOMBSTONE.equalsIgnoreCase(treeKind) ? tombstoneTree : liveTree;
    }
  }

  private static class SnapshotTree {
    private static final String ROOT_HANDLE = "root";

    private final Map<String, SnapshotNode> nodesByHandle = new LinkedHashMap<>();
    private DualDigest rootDigest = DualDigest.ZERO;

    private static SnapshotTree empty() {
      SnapshotTree tree = new SnapshotTree();
      tree.nodesByHandle.put(
          ROOT_HANDLE,
          new SnapshotNode("", ROOT_HANDLE, 0, false, DualDigest.ZERO, 0L, null, null, null));
      return tree;
    }

    private void addLeafEntry(
        String leafId, int shard, String logicalKey, long hash, long itemCount) {
      String shardHandle = "shard:" + shard;
      nodesByHandle.computeIfAbsent(
          ROOT_HANDLE,
          ignored ->
              new SnapshotNode("", ROOT_HANDLE, 0, false, DualDigest.ZERO, 0L, null, null, null));
      SnapshotNode shardNode =
          nodesByHandle.computeIfAbsent(
              shardHandle,
              ignored ->
                  new SnapshotNode(
                      ROOT_HANDLE, shardHandle, 1, false, DualDigest.ZERO, 0L, null, null, null));
      if (!nodesByHandle.get(ROOT_HANDLE).childrenHandles.contains(shardHandle)) {
        nodesByHandle.get(ROOT_HANDLE).childrenHandles.add(shardHandle);
      }
      SnapshotNode leafNode =
          nodesByHandle.computeIfAbsent(
              leafId,
              ignored ->
                  new SnapshotNode(
                      shardHandle,
                      leafId,
                      2,
                      true,
                      DualDigest.ZERO,
                      0L,
                      leafId,
                      logicalKey,
                      logicalKey));
      if (!shardNode.childrenHandles.contains(leafId)) {
        shardNode.childrenHandles.add(leafId);
      }
      mergeDigest(leafNode, logicalKey, hash, itemCount);
    }

    private void finalizeTree() {
      Map<String, SnapshotNode> rebuilt = new LinkedHashMap<>();
      SnapshotNode root = nodesByHandle.get(ROOT_HANDLE);
      if (root == null) {
        root = new SnapshotNode("", ROOT_HANDLE, 0, false, DualDigest.ZERO, 0L, null, null, null);
      }
      DualDigest aggregatedRoot = DualDigest.ZERO;
      long rootItemCount = 0L;
      rebuilt.put(ROOT_HANDLE, root);
      List<String> shardHandles = new ArrayList<>(root.childrenHandles);
      shardHandles.sort(String::compareTo);
      root.childrenHandles.clear();
      for (String shardHandle : shardHandles) {
        SnapshotNode rawShard = nodesByHandle.get(shardHandle);
        if (rawShard == null) {
          continue;
        }
        DualDigest shardDigest = DualDigest.ZERO;
        long shardItemCount = 0L;
        SnapshotNode shard =
            new SnapshotNode(
                ROOT_HANDLE, shardHandle, 1, false, DualDigest.ZERO, 0L, null, null, null);
        List<String> leafHandles = new ArrayList<>(rawShard.childrenHandles);
        leafHandles.sort(String::compareTo);
        for (String leafHandle : leafHandles) {
          SnapshotNode leaf = nodesByHandle.get(leafHandle);
          if (leaf == null) {
            continue;
          }
          shard.childrenHandles.add(leafHandle);
          shardDigest = shardDigest.merge(leaf.digest);
          shardItemCount += leaf.itemCount;
          rebuilt.put(leafHandle, leaf);
        }
        SnapshotNode finalizedShard =
            new SnapshotNode(
                ROOT_HANDLE, shardHandle, 1, false, shardDigest, shardItemCount, null, null, null);
        finalizedShard.childrenHandles.addAll(shard.childrenHandles);
        rebuilt.put(shardHandle, finalizedShard);
        root.childrenHandles.add(shardHandle);
        aggregatedRoot = aggregatedRoot.merge(shardDigest);
        rootItemCount += shardItemCount;
      }
      SnapshotNode finalizedRoot =
          new SnapshotNode(
              "", ROOT_HANDLE, 0, false, aggregatedRoot, rootItemCount, null, null, null);
      finalizedRoot.childrenHandles.addAll(root.childrenHandles);
      rebuilt.put(ROOT_HANDLE, finalizedRoot);
      nodesByHandle.clear();
      nodesByHandle.putAll(rebuilt);
      rootDigest = aggregatedRoot;
    }

    private void mergeDigest(SnapshotNode leafNode, String logicalKey, long hash, long itemCount) {
      SnapshotNode merged =
          new SnapshotNode(
              leafNode.parentNodeHandle,
              leafNode.nodeHandle,
              leafNode.depth,
              leafNode.leaf,
              leafNode.digest.merge(DualDigest.fromSingleHash(hash)),
              leafNode.itemCount + itemCount,
              leafNode.leafId,
              minComparable(leafNode.keyRangeStart, logicalKey),
              maxComparable(leafNode.keyRangeEnd, logicalKey));
      merged.childrenHandles.addAll(leafNode.childrenHandles);
      nodesByHandle.put(leafNode.nodeHandle, merged);
    }

    private static String minComparable(String left, String right) {
      if (left == null) {
        return right;
      }
      if (right == null) {
        return left;
      }
      return left.compareTo(right) <= 0 ? left : right;
    }

    private static String maxComparable(String left, String right) {
      if (left == null) {
        return right;
      }
      if (right == null) {
        return left;
      }
      return left.compareTo(right) >= 0 ? left : right;
    }
  }

  private static class LogicalLeafSelector {
    private final String leafId;
    private final int shard;
    private final long bucket;
    private final String keyRangeStart;
    private final String keyRangeEnd;
    private final Set<String> exactKeys;

    private LogicalLeafSelector(
        String leafId,
        int shard,
        long bucket,
        String keyRangeStart,
        String keyRangeEnd,
        Set<String> exactKeys) {
      this.leafId = leafId;
      this.shard = shard;
      this.bucket = bucket;
      this.keyRangeStart = keyRangeStart;
      this.keyRangeEnd = keyRangeEnd;
      this.exactKeys = exactKeys == null ? Collections.emptySet() : new LinkedHashSet<>(exactKeys);
    }

    private static LogicalLeafSelector parse(String selectorToken) {
      String[] selectorParts = selectorToken.split("@", -1);
      String rawLeafId = selectorParts[0];
      String[] parts = rawLeafId.split(":");
      if (parts.length != 3 || !"leaf".equals(parts[0])) {
        throw new IllegalArgumentException("Unsupported leaf id: " + selectorToken);
      }
      return new LogicalLeafSelector(
          rawLeafId,
          Integer.parseInt(parts[1]),
          Long.parseLong(parts[2]),
          selectorParts.length >= 2 ? decodeNullable(selectorParts[1]) : null,
          selectorParts.length >= 3 ? decodeNullable(selectorParts[2]) : null,
          selectorParts.length >= 4 ? decodeStringSet(selectorParts[3]) : Collections.emptySet());
    }

    private static String leafId(int shard, long bucket) {
      return "leaf:" + shard + ":" + bucket;
    }

    private boolean matches(String deviceId, long time) {
      return computeDeviceShard(deviceId) == shard
          && Math.floorDiv(time, LEAF_TIME_BUCKET_MS) == bucket;
    }

    private boolean matchesLiveCell(
        String deviceId, String measurement, TSDataType dataType, long time, Object value) {
      if (!matches(deviceId, time)) {
        return false;
      }
      String logicalKey = encodeLogicalCellStatic(deviceId, measurement, time, dataType, value);
      if (!isWithinRange(logicalKey)) {
        return false;
      }
      return exactKeys.isEmpty() || exactKeys.contains(logicalKey);
    }

    private boolean matches(TConsistencyDeletionSummary summary) {
      return computeDeviceShard(summary.getPathPattern()) == shard
          && Math.floorDiv(summary.getTimeRangeStart(), LEAF_TIME_BUCKET_MS) <= bucket
          && Math.floorDiv(summary.getTimeRangeEnd(), LEAF_TIME_BUCKET_MS) >= bucket
          && isWithinRange(encodeDeletionKeyStatic(summary));
    }

    private boolean requiresScopedReset() {
      return !exactKeys.isEmpty() || keyRangeStart != null || keyRangeEnd != null;
    }

    private ByteBuffer serialize() {
      return ByteBuffer.wrap(toSelectorToken().getBytes(StandardCharsets.UTF_8));
    }

    private String toSelectorToken() {
      return leafId
          + "@"
          + encodeNullable(keyRangeStart)
          + "@"
          + encodeNullable(keyRangeEnd)
          + "@"
          + encodeStringSet(exactKeys);
    }

    private static LogicalLeafSelector deserialize(ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return parse(new String(bytes, StandardCharsets.UTF_8));
    }

    private static String encodeNullable(String value) {
      if (value == null) {
        return "";
      }
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeNullable(String value) {
      if (value == null || value.isEmpty()) {
        return null;
      }
      return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static String encodeStringSet(Set<String> values) {
      if (values == null || values.isEmpty()) {
        return "";
      }
      return encodeNullable(String.join("\n", values));
    }

    private static Set<String> decodeStringSet(String encoded) {
      String decoded = decodeNullable(encoded);
      if (decoded == null || decoded.isEmpty()) {
        return Collections.emptySet();
      }
      Set<String> values = new LinkedHashSet<>();
      Collections.addAll(values, decoded.split("\n"));
      values.remove("");
      return values;
    }

    private boolean isWithinRange(String logicalKey) {
      if (logicalKey == null) {
        return false;
      }
      if (keyRangeStart != null && logicalKey.compareTo(keyRangeStart) < 0) {
        return false;
      }
      return keyRangeEnd == null || logicalKey.compareTo(keyRangeEnd) <= 0;
    }
  }

  private static String encodeLogicalCellStatic(
      String deviceId, String measurement, long time, TSDataType dataType, Object value) {
    return deviceId
        + '|'
        + measurement
        + '|'
        + time
        + '|'
        + dataType.name()
        + '|'
        + valueHashStatic(value);
  }

  private static String encodeDeletionKeyStatic(TConsistencyDeletionSummary summary) {
    return summary.getPathPattern()
        + '|'
        + summary.getTimeRangeStart()
        + '|'
        + summary.getTimeRangeEnd();
  }

  private static int valueHashStatic(Object value) {
    if (value == null) {
      return 0;
    }
    if (value instanceof Binary) {
      return value.toString().hashCode();
    }
    return value.hashCode();
  }
}
