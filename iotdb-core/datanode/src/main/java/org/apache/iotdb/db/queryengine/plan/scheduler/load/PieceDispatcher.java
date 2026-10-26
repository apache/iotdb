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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.load.RegionReplicaSetChangedException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Buffers chunks and deletions of one source TsFile per target region partition, managing
 * memory-bounded eviction (largest-first) and final piece flush.
 */
class PieceDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(PieceDispatcher.class);

  @FunctionalInterface
  interface DispatchCallback {
    boolean dispatch(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet);
  }

  private final LoadSingleTsFileNode singleTsFileNode;
  private final MemoryBoundedBuffer memoryBuffer;
  private final DispatchCallback dispatchCallback;

  private final Map<PartitionKey, Pair<TRegionReplicaSet, LoadTsFilePieceNode>>
      partition2ReplicaSetAndNode = new HashMap<>();
  private final Map<TConsensusGroupId, Long> region2NextPieceIndex = new HashMap<>();
  private final Map<PartitionKey, ChunkOffsetCalculator> partition2OffsetCalculator =
      new HashMap<>();

  /** One ordered entry per non-empty partition, so tracking stays bounded as chunks stream in. */
  private final NavigableSet<PieceSize> largestPieceRegions =
      new TreeSet<>(
          Comparator.comparingLong(PieceSize::dataSize)
              .reversed()
              .thenComparing(entry -> entry.key().regionId())
              .thenComparingLong(entry -> entry.key().timePartitionStart()));

  PieceDispatcher(
      final LoadSingleTsFileNode singleTsFileNode,
      final MemoryBoundedBuffer memoryBuffer,
      final DispatchCallback dispatchCallback) {
    this.singleTsFileNode =
        Objects.requireNonNull(
            singleTsFileNode,
            DataNodeQueryMessages.EXCEPTION_SINGLETSFILENODE_CANNOT_BE_NULL_4EA6CF51);
    this.memoryBuffer =
        Objects.requireNonNull(
            memoryBuffer, DataNodeQueryMessages.EXCEPTION_MEMORYBUFFER_CANNOT_BE_NULL_77101F0C);
    this.dispatchCallback =
        Objects.requireNonNull(
            dispatchCallback,
            DataNodeQueryMessages.EXCEPTION_DISPATCHCALLBACK_CANNOT_BE_NULL_C7A1AC6A);
  }

  // -------------------------------------------------------------------------
  // Ingestion: Chunks & Deletions
  // -------------------------------------------------------------------------

  /**
   * Buffers a chunk into its corresponding target partition piece, validating topology continuity.
   */
  void offerChunk(final ChunkData chunkData, final TRegionReplicaSet replicaSet)
      throws LoadFileException, IOException {
    Objects.requireNonNull(
        chunkData, DataNodeQueryMessages.EXCEPTION_CHUNKDATA_CANNOT_BE_NULL_7D931C4D);
    Objects.requireNonNull(
        replicaSet, StorageEngineMessages.EXCEPTION_REPLICASET_CANNOT_BE_NULL_A7340AC3);

    final TConsensusGroupId regionId = replicaSet.getRegionId();
    final PartitionKey key =
        new PartitionKey(regionId, chunkData.getTimePartitionSlot().getStartTime());

    final Pair<TRegionReplicaSet, LoadTsFilePieceNode> existingEntry =
        partition2ReplicaSetAndNode.get(key);
    if (existingEntry != null && !Objects.equals(existingEntry.getLeft(), replicaSet)) {
      throw new RegionReplicaSetChangedException(existingEntry.getLeft(), replicaSet);
    }

    final ChunkOffsetCalculator calculator =
        partition2OffsetCalculator.computeIfAbsent(key, ignored -> new ChunkOffsetCalculator());
    calculator.assign(chunkData);

    final Pair<TRegionReplicaSet, LoadTsFilePieceNode> targetPair =
        partition2ReplicaSetAndNode.computeIfAbsent(
            key, k -> new Pair<>(replicaSet, createPieceNode(k)));

    largestPieceRegions.remove(new PieceSize(key, targetPair.getRight().getDataSize()));
    targetPair.getRight().addTsFileData(chunkData);
    trackPieceSize(key, targetPair.getRight().getDataSize());
  }

  /** Buffers one copy per region; its receiver applies the deletion to all staged partitions. */
  void addDeletionToAllRegions(final DeletionData deletionData) {
    if (deletionData == null || partition2ReplicaSetAndNode.isEmpty()) {
      return;
    }

    final long deletionSize = deletionData.getDataSize();
    final Set<TConsensusGroupId> regions = new HashSet<>();
    for (final Map.Entry<PartitionKey, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
        partition2ReplicaSetAndNode.entrySet()) {
      if (!regions.add(entry.getKey().regionId())) {
        continue;
      }
      memoryBuffer.add(deletionSize);
      final LoadTsFilePieceNode pieceNode = entry.getValue().getRight();
      largestPieceRegions.remove(new PieceSize(entry.getKey(), pieceNode.getDataSize()));
      pieceNode.addTsFileData(deletionData);
      trackPieceSize(entry.getKey(), pieceNode.getDataSize());
    }
  }

  // -------------------------------------------------------------------------
  // Dispatch & Eviction Controls
  // -------------------------------------------------------------------------

  /** Progressively dispatches largest buffered pieces until memory usage is back within budget. */
  boolean dispatchLargestUntilMemoryEnough() throws LoadFileException {
    while (!memoryBuffer.isMemoryEnough()) {
      final PartitionKey key = pollLargestPartition();
      if (key == null) {
        break;
      }

      final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair =
          partition2ReplicaSetAndNode.get(key);
      if (pair == null) {
        continue;
      }

      final LoadTsFilePieceNode pieceNode = pair.getRight();
      final long pieceSize = pieceNode.getDataSize();

      if (!dispatchCallback.dispatch(pieceNode, pair.getLeft())) {
        trackPieceSize(key, pieceSize);
        return false;
      }

      memoryBuffer.release(pieceSize);
      replacePieceNode(key, pair.getLeft());
    }
    return true;
  }

  /** Flushes all remaining non-empty buffered pieces across all partitions. */
  boolean flushAll() throws LoadFileException {
    for (final Map.Entry<PartitionKey, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
        partition2ReplicaSetAndNode.entrySet()) {
      final LoadTsFilePieceNode pieceNode = entry.getValue().getRight();
      final long pieceSize = pieceNode.getDataSize();
      if (pieceSize == 0) {
        continue;
      }

      if (!dispatchCallback.dispatch(pieceNode, entry.getValue().getLeft())) {
        LOGGER.warn(
            DataNodeQueryMessages.DISPATCH_PIECE_NODE_ARG_OF_TSFILE_ARG_ERROR,
            pieceNode,
            singleTsFileNode.getTsFileResource().getTsFile());
        return false;
      }

      memoryBuffer.release(pieceSize);
      replacePieceNode(entry.getKey(), entry.getValue().getLeft());
    }
    return true;
  }

  // -------------------------------------------------------------------------
  // Helpers & Eviction Tracking
  // -------------------------------------------------------------------------

  private void replacePieceNode(final PartitionKey key, final TRegionReplicaSet replicaSet) {
    largestPieceRegions.remove(
        new PieceSize(key, partition2ReplicaSetAndNode.get(key).getRight().getDataSize()));
    partition2ReplicaSetAndNode.put(key, new Pair<>(replicaSet, createPieceNode(key)));
  }

  private LoadTsFilePieceNode createPieceNode(final PartitionKey key) {
    final long pieceIndex = region2NextPieceIndex.getOrDefault(key.regionId(), 0L);
    region2NextPieceIndex.put(key.regionId(), pieceIndex + 1);
    return new LoadTsFilePieceNode(
        singleTsFileNode.getPlanNodeId(),
        singleTsFileNode.getTsFileResource().getTsFile(),
        pieceIndex);
  }

  private void trackPieceSize(final PartitionKey key, final long size) {
    if (size > 0) {
      largestPieceRegions.add(new PieceSize(key, size));
    }
  }

  private PartitionKey pollLargestPartition() {
    final PieceSize entry = largestPieceRegions.pollFirst();
    return entry == null ? null : entry.key();
  }

  void clear() {
    partition2ReplicaSetAndNode.clear();
    region2NextPieceIndex.clear();
    partition2OffsetCalculator.clear();
    largestPieceRegions.clear();
  }

  // -------------------------------------------------------------------------
  // Immutable Records
  // -------------------------------------------------------------------------

  private record PartitionKey(TConsensusGroupId regionId, long timePartitionStart) {}

  private record PieceSize(PartitionKey key, long dataSize) {}
}
