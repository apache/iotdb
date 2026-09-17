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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * LOAD piece dispatcher: holds the buffered pieces of one source TsFile - one {@link
 * LoadTsFilePieceNode} per target region - and decides when they are sent.
 *
 * <p>State:
 *
 * <ul>
 *   <li>{@code regionId2ReplicaSetAndNode} - the current piece of every touched region;
 *   <li>{@code largestPieceRegions} - max-heap of (region, piece size at offer time) used for
 *       largest-first eviction; entries become stale once a piece grows or is dispatched and are
 *       skipped lazily on poll.
 * </ul>
 *
 * <p>Behaviors:
 *
 * <ul>
 *   <li>{@link #offerChunk(ChunkData, TRegionReplicaSet)} appends a chunk and throws {@link
 *       RegionReplicaSetChangedException} if the same region suddenly maps to a different replica
 *       set (region migration);
 *   <li>{@link #addDeletionToAll(DeletionData)} replicates a deletion into every buffered piece
 *       (memory is accounted once per region);
 *   <li>{@link #dispatchLargestUntilMemoryEnough()} evicts largest pieces while over budget;
 *   <li>{@link #flushAll()} flushes the remainder at end of file.
 * </ul>
 *
 * Dispatch is delegated back through {@link DispatchCallback}, so this class never talks to
 * consensus itself; {@link TwoPhaseConsensusLoadStrategy} implements the callback with its
 * BEGIN/PIECE submission logic.
 */
class PieceDispatcher {

  private record PartitionKey(TConsensusGroupId regionId, long timePartitionStart) {}

  private static final Logger LOGGER = LoggerFactory.getLogger(PieceDispatcher.class);

  @FunctionalInterface
  interface DispatchCallback {
    boolean dispatch(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet);
  }

  private final LoadSingleTsFileNode singleTsFileNode;
  private final MemoryBoundedBuffer memoryBuffer;
  private final DispatchCallback dispatchCallback;

  private final Map<PartitionKey, Pair<TRegionReplicaSet, LoadTsFilePieceNode>>
      regionId2ReplicaSetAndNode = new HashMap<>();
  private final Map<TConsensusGroupId, Long> region2NextPieceIndex = new HashMap<>();
  private final Map<PartitionKey, ChunkOffsetCalculator> partition2OffsetCalculator =
      new HashMap<>();

  /**
   * Max-heap of (regionId, buffered piece size at offer time) used to dispatch the largest pieces
   * first when the data cache is over budget. Entries become stale once the piece size changes or
   * the piece is dispatched, and are skipped lazily on poll, which avoids re-sorting all buffered
   * pieces on every over-budget event.
   */
  private final PriorityQueue<Map.Entry<PartitionKey, Long>> largestPieceRegions =
      new PriorityQueue<>((a, b) -> Long.compare(b.getValue(), a.getValue()));

  PieceDispatcher(
      LoadSingleTsFileNode singleTsFileNode,
      MemoryBoundedBuffer memoryBuffer,
      DispatchCallback dispatchCallback) {
    this.singleTsFileNode = singleTsFileNode;
    this.memoryBuffer = memoryBuffer;
    this.dispatchCallback = dispatchCallback;
  }

  void offerChunk(ChunkData chunkData, TRegionReplicaSet replicaSet)
      throws LoadFileException, IOException {
    final TConsensusGroupId regionId = replicaSet.getRegionId();
    final PartitionKey key =
        new PartitionKey(regionId, chunkData.getTimePartitionSlot().getStartTime());
    if (regionId2ReplicaSetAndNode.containsKey(key)
        && !Objects.equals(regionId2ReplicaSetAndNode.get(key).getLeft(), replicaSet)) {
      // Detected region replica set changed (maybe due to region migration), throw an exception
      throw new RegionReplicaSetChangedException(
          regionId2ReplicaSetAndNode.get(key).getLeft(), replicaSet);
    }

    ChunkOffsetCalculator calculator =
        partition2OffsetCalculator.computeIfAbsent(key, ignored -> new ChunkOffsetCalculator());

    calculator.assign(chunkData);

    regionId2ReplicaSetAndNode
        .computeIfAbsent(key, o -> new Pair<>(replicaSet, newPieceNode(key)))
        .getRight()
        .addTsFileData(chunkData);
    offerPieceRegion(key);
  }

  /** Replicates the deletion into every buffered piece; memory is accounted once per region. */
  void addDeletionToAll(DeletionData deletionData) {
    for (Map.Entry<PartitionKey, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
        regionId2ReplicaSetAndNode.entrySet()) {
      memoryBuffer.add(deletionData.getDataSize());
      entry.getValue().getRight().addTsFileData(deletionData);
      offerPieceRegion(entry.getKey());
    }
  }

  /** Dispatches from the biggest buffered piece until the data cache is back under budget. */
  boolean dispatchLargestUntilMemoryEnough() throws LoadFileException {
    while (!memoryBuffer.isMemoryEnough()) {
      final PartitionKey key = pollLargestPieceRegion();
      if (key == null) {
        // No dispatchable piece remains; the remaining buffered data stays buffered until the
        // next flush (end of file, a later deletion, or another over-budget event).
        break;
      }
      final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair = regionId2ReplicaSetAndNode.get(key);
      final LoadTsFilePieceNode pieceNode = pair.getRight();
      memoryBuffer.release(pieceNode.getDataSize());
      if (!dispatchOne(pieceNode, pair.getLeft())) {
        return false;
      }
      replacePieceNode(key, pair.getLeft());
    }
    return true;
  }

  /** Dispatches every non-empty buffered piece, e.g. at the end of the source TsFile. */
  boolean flushAll() throws LoadFileException {
    for (Map.Entry<PartitionKey, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
        regionId2ReplicaSetAndNode.entrySet()) {
      final LoadTsFilePieceNode pieceNode = entry.getValue().getRight();
      if (pieceNode.getDataSize() == 0) {
        continue;
      }
      if (!dispatchPieces(Collections.singleton(entry.getKey()))) {
        return false;
      }
    }
    return true;
  }

  private boolean dispatchPieces(Collection<PartitionKey> keys) throws LoadFileException {
    for (PartitionKey key : keys) {
      final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair = regionId2ReplicaSetAndNode.get(key);
      if (pair == null) {
        continue;
      }
      final LoadTsFilePieceNode pieceNode = pair.getRight();
      if (pieceNode.getDataSize() == 0) {
        continue;
      }
      memoryBuffer.release(pieceNode.getDataSize());
      if (!dispatchOne(pieceNode, pair.getLeft())) {
        LOGGER.warn(
            DataNodeQueryMessages.DISPATCH_PIECE_NODE_ARG_OF_TSFILE_ARG_ERROR,
            pieceNode,
            singleTsFileNode.getTsFileResource().getTsFile());
        return false;
      }
      replacePieceNode(key, pair.getLeft());
    }
    return true;
  }

  private boolean dispatchOne(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    return dispatchCallback.dispatch(pieceNode, replicaSet);
  }

  private void replacePieceNode(PartitionKey key, TRegionReplicaSet replicaSet) {
    regionId2ReplicaSetAndNode.replace(key, new Pair<>(replicaSet, newPieceNode(key)));
  }

  private LoadTsFilePieceNode newPieceNode(PartitionKey key) {
    // pieceIndex is a consensus request identity, not a physical file offset. It must be unique
    // across every time partition routed to the same Region/loadId.
    final long pieceIndex = region2NextPieceIndex.getOrDefault(key.regionId(), 0L);
    region2NextPieceIndex.put(key.regionId(), pieceIndex + 1);
    return new LoadTsFilePieceNode(
        singleTsFileNode.getPlanNodeId(),
        singleTsFileNode.getTsFileResource().getTsFile(),
        pieceIndex);
  }

  private void offerPieceRegion(final PartitionKey key) {
    final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair = regionId2ReplicaSetAndNode.get(key);
    if (pair != null) {
      largestPieceRegions.offer(Map.entry(key, pair.getRight().getDataSize()));
    }
  }

  /** Pops the region with the largest non-empty buffered piece, skipping stale heap entries. */
  private PartitionKey pollLargestPieceRegion() {
    while (!largestPieceRegions.isEmpty()) {
      final Map.Entry<PartitionKey, Long> entry = largestPieceRegions.poll();
      final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair =
          regionId2ReplicaSetAndNode.get(entry.getKey());
      if (pair == null) {
        continue;
      }
      final long currentSize = pair.getRight().getDataSize();
      if (entry.getValue() == currentSize && currentSize > 0) {
        return entry.getKey();
      }
    }
    return null;
  }

  void clear() {
    regionId2ReplicaSetAndNode.clear();
    region2NextPieceIndex.clear();
    partition2OffsetCalculator.clear();
    largestPieceRegions.clear();
  }
}
