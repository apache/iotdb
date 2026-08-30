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

  private static final Logger LOGGER = LoggerFactory.getLogger(PieceDispatcher.class);

  @FunctionalInterface
  interface DispatchCallback {
    boolean dispatch(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet);
  }

  private final LoadSingleTsFileNode singleTsFileNode;
  private final MemoryBoundedBuffer memoryBuffer;
  private final DispatchCallback dispatchCallback;

  private final Map<TConsensusGroupId, Pair<TRegionReplicaSet, LoadTsFilePieceNode>>
      regionId2ReplicaSetAndNode = new HashMap<>();

  /**
   * Max-heap of (regionId, buffered piece size at offer time) used to dispatch the largest pieces
   * first when the data cache is over budget. Entries become stale once the piece size changes or
   * the piece is dispatched, and are skipped lazily on poll, which avoids re-sorting all buffered
   * pieces on every over-budget event.
   */
  private final PriorityQueue<Map.Entry<TConsensusGroupId, Long>> largestPieceRegions =
      new PriorityQueue<>((a, b) -> Long.compare(b.getValue(), a.getValue()));

  PieceDispatcher(
      LoadSingleTsFileNode singleTsFileNode,
      MemoryBoundedBuffer memoryBuffer,
      DispatchCallback dispatchCallback) {
    this.singleTsFileNode = singleTsFileNode;
    this.memoryBuffer = memoryBuffer;
    this.dispatchCallback = dispatchCallback;
  }

  void offerChunk(ChunkData chunkData, TRegionReplicaSet replicaSet) throws LoadFileException {
    final TConsensusGroupId regionId = replicaSet.getRegionId();
    if (regionId2ReplicaSetAndNode.containsKey(regionId)
        && !Objects.equals(regionId2ReplicaSetAndNode.get(regionId).getLeft(), replicaSet)) {
      // Detected region replica set changed (maybe due to region migration), throw an exception
      throw new RegionReplicaSetChangedException(
          regionId2ReplicaSetAndNode.get(regionId).getLeft(), replicaSet);
    }

    regionId2ReplicaSetAndNode
        .computeIfAbsent(regionId, o -> new Pair<>(replicaSet, newPieceNode()))
        .getRight()
        .addTsFileData(chunkData);
    offerPieceRegion(regionId);
  }

  /** Replicates the deletion into every buffered piece; memory is accounted once per region. */
  void addDeletionToAll(DeletionData deletionData) {
    for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
        regionId2ReplicaSetAndNode.entrySet()) {
      memoryBuffer.add(deletionData.getDataSize());
      entry.getValue().getRight().addTsFileData(deletionData);
      offerPieceRegion(entry.getKey());
    }
  }

  /** Dispatches from the biggest buffered piece until the data cache is back under budget. */
  boolean dispatchLargestUntilMemoryEnough() throws LoadFileException {
    while (!memoryBuffer.isMemoryEnough()) {
      final TConsensusGroupId regionId = pollLargestPieceRegion();
      if (regionId == null) {
        // No dispatchable piece remains; the remaining buffered data stays buffered until the
        // next flush (end of file, a later deletion, or another over-budget event).
        break;
      }
      final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair =
          regionId2ReplicaSetAndNode.get(regionId);
      final LoadTsFilePieceNode pieceNode = pair.getRight();
      memoryBuffer.release(pieceNode.getDataSize());
      if (!dispatchOne(pieceNode, pair.getLeft())) {
        return false;
      }
      replacePieceNode(regionId, pair.getLeft());
    }
    return true;
  }

  /** Dispatches every non-empty buffered piece, e.g. at the end of the source TsFile. */
  boolean flushAll() throws LoadFileException {
    for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
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

  private boolean dispatchPieces(Collection<TConsensusGroupId> regionIds) throws LoadFileException {
    for (TConsensusGroupId regionId : regionIds) {
      final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair =
          regionId2ReplicaSetAndNode.get(regionId);
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
      replacePieceNode(regionId, pair.getLeft());
    }
    return true;
  }

  private boolean dispatchOne(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    return dispatchCallback.dispatch(pieceNode, replicaSet);
  }

  private void replacePieceNode(TConsensusGroupId regionId, TRegionReplicaSet replicaSet) {
    regionId2ReplicaSetAndNode.replace(regionId, new Pair<>(replicaSet, newPieceNode()));
  }

  private LoadTsFilePieceNode newPieceNode() {
    return new LoadTsFilePieceNode(
        singleTsFileNode.getPlanNodeId(), singleTsFileNode.getTsFileResource().getTsFile());
  }

  private void offerPieceRegion(final TConsensusGroupId regionId) {
    final Pair<TRegionReplicaSet, LoadTsFilePieceNode> pair =
        regionId2ReplicaSetAndNode.get(regionId);
    if (pair != null) {
      largestPieceRegions.offer(Map.entry(regionId, pair.getRight().getDataSize()));
    }
  }

  /** Pops the region with the largest non-empty buffered piece, skipping stale heap entries. */
  private TConsensusGroupId pollLargestPieceRegion() {
    while (!largestPieceRegions.isEmpty()) {
      final Map.Entry<TConsensusGroupId, Long> entry = largestPieceRegions.poll();
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
    largestPieceRegions.clear();
  }
}
