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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LOAD chunk router: maps chunks to their target regions. {@link #route(List)} takes the
 * directionless chunk buffer and:
 *
 * <ol>
 *   <li>deduplicates the (device, time-partition) pairs, so many chunks over the same slot issue
 *       only one partition query per distinct slot;
 *   <li>resolves every distinct pair through {@link DataPartitionBatchFetcher};
 *   <li>returns the replica set of every input chunk, preserving the input order.
 * </ol>
 *
 * The caller ({@code TsFileSplitConsumer.routeChunkData()}) feeds the result to {@link
 * PieceDispatcher}, which performs the replica-set-change detection and appends the chunk to the
 * per-region piece.
 */
class DataPartitionRouter {

  private final DataPartitionBatchFetcher partitionFetcher;
  private final String userName;

  DataPartitionRouter(DataPartitionBatchFetcher partitionFetcher, String userName) {
    this.partitionFetcher = partitionFetcher;
    this.userName = userName;
  }

  /**
   * Returns, for every chunk in the input list (same order), the region replica set it must be
   * written to.
   */
  List<TRegionReplicaSet> route(List<ChunkData> chunkDataList) {
    if (chunkDataList.isEmpty()) {
      return new ArrayList<>();
    }

    final List<Pair<IDeviceID, TTimePartitionSlot>> partitionSlotList = new ArrayList<>();
    final int[] chunkPartitionIndexes = new int[chunkDataList.size()];
    final Map<IDeviceID, Map<TTimePartitionSlot, Integer>> partitionSlotIndexes = new HashMap<>();
    for (int i = 0, size = chunkDataList.size(); i < size; i++) {
      final ChunkData chunkData = chunkDataList.get(i);
      final IDeviceID device = chunkData.getDevice();
      final TTimePartitionSlot timePartitionSlot = chunkData.getTimePartitionSlot();
      final Map<TTimePartitionSlot, Integer> slotIndexes =
          partitionSlotIndexes.computeIfAbsent(device, key -> new HashMap<>());
      Integer partitionSlotIndex = slotIndexes.get(timePartitionSlot);
      if (partitionSlotIndex == null) {
        partitionSlotIndex = partitionSlotList.size();
        slotIndexes.put(timePartitionSlot, partitionSlotIndex);
        partitionSlotList.add(new Pair<>(device, timePartitionSlot));
      }
      chunkPartitionIndexes[i] = partitionSlotIndex;
    }

    final List<TRegionReplicaSet> replicaSets =
        partitionFetcher.queryDataPartition(partitionSlotList, userName);
    final List<TRegionReplicaSet> routedReplicaSets = new ArrayList<>(chunkDataList.size());
    for (int i = 0, size = chunkDataList.size(); i < size; i++) {
      routedReplicaSets.add(replicaSets.get(chunkPartitionIndexes[i]));
    }
    return routedReplicaSets;
  }
}
