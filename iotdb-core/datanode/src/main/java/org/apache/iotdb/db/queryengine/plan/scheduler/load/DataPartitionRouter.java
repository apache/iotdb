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
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Routes unordered chunk data to designated data region replica sets. Deduplicates (device,
 * time-partition) slots, queries partitions in batch, and reconstructs the mapped target replica
 * set for every chunk in original input order.
 */
class DataPartitionRouter {

  private final DataPartitionBatchFetcher partitionFetcher;
  private final String userName;

  DataPartitionRouter(final DataPartitionBatchFetcher partitionFetcher, final String userName) {
    this.partitionFetcher =
        Objects.requireNonNull(
            partitionFetcher,
            DataNodeQueryMessages.EXCEPTION_PARTITIONFETCHER_CANNOT_BE_NULL_5B709CA8);
    this.userName = userName;
  }

  // -------------------------------------------------------------------------
  // Chunk Routing Pipeline
  // -------------------------------------------------------------------------

  /**
   * Resolves target region replica sets for the input chunk list, preserving input order.
   *
   * @param chunkDataList chunks to be routed
   * @return mapped target replica sets matching the chunkDataList 1:1
   */
  List<TRegionReplicaSet> route(final List<ChunkData> chunkDataList) {
    if (chunkDataList == null || chunkDataList.isEmpty()) {
      return Collections.emptyList();
    }

    final int totalChunks = chunkDataList.size();
    final int[] chunkPartitionIndexes = new int[totalChunks];
    final List<Pair<IDeviceID, TTimePartitionSlot>> distinctSlotList = new ArrayList<>();
    final Map<SlotKey, Integer> slotToIndexMap = new HashMap<>(totalChunks);

    // 1. Deduplicate (device, time-partition) pairs and record mapping index per chunk
    for (int i = 0; i < totalChunks; i++) {
      final ChunkData chunkData = chunkDataList.get(i);
      if (chunkData == null) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_CHUNKDATA_AT_INDEX_ARG_CANNOT_BE_NULL_72FCDB2B, i));
      }

      final IDeviceID device =
          Objects.requireNonNull(
              chunkData.getDevice(),
              DataNodeQueryMessages.EXCEPTION_CHUNK_DEVICE_CANNOT_BE_NULL_2EC887AC);
      final TTimePartitionSlot slot =
          Objects.requireNonNull(
              chunkData.getTimePartitionSlot(),
              DataNodeQueryMessages.EXCEPTION_CHUNK_TIME_PARTITION_SLOT_CANNOT_BE_NULL_E5B04C6F);

      final SlotKey slotKey = new SlotKey(device, slot);
      final Integer existingIndex = slotToIndexMap.get(slotKey);

      if (existingIndex == null) {
        final int newIndex = distinctSlotList.size();
        slotToIndexMap.put(slotKey, newIndex);
        distinctSlotList.add(new Pair<>(device, slot));
        chunkPartitionIndexes[i] = newIndex;
      } else {
        chunkPartitionIndexes[i] = existingIndex;
      }
    }

    // 2. Fetch data partition replica sets in batch
    final List<TRegionReplicaSet> replicaSets =
        partitionFetcher.queryDataPartition(distinctSlotList, userName);

    if (replicaSets == null || replicaSets.size() != distinctSlotList.size()) {
      throw new IllegalStateException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_PARTITION_FETCHER_RETURNED_MISMATCHED_REPLICA_SET_SIZE_EXPECTED_ARG_ACTUAL_ARG_B7A988CD,
              distinctSlotList.size(),
              replicaSets == null ? 0 : replicaSets.size()));
    }

    // 3. Map resolved replica sets back to original input order
    final List<TRegionReplicaSet> routedReplicaSets = new ArrayList<>(totalChunks);
    for (int i = 0; i < totalChunks; i++) {
      final int partitionIndex = chunkPartitionIndexes[i];
      final TRegionReplicaSet replicaSet = replicaSets.get(partitionIndex);
      if (replicaSet == null) {
        final Pair<IDeviceID, TTimePartitionSlot> slotPair = distinctSlotList.get(partitionIndex);
        throw new IllegalStateException(
            String.format(
                DataNodeQueryMessages
                    .EXCEPTION_NULL_REPLICA_SET_RESOLVED_FOR_DEVICE_ARG_AT_PARTITION_ARG_19CA94B2,
                slotPair.left,
                slotPair.right));
      }
      routedReplicaSets.add(replicaSet);
    }

    return routedReplicaSets;
  }

  // -------------------------------------------------------------------------
  // Internal Model
  // -------------------------------------------------------------------------

  /** Compact composite key for slot deduplication within a single route pass. */
  private record SlotKey(IDeviceID device, TTimePartitionSlot slot) {}
}
