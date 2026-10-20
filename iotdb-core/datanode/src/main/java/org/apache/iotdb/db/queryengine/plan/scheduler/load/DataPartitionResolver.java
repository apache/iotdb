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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Resolves the data partitions of a TsFile for the load. The judgement of the file resolves the
 * partitions of its devices anyway, so the ones it resolved are kept and reused when the routing of
 * its chunks asks for the same slots.
 */
class DataPartitionResolver {

  private final DataPartitionBatchFetcher partitionFetcher;
  private final String userName;

  /** The replica set resolved for every slot so far. */
  private final Map<SlotKey, TRegionReplicaSet> resolvedPartitions = new HashMap<>();

  DataPartitionResolver(final DataPartitionBatchFetcher partitionFetcher, final String userName) {
    this.partitionFetcher =
        Objects.requireNonNull(
            partitionFetcher,
            DataNodeQueryMessages.EXCEPTION_PARTITIONFETCHER_CANNOT_BE_NULL_5B709CA8);
    this.userName = userName;
  }

  /**
   * Resolves the replica sets of the given slots in order: the ones resolved before are reused, the
   * other ones are fetched in one batch and kept.
   */
  List<TRegionReplicaSet> resolve(final List<Pair<IDeviceID, TTimePartitionSlot>> slotList) {
    final List<TRegionReplicaSet> replicaSets = new ArrayList<>(slotList.size());
    final List<Pair<IDeviceID, TTimePartitionSlot>> slotsToFetch = new ArrayList<>();
    final List<Integer> indexesToFetch = new ArrayList<>();
    for (int i = 0; i < slotList.size(); i++) {
      final Pair<IDeviceID, TTimePartitionSlot> slot = slotList.get(i);
      final TRegionReplicaSet resolved = resolvedPartitions.get(new SlotKey(slot.left, slot.right));
      replicaSets.add(resolved);
      if (resolved == null) {
        slotsToFetch.add(slot);
        indexesToFetch.add(i);
      }
    }

    if (!slotsToFetch.isEmpty()) {
      final List<TRegionReplicaSet> fetched =
          partitionFetcher.queryDataPartition(slotsToFetch, userName);
      if (fetched == null || fetched.size() != slotsToFetch.size()) {
        throw new IllegalStateException(
            String.format(
                DataNodeQueryMessages
                    .EXCEPTION_PARTITION_FETCHER_RETURNED_MISMATCHED_REPLICA_SET_SIZE_EXPECTED_ARG_ACTUAL_ARG_B7A988CD,
                slotsToFetch.size(),
                fetched == null ? 0 : fetched.size()));
      }

      // The fetcher answers in the order it was asked, which is not the order of the slots.
      for (int i = 0; i < fetched.size(); i++) {
        final TRegionReplicaSet replicaSet = fetched.get(i);
        replicaSets.set(indexesToFetch.get(i), replicaSet);
        final Pair<IDeviceID, TTimePartitionSlot> slot = slotsToFetch.get(i);
        resolvedPartitions.put(new SlotKey(slot.left, slot.right), replicaSet);
      }
    }

    return replicaSets;
  }

  /** One (device, time partition) pair, the key the partitions are resolved by. */
  private record SlotKey(IDeviceID device, TTimePartitionSlot slot) {}
}
