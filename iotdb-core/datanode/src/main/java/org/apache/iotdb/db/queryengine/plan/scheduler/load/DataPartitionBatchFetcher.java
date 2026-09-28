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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Batched data-partition fetcher for the LOAD execution pipeline. Splits high-volume partition
 * requests into chunks bounded by transmission limits and resolves corresponding region replica
 * sets in consistent order.
 */
class DataPartitionBatchFetcher {

  private static final int TRANSMIT_LIMIT =
      CommonDescriptor.getInstance().getConfig().getTTimePartitionSlotTransmitLimit();

  private final IPartitionFetcher fetcher;
  private volatile String database;

  DataPartitionBatchFetcher(final IPartitionFetcher fetcher) {
    this.fetcher =
        Objects.requireNonNull(
            fetcher, DataNodeQueryMessages.EXCEPTION_FETCHER_CANNOT_BE_NULL_6C143C90);
  }

  void setDatabase(final String database) {
    this.database = database;
  }

  String getDatabase() {
    return database;
  }

  // -------------------------------------------------------------------------
  // Partition Querying
  // -------------------------------------------------------------------------

  /**
   * Queries or creates data partition replica sets for the supplied (device, time-partition) slots.
   * Guarantees 1:1 order-matching between input slots and returned replica sets.
   */
  List<TRegionReplicaSet> queryDataPartition(
      final List<Pair<IDeviceID, TTimePartitionSlot>> slotList, final String userName) {
    if (slotList == null || slotList.isEmpty()) {
      return Collections.emptyList();
    }

    final int totalSlots = slotList.size();
    final List<TRegionReplicaSet> replicaSets = new ArrayList<>(totalSlots);
    final String currentDatabase = this.database;

    for (int fromIndex = 0; fromIndex < totalSlots; fromIndex += TRANSMIT_LIMIT) {
      final int toIndex = Math.min(totalSlots, fromIndex + TRANSMIT_LIMIT);
      final List<Pair<IDeviceID, TTimePartitionSlot>> batchSlots =
          slotList.subList(fromIndex, toIndex);

      final List<DataPartitionQueryParam> queryParams = toQueryParams(batchSlots, currentDatabase);
      final DataPartition dataPartition = fetcher.getOrCreateDataPartition(queryParams, userName);

      if (dataPartition == null) {
        throw new IllegalStateException(
            DataNodeQueryMessages
                .EXCEPTION_FAILED_TO_RETRIEVE_PARTITION_FROM_FETCHER_PARTITION_RESULT_IS_NULL_9CD4434D);
      }

      for (final Pair<IDeviceID, TTimePartitionSlot> slot : batchSlots) {
        final TRegionReplicaSet replicaSet =
            currentDatabase != null
                ? dataPartition.getDataRegionReplicaSetForWriting(
                    slot.left, slot.right, currentDatabase)
                : dataPartition.getDataRegionReplicaSetForWriting(slot.left, slot.right);

        if (replicaSet == null) {
          throw new IllegalStateException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_MISSING_DATA_REGION_REPLICA_SET_FOR_DEVICE_ARG_AT_PARTITION_ARG_18A9E160,
                  slot.left,
                  slot.right));
        }
        replicaSets.add(replicaSet);
      }
    }

    return replicaSets;
  }

  // -------------------------------------------------------------------------
  // Parameter Transformation
  // -------------------------------------------------------------------------

  /** Deduplicates slots per device into RPC query parameters with preserved database scope. */
  private List<DataPartitionQueryParam> toQueryParams(
      final List<Pair<IDeviceID, TTimePartitionSlot>> slots, final String databaseScope) {
    final Map<IDeviceID, Set<TTimePartitionSlot>> deviceToSlotsMap = new HashMap<>();

    for (final Pair<IDeviceID, TTimePartitionSlot> slot : slots) {
      if (slot != null && slot.left != null && slot.right != null) {
        deviceToSlotsMap.computeIfAbsent(slot.left, k -> new HashSet<>()).add(slot.right);
      }
    }

    final List<DataPartitionQueryParam> queryParams = new ArrayList<>(deviceToSlotsMap.size());
    for (final Map.Entry<IDeviceID, Set<TTimePartitionSlot>> entry : deviceToSlotsMap.entrySet()) {
      final DataPartitionQueryParam queryParam =
          new DataPartitionQueryParam(entry.getKey(), new ArrayList<>(entry.getValue()));

      if (databaseScope != null) {
        queryParam.setDatabaseName(databaseScope);
      }
      queryParams.add(queryParam);
    }

    return queryParams;
  }
}
