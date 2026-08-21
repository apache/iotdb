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
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch partition fetcher for LOAD. It wraps the query-engine {@link IPartitionFetcher} and hides
 * two details from the rest of the pipeline:
 *
 * <ul>
 *   <li><b>Transmit limit.</b> The requested (device, time-partition) pairs are split into batches
 *       of at most {@code TTimePartitionSlotTransmitLimit} entries; each batch is resolved with one
 *       {@code getOrCreateDataPartition} call.
 *   <li><b>Database hint.</b> {@link #setDatabase(String)} enables the explicit database lookup
 *       used by table-model loads and pipe-generated tree-model loads.
 * </ul>
 *
 * {@link #queryDataPartition(List, String)} returns one {@link TRegionReplicaSet} per input pair,
 * in the same order, which {@link DataPartitionRouter} maps back onto chunks.
 */
class DataPartitionBatchFetcher {

  private static final int TRANSMIT_LIMIT =
      CommonDescriptor.getInstance().getConfig().getTTimePartitionSlotTransmitLimit();

  private final IPartitionFetcher fetcher;
  private String database;

  DataPartitionBatchFetcher(IPartitionFetcher fetcher) {
    this.fetcher = fetcher;
  }

  void setDatabase(String database) {
    this.database = database;
  }

  List<TRegionReplicaSet> queryDataPartition(
      List<Pair<IDeviceID, TTimePartitionSlot>> slotList, String userName) {
    List<TRegionReplicaSet> replicaSets = new ArrayList<>(slotList.size());
    int size = slotList.size();

    for (int i = 0; i < size; i += TRANSMIT_LIMIT) {
      List<Pair<IDeviceID, TTimePartitionSlot>> subSlotList =
          slotList.subList(i, Math.min(size, i + TRANSMIT_LIMIT));
      DataPartition dataPartition =
          fetcher.getOrCreateDataPartition(toQueryParam(subSlotList), userName);
      for (final Pair<IDeviceID, TTimePartitionSlot> pair : subSlotList) {
        // database is an explicit database hint for table-model loads and
        // pipe-generated tree-model loads.
        replicaSets.add(
            database != null
                ? dataPartition.getDataRegionReplicaSetForWriting(pair.left, pair.right, database)
                : dataPartition.getDataRegionReplicaSetForWriting(pair.left, pair.right));
      }
    }
    return replicaSets;
  }

  private List<DataPartitionQueryParam> toQueryParam(
      List<Pair<IDeviceID, TTimePartitionSlot>> slots) {
    final Map<IDeviceID, Set<TTimePartitionSlot>> device2TimePartitionSlots = new HashMap<>();
    for (final Pair<IDeviceID, TTimePartitionSlot> slot : slots) {
      device2TimePartitionSlots.computeIfAbsent(slot.left, key -> new HashSet<>()).add(slot.right);
    }

    final List<DataPartitionQueryParam> queryParams =
        new ArrayList<>(device2TimePartitionSlots.size());
    for (final Map.Entry<IDeviceID, Set<TTimePartitionSlot>> entry :
        device2TimePartitionSlots.entrySet()) {
      final DataPartitionQueryParam queryParam =
          new DataPartitionQueryParam(entry.getKey(), new ArrayList<>(entry.getValue()));
      // database is an explicit database hint for table-model loads and
      // pipe-generated tree-model loads.
      if (database != null) {
        queryParam.setDatabaseName(database);
      }
      queryParams.add(queryParam);
    }
    return queryParams;
  }
}
