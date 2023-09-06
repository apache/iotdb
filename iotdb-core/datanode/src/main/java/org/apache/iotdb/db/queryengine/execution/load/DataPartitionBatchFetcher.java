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

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataPartitionBatchFetcher {

  private final IPartitionFetcher fetcher;

  public DataPartitionBatchFetcher(IPartitionFetcher fetcher) {
    this.fetcher = fetcher;
  }

  public List<TRegionReplicaSet> queryDataPartition(
      List<Pair<String, TTimePartitionSlot>> slotList) {
    List<TRegionReplicaSet> replicaSets = new ArrayList<>();
    int size = slotList.size();

    for (int i = 0; i < size; i += LoadTsFileScheduler.TRANSMIT_LIMIT) {
      List<Pair<String, TTimePartitionSlot>> subSlotList =
          slotList.subList(i, Math.min(size, i + LoadTsFileScheduler.TRANSMIT_LIMIT));
      DataPartition dataPartition = fetcher.getOrCreateDataPartition(toQueryParam(subSlotList));
      replicaSets.addAll(
          subSlotList.stream()
              .map(pair -> dataPartition.getDataRegionReplicaSetForWriting(pair.left, pair.right))
              .collect(Collectors.toList()));
    }
    return replicaSets;
  }

  private List<DataPartitionQueryParam> toQueryParam(List<Pair<String, TTimePartitionSlot>> slots) {
    return slots.stream()
        .collect(
            Collectors.groupingBy(
                Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toSet())))
        .entrySet()
        .stream()
        .map(
            entry -> new DataPartitionQueryParam(entry.getKey(), new ArrayList<>(entry.getValue())))
        .collect(Collectors.toList());
  }
}
