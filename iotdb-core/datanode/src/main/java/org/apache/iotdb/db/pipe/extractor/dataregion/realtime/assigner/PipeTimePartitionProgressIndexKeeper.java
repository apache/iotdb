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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;

import org.apache.tsfile.utils.Pair;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTimePartitionProgressIndexKeeper {

  // data region id -> (time partition id, <max progress index, is valid>)
  private final Map<String, Map<Long, Pair<ProgressIndex, Boolean>>> progressIndexKeeper =
      new ConcurrentHashMap<>();

  public synchronized void updateProgressIndex(
      final String dataRegionId, final long timePartitionId, final ProgressIndex progressIndex) {
    progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .compute(
            timePartitionId,
            (k, v) -> {
              if (v == null) {
                return new Pair<>(progressIndex.deepCopy(), true);
              }
              return new Pair<>(
                  v.getLeft().updateToMinimumEqualOrIsAfterProgressIndex(progressIndex), true);
            });
  }

  public synchronized void eliminateProgressIndex(
      final String dataRegionId, final long timePartitionId, final ProgressIndex progressIndex) {
    progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .compute(
            timePartitionId,
            (k, v) -> {
              if (v == null) {
                return null;
              }
              if (v.getRight() && v.getLeft().equals(progressIndex)) {
                return new Pair<>(v.getLeft(), false);
              }
              return v;
            });
  }

  public synchronized boolean isProgressIndexAfterOrEquals(
      final String dataRegionId, final long timePartitionId, final ProgressIndex progressIndex) {
    return progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey() != timePartitionId)
        .map(Entry::getValue)
        .filter(pair -> pair.right)
        .map(Pair::getLeft)
        .anyMatch(index -> progressIndex.isAfter(index) || progressIndex.equals(index));
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeTimePartitionProgressIndexKeeperHolder {

    private static final PipeTimePartitionProgressIndexKeeper INSTANCE =
        new PipeTimePartitionProgressIndexKeeper();

    private PipeTimePartitionProgressIndexKeeperHolder() {
      // empty constructor
    }
  }

  public static PipeTimePartitionProgressIndexKeeper getInstance() {
    return PipeTimePartitionProgressIndexKeeper.PipeTimePartitionProgressIndexKeeperHolder.INSTANCE;
  }

  private PipeTimePartitionProgressIndexKeeper() {
    // empty constructor
  }
}
