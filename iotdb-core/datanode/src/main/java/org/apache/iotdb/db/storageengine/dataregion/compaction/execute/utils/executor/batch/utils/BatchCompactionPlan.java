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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;

import org.apache.tsfile.read.common.TimeRange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchCompactionPlan {
  private final List<CompactChunkPlan> compactChunkPlans = new ArrayList<>();
  private final Map<String, Map<TimeRange, ModifiedStatus>> alignedPageModifiedStatusCache =
      new HashMap<>();

  public void recordCompactedChunk(CompactChunkPlan compactChunkPlan) {
    compactChunkPlans.add(compactChunkPlan);
  }

  public CompactChunkPlan getCompactChunkPlan(int i) {
    return compactChunkPlans.get(i);
  }

  public void recordPageModifiedStatus(
      String file, TimeRange timeRange, ModifiedStatus modifiedStatus) {
    alignedPageModifiedStatusCache
        .computeIfAbsent(file, k1 -> new HashMap<>())
        .computeIfAbsent(timeRange, k2 -> modifiedStatus);
  }

  public ModifiedStatus getAlignedPageModifiedStatus(String file, TimeRange timeRange) {
    return alignedPageModifiedStatusCache.getOrDefault(file, Collections.emptyMap()).get(timeRange);
  }

  public int compactedChunkNum() {
    return compactChunkPlans.size();
  }

  public boolean isEmpty() {
    return compactChunkPlans.isEmpty();
  }

  @Override
  public String toString() {
    return compactChunkPlans.toString();
  }
}
