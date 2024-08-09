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

import org.apache.tsfile.read.common.TimeRange;

import java.util.Collections;
import java.util.List;

public class CompactChunkPlan {
  private final TimeRange timeRange;
  private List<CompactPagePlan> pageRecords;
  private final boolean isCompactedByDirectlyFlush;

  public CompactChunkPlan(List<CompactPagePlan> pageRecords) {
    this.timeRange =
        new TimeRange(
            pageRecords.get(0).getTimeRange().getMin(),
            pageRecords.get(pageRecords.size() - 1).getTimeRange().getMax());
    this.pageRecords = pageRecords;
    this.isCompactedByDirectlyFlush = false;
  }

  public CompactChunkPlan(long startTime, long endTime) {
    this.timeRange = new TimeRange(startTime, endTime);
    this.pageRecords = Collections.emptyList();
    this.isCompactedByDirectlyFlush = true;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public List<CompactPagePlan> getPageRecords() {
    return pageRecords;
  }

  public boolean isCompactedByDirectlyFlush() {
    return isCompactedByDirectlyFlush;
  }

  @Override
  public String toString() {
    return "CompactChunkPlan{"
        + "timeRange="
        + timeRange
        + ", pageRecords="
        + pageRecords
        + ", isCompactedByDirectlyFlush="
        + isCompactedByDirectlyFlush
        + '}';
  }
}
