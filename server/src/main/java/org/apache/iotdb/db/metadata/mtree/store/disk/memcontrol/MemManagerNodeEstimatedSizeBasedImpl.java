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

package org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.rescon.MemoryStatistics;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MemManagerNodeEstimatedSizeBasedImpl implements IMemManager {

  private static final double RELEASE_THRESHOLD_RATIO = 0.6;
  private static final double FLUSH_THRESHOLD_RATION = 0.75;

  private MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();

  private long releaseThreshold;
  private long flushThreshold;

  private AtomicLong size = new AtomicLong(0);

  private AtomicLong pinnedSize = new AtomicLong(0);

  private CachedMNodeSizeEstimator estimator = new CachedMNodeSizeEstimator();

  @Override
  public void init() {
    size.getAndSet(0);
    pinnedSize.getAndSet(0);
    releaseThreshold = (long) (memoryStatistics.getMemoryCapacity() * RELEASE_THRESHOLD_RATIO);
    flushThreshold = (long) (memoryStatistics.getMemoryCapacity() * FLUSH_THRESHOLD_RATION);
  }

  @Override
  public boolean isEmpty() {
    return size.get() == 0;
  }

  @Override
  public boolean isExceedReleaseThreshold() {
    return !isEmpty() && memoryStatistics.getMemoryUsage() > releaseThreshold;
  }

  @Override
  public boolean isExceedFlushThreshold() {
    return !isEmpty() && memoryStatistics.getMemoryUsage() > flushThreshold;
  }

  @Override
  public void requestPinnedMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    memoryStatistics.requestMemory(size);
    pinnedSize.getAndUpdate(v -> v += size);
  }

  @Override
  public void upgradeMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    pinnedSize.getAndUpdate(v -> v += size);
    this.size.getAndUpdate(v -> v -= size);
  }

  @Override
  public void releasePinnedMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    this.size.getAndUpdate(v -> v += size);
    pinnedSize.getAndUpdate(v -> v -= size);
  }

  @Override
  public void releaseMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    this.size.getAndUpdate(v -> v -= size);
    memoryStatistics.releaseMemory(size);
  }

  @Override
  public void releaseMemResource(List<IMNode> evictedNodes) {
    int size = 0;
    for (IMNode node : evictedNodes) {
      size += estimator.estimateSize(node);
    }
    int finalSize = size;
    this.size.getAndUpdate(v -> v -= finalSize);
    memoryStatistics.releaseMemory(size);
  }

  @Override
  public void updatePinnedSize(int deltaSize) {
    if (deltaSize > 0) {
      memoryStatistics.requestMemory(deltaSize);
    } else {
      memoryStatistics.releaseMemory(deltaSize);
    }
    pinnedSize.getAndUpdate(v -> v += deltaSize);
  }

  @Override
  public void clear() {
    size.getAndSet(0);
    pinnedSize.getAndSet(0);
  }

  @Override
  public long getPinnedSize() {
    return pinnedSize.get();
  }

  @Override
  public long getCachedSize() {
    return size.get();
  }
}
