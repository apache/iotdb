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

// This class is used for memory control in industry environment.
public class MemManagerNodeEstimatedSizeBasedImpl implements IMemManager {

  private static final double RELEASE_THRESHOLD_RATIO = 0.6;
  private static final double FLUSH_THRESHOLD_RATION = 0.75;

  private final MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();

  private long releaseThreshold;
  private long flushThreshold;

  // memory size
  private final AtomicLong unpinnedSize = new AtomicLong(0);

  private final AtomicLong pinnedSize = new AtomicLong(0);

  private final CachedMNodeSizeEstimator estimator = new CachedMNodeSizeEstimator();

  @Override
  public void init() {
    unpinnedSize.getAndSet(0);
    pinnedSize.getAndSet(0);
    releaseThreshold = (long) (memoryStatistics.getMemoryCapacity() * RELEASE_THRESHOLD_RATIO);
    flushThreshold = (long) (memoryStatistics.getMemoryCapacity() * FLUSH_THRESHOLD_RATION);
  }

  @Override
  public boolean isEmpty() {
    return unpinnedSize.get() == 0;
  }

  @Override
  public boolean isExceedReleaseThreshold() {
    return memoryStatistics.getMemoryUsage() > releaseThreshold;
  }

  @Override
  public boolean isExceedFlushThreshold() {
    return memoryStatistics.getMemoryUsage() > flushThreshold;
  }

  @Override
  public void requestPinnedMemResource(IMNode node, int schemaRegionId) {
    int size = estimator.estimateSize(node);
    memoryStatistics.requestMemory(size, schemaRegionId);
    pinnedSize.getAndUpdate(v -> v += size);
  }

  @Override
  public void upgradeMemResource(IMNode node, int schemaRegionId) {
    int size = estimator.estimateSize(node);
    pinnedSize.getAndUpdate(v -> v += size);
    this.unpinnedSize.getAndUpdate(v -> v -= size);
  }

  @Override
  public void releasePinnedMemResource(IMNode node, int schemaRegionId) {
    int size = estimator.estimateSize(node);
    this.unpinnedSize.getAndUpdate(v -> v += size);
    pinnedSize.getAndUpdate(v -> v -= size);
  }

  @Override
  public void releaseMemResource(IMNode node, int schemaRegionId) {
    int size = estimator.estimateSize(node);
    this.unpinnedSize.getAndUpdate(v -> v -= size);
    memoryStatistics.releaseMemory(size, schemaRegionId);
  }

  @Override
  public void releaseMemResource(List<IMNode> evictedNodes, int schemaRegionId) {
    int size = 0;
    for (IMNode node : evictedNodes) {
      size += estimator.estimateSize(node);
    }
    int finalSize = size;
    this.unpinnedSize.getAndUpdate(v -> v -= finalSize);
    memoryStatistics.releaseMemory(size, schemaRegionId);
  }

  @Override
  public void updatePinnedSize(int deltaSize, int schemaRegionId) {
    if (deltaSize > 0) {
      memoryStatistics.requestMemory(deltaSize, schemaRegionId);
    } else {
      memoryStatistics.releaseMemory(-deltaSize, schemaRegionId);
    }
    pinnedSize.getAndUpdate(v -> v += deltaSize);
  }

  @Override
  public void initSchemaRegion(int schemaRegionId) {
    memoryStatistics.initSchemaRegion(schemaRegionId);
  }

  @Override
  public void clearSchemaRegion(int schemaRegionId) {
    memoryStatistics.clearSchemaRegion(schemaRegionId);
  }

  @Override
  public void clear() {
    unpinnedSize.getAndSet(0);
    pinnedSize.getAndSet(0);
  }

  @Override
  public long getPinnedSize() {
    return pinnedSize.get();
  }

  @Override
  public long getCachedSize() {
    return unpinnedSize.get();
  }
}
