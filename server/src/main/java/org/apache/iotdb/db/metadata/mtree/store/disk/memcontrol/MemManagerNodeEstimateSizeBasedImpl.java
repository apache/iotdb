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

public class MemManagerNodeEstimateSizeBasedImpl implements IMemManager {

  private static final double THRESHOLD_RATIO = 0.6;

  private MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();

  private long threshold;

  private AtomicLong size = new AtomicLong(0);

  private AtomicLong pinnedSize = new AtomicLong(0);

  private CachedMNodeSizeEstimator estimator = new CachedMNodeSizeEstimator();

  @Override
  public void init() {
    size.getAndSet(0);
    pinnedSize.getAndSet(0);
    threshold = (long) (memoryStatistics.getMemoryCapacity() * THRESHOLD_RATIO);
  }

  @Override
  public boolean isEmpty() {
    return size.get() == 0;
  }

  @Override
  public boolean isExceedThreshold() {
    return !isEmpty() && memoryStatistics.getMemoryUsage() > threshold;
  }

  @Override
  public boolean isExceedCapacity() {
    return memoryStatistics.isExceedCapacity();
  }

  @Override
  public void requestPinnedMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    memoryStatistics.requestMemory(size);
    pinnedSize.getAndUpdate(v -> v += size);
  }

  @Override
  public void upgradeMemResource(IMNode node) {
    pinnedSize.getAndUpdate(v -> v += estimator.estimateSize(node));
    size.getAndUpdate(v -> v -= estimator.estimateSize(node));
  }

  @Override
  public void releasePinnedMemResource(IMNode node) {
    size.getAndUpdate(v -> v += estimator.estimateSize(node));
    pinnedSize.getAndUpdate(v -> v -= estimator.estimateSize(node));
  }

  @Override
  public void releaseMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    this.size.getAndUpdate(v -> v -= size);
    memoryStatistics.releaseMemory(size);
  }

  @Override
  public void releaseMemResource(List<IMNode> evictedNodes) {
    for (IMNode node : evictedNodes) {
      releaseMemResource(node);
    }
  }

  @Override
  public void clear() {
    size.getAndSet(0);
    pinnedSize.getAndSet(0);
  }

  @Override
  public double getPinnedSize() {
    return pinnedSize.get();
  }

  @Override
  public double getCachedSize() {
    return size.get();
  }
}
