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
import org.apache.iotdb.db.metadata.rescon.CachedSchemaRegionStatistics;

import java.util.List;

// This class is used for memory control in industry environment.
public class MemManager {

  private final CachedSchemaRegionStatistics regionStatistics;

  private final CachedMNodeSizeEstimator estimator = new CachedMNodeSizeEstimator();

  public MemManager(CachedSchemaRegionStatistics regionStatistics) {
    this.regionStatistics = regionStatistics;
  }

  public void requestPinnedMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    regionStatistics.requestMemory(size);
    regionStatistics.updatePinnedSize(size);
    regionStatistics.updatePinnedNum(1);
  }

  public void upgradeMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    regionStatistics.updatePinnedSize(size);
    regionStatistics.updatePinnedNum(1);
    regionStatistics.updateUnpinnedSize(-size);
    regionStatistics.updateUnpinnedNum(-1);
  }

  public void releasePinnedMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    regionStatistics.updateUnpinnedSize(size);
    regionStatistics.updateUnpinnedNum(1);
    regionStatistics.updatePinnedSize(-size);
    regionStatistics.updatePinnedNum(-1);
  }

  public void releaseMemResource(IMNode node) {
    int size = estimator.estimateSize(node);
    regionStatistics.updateUnpinnedSize(-size);
    regionStatistics.updateUnpinnedNum(-1);
    regionStatistics.releaseMemory(size);
  }

  public void releaseMemResource(List<IMNode> evictedNodes) {
    int size = 0;
    for (IMNode node : evictedNodes) {
      size += estimator.estimateSize(node);
    }
    regionStatistics.updateUnpinnedNum(-evictedNodes.size());
    regionStatistics.updateUnpinnedSize(-size);
    regionStatistics.releaseMemory(size);
  }

  public void updatePinnedSize(int deltaSize) {
    if (deltaSize > 0) {
      regionStatistics.requestMemory(deltaSize);
    } else {
      regionStatistics.releaseMemory(-deltaSize);
    }
    regionStatistics.updatePinnedSize(deltaSize);
  }
}
