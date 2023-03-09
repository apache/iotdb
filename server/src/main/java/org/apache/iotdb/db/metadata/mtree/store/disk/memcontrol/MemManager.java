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

import org.apache.iotdb.db.metadata.mnode.schemafile.ICacheMNode;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaRegionStatistics;

import java.util.List;

// This class is used for memory control in industry environment.
public class MemManager {

  private final CachedSchemaRegionStatistics regionStatistics;

  public MemManager(CachedSchemaRegionStatistics regionStatistics) {
    this.regionStatistics = regionStatistics;
  }

  public void requestPinnedMemResource(ICacheMNode node) {
    int size = node.estimateSize();
    regionStatistics.requestMemory(size);
    regionStatistics.updatePinnedMemorySize(size);
    regionStatistics.updatePinnedMNodeNum(1);
  }

  public void upgradeMemResource(ICacheMNode node) {
    int size = node.estimateSize();
    regionStatistics.updatePinnedMemorySize(size);
    regionStatistics.updatePinnedMNodeNum(1);
    regionStatistics.updateUnpinnedMemorySize(-size);
    regionStatistics.updateUnpinnedMNodeNum(-1);
  }

  public void releasePinnedMemResource(ICacheMNode node) {
    int size = node.estimateSize();
    regionStatistics.updateUnpinnedMemorySize(size);
    regionStatistics.updateUnpinnedMNodeNum(1);
    regionStatistics.updatePinnedMemorySize(-size);
    regionStatistics.updatePinnedMNodeNum(-1);
  }

  public void releaseMemResource(ICacheMNode node) {
    int size = node.estimateSize();
    regionStatistics.updateUnpinnedMemorySize(-size);
    regionStatistics.updateUnpinnedMNodeNum(-1);
    regionStatistics.releaseMemory(size);
  }

  public void releaseMemResource(List<ICacheMNode> evictedNodes) {
    int size = 0;
    for (ICacheMNode node : evictedNodes) {
      size += node.estimateSize();
    }
    regionStatistics.updateUnpinnedMNodeNum(-evictedNodes.size());
    regionStatistics.updateUnpinnedMemorySize(-size);
    regionStatistics.releaseMemory(size);
  }

  public void updatePinnedSize(int deltaSize) {
    if (deltaSize > 0) {
      regionStatistics.requestMemory(deltaSize);
    } else {
      regionStatistics.releaseMemory(-deltaSize);
    }
    regionStatistics.updatePinnedMemorySize(deltaSize);
  }
}
