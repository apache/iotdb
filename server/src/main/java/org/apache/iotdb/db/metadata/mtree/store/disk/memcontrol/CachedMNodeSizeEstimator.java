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
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.estimator.BasicMNodSizeEstimator;

public class CachedMNodeSizeEstimator extends BasicMNodSizeEstimator {

  /** Estimated size of CacheEntry */
  private static final int CACHE_ENTRY_SIZE = 40;

  /**
   * Estimated delta size of CachedMNodeContainer compared with MNodeContainerMapImpl
   *
   * <ol>
   *   <li>address, 8B
   *   <li>three map reference (1 cache and 2 buffer), 8 * 3 = 24B
   *   <li>estimate occupation of map implementation, minus the basic container occupation, 80 * 3 -
   *       80 = 160B
   * </ol>
   */
  private static final int CACHED_MNODE_CONTAINER_SIZE_DELTA = 192;

  private static final int NODE_BASE_SIZE =
      BasicMNodSizeEstimator.NODE_BASE_SIZE + CACHE_ENTRY_SIZE;

  private static final int INTERNAL_NODE_BASE_SIZE =
      BasicMNodSizeEstimator.INTERNAL_NODE_BASE_SIZE + CACHED_MNODE_CONTAINER_SIZE_DELTA;

  @Override
  public int estimateSize(IMNode node) {
    int size = NODE_BASE_SIZE + node.getName().length();
    if (node.isMeasurement()) {
      size += MEASUREMENT_NODE_BASE_SIZE;
      IMeasurementMNode measurementMNode = node.getAsMeasurementMNode();
      if (measurementMNode.getAlias() != null) {
        size += ALIAS_BASE_SIZE + measurementMNode.getAlias().length();
      }
    } else {
      size += INTERNAL_NODE_BASE_SIZE;
      if (node.isStorageGroup()) {
        size += STORAGE_GROUP_NODE_BASE_SIZE;
        size += node.getAsStorageGroupMNode().getFullPath().length();
      }

      if (node.isEntity()) {
        size += ENTITY_NODE_BASE_SIZE;
      }
    }

    return size;
  }
}
