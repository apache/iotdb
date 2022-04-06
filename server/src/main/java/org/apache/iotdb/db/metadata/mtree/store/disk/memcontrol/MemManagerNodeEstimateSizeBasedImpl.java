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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MemManagerNodeEstimateSizeBasedImpl implements IMemManager {

  private long capacity;

  private AtomicLong size = new AtomicLong(0);

  private AtomicLong pinnedSize = new AtomicLong(0);

  private MNodeSizeEstimator estimator = new MNodeSizeEstimator();

  @Override
  public void init() {
    capacity = IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchema();
  }

  @Override
  public boolean isEmpty() {
    return size.get() == 0;
  }

  @Override
  public boolean isExceedThreshold() {
    return !isEmpty() && size.get() + pinnedSize.get() > capacity * 0.6;
  }

  @Override
  public boolean isExceedCapacity() {
    return size.get() + pinnedSize.get() > capacity;
  }

  @Override
  public void requestPinnedMemResource(IMNode node) {
    pinnedSize.getAndUpdate(v -> v += estimator.calculateSize(node));
  }

  @Override
  public void upgradeMemResource(IMNode node) {
    pinnedSize.getAndUpdate(v -> v += estimator.calculateSize(node));
    size.getAndUpdate(v -> v -= estimator.calculateSize(node));
  }

  @Override
  public void releasePinnedMemResource(IMNode node) {
    size.getAndUpdate(v -> v += estimator.calculateSize(node));
    pinnedSize.getAndUpdate(v -> v -= estimator.calculateSize(node));
  }

  @Override
  public void releaseMemResource(IMNode node) {
    size.getAndUpdate(v -> v -= estimator.calculateSize(node));
  }

  @Override
  public void releaseMemResource(List<IMNode> evictedNodes) {
    for (IMNode node : evictedNodes) {
      size.getAndUpdate(value -> value -= estimator.calculateSize(node));
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

  private static class MNodeSizeEstimator {

    /**
     * The basic memory occupied by any MNode object
     *
     * <ol>
     *   <li>object header, 8B
     *   <li>node attributes
     *       <ol>
     *         <li>name reference, name length and name hash code, 8B
     *         <li>parent reference, 8B
     *         <li>fullPath reference, 8B
     *         <li>cacheEntry reference, 8B
     *       </ol>
     *   <li>MapEntry in parent
     *       <ol>
     *         <li>key reference, 8B
     *         <li>value reference, 8B
     *         <li>entry size, see ConcurrentHashMap.Node, 28
     *       </ol>
     * </ol>
     */
    private static final int NODE_BASE_SIZE = 84;

    /**
     * The basic extra memory occupied by an InternalMNode based on MNode occupation
     *
     * <ol>
     *   <li>template reference, 8B
     *   <li>boolean useTemplate, 1B
     *   <li>MNodeContainer occupation
     *       <ol>
     *         <li>MNodeContainer reference, 8B
     *         <li>address, 8B
     *         <li>three map reference (1 cache and 2 buffer), 8 * 3 = 24B
     *         <li>estimate occupation of map implementation, 80 * 3 = 240B
     *       </ol>
     * </ol>
     */
    private static final int INTERNAL_NODE_BASE_SIZE = 289;

    /**
     * The basic extra memory occupied by an StorageGroupMNode based on InternalMNode occupation
     * dataTTL, 8B
     */
    private static final int STORAGE_GROUP_NODE_BASE_SIZE = 8;

    /**
     * The basic extra memory occupied by an EntityMNode based on InternalMNode occupation
     *
     * <ol>
     *   <li>isAligned, 1B
     *   <li>aliasChildren reference, 8B
     *   <li>lastCacheMap, 8B
     * </ol>
     */
    private static final int ENTITY_NODE_BASE_SIZE = 17;

    /**
     * The basic extra memory occupied by an MeasurementMNode based on MNode occupation
     *
     * <ol>
     *   <li>alias reference, 8B
     *   <li>tagOffset, 8B
     *   <li>estimated schema size, 32B
     *   <li>lastCache, 8B
     *   <li>trigger, 8B
     * </ol>
     */
    private static final int MEASUREMENT_NODE_BASE_SIZE = 64;
    // alias occupation in aliasMap, 44B
    private static final int ALIAS_OCCUPATION = 44;

    int calculateSize(IMNode node) {
      int size = NODE_BASE_SIZE + getStringSize(node.getName());
      if (node.isMeasurement()) {
        size += MEASUREMENT_NODE_BASE_SIZE;
        IMeasurementMNode measurementMNode = node.getAsMeasurementMNode();
        if (measurementMNode.getAlias() != null) {
          size += ALIAS_OCCUPATION + getStringSize(measurementMNode.getAlias());
        }
      } else {
        size += INTERNAL_NODE_BASE_SIZE;
        if (node.isStorageGroup()) {
          size += STORAGE_GROUP_NODE_BASE_SIZE;
          size += getStringSize(node.getAsStorageGroupMNode().getFullPath());
        }

        if (node.isEntity()) {
          size += ENTITY_NODE_BASE_SIZE;
        }
      }

      return size;
    }

    private int getStringSize(String string) {
      // string value size + hashcode + length
      return string.length() + 8;
    }
  }
}
