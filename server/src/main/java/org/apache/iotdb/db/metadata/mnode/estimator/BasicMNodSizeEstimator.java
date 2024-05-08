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

package org.apache.iotdb.db.metadata.mnode.estimator;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;

public class BasicMNodSizeEstimator implements IMNodeSizeEstimator {
  /**
   * The basic memory occupied by any MNode object
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>node attributes
   *       <ol>
   *         <li>name reference, name length and name hash code, 8 + 4 + 4 = 16B
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
  protected static final int NODE_BASE_SIZE = 92;

  /**
   * The basic extra memory occupied by an InternalMNode based on MNode occupation
   *
   * <ol>
   *   <li>template reference, 8B
   *   <li>boolean useTemplate, 1B
   *   <li>MNodeContainer reference and basic occupation, 8 + 80B
   * </ol>
   */
  protected static final int INTERNAL_NODE_BASE_SIZE = 97;

  /**
   * The basic extra memory occupied by an StorageGroupMNode based on InternalMNode occupation
   *
   * <ol>
   *   <li>dataTTL, 8B
   *   <li>fullPath length and hashCode, 4 + 4 = 8B
   * </ol>
   */
  protected static final int STORAGE_GROUP_NODE_BASE_SIZE = 16;

  /**
   * The basic extra memory occupied by an EntityMNode based on InternalMNode occupation
   *
   * <ol>
   *   <li>isAligned, 1B
   *   <li>aliasChildren reference, 8B
   * </ol>
   */
  protected static final int ENTITY_NODE_BASE_SIZE = 9;

  /**
   * The basic extra memory occupied by an MeasurementMNode based on MNode occupation
   *
   * <ol>
   *   <li>alias reference, 8B
   *   <li>tagOffset, 8B
   *   <li>estimated schema size, 32B
   * </ol>
   */
  protected static final int MEASUREMENT_NODE_BASE_SIZE = 48;
  // alias length, hashCode and occupation in aliasMap, 4 + 4 + 44 = 52B
  protected static final int ALIAS_BASE_SIZE = 52;

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
