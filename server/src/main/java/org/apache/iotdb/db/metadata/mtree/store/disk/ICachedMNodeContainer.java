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
package org.apache.iotdb.db.metadata.mtree.store.disk;

import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.container.MNodeContainers;
import org.apache.iotdb.db.metadata.newnode.ICacheMNode;

import java.util.Iterator;
import java.util.Map;

public interface ICachedMNodeContainer extends IMNodeContainer<ICacheMNode> {

  long getSegmentAddress();

  void setSegmentAddress(long segmentAddress);

  boolean isVolatile();

  boolean isFull();

  boolean isExpelled();

  boolean hasChildInNewChildBuffer(String name);

  boolean hasChildInBuffer(String name);

  Iterator<ICacheMNode> getChildrenIterator();

  Iterator<ICacheMNode> getChildrenBufferIterator();

  Iterator<ICacheMNode> getNewChildBufferIterator();

  Map<String, ICacheMNode> getChildCache();

  Map<String, ICacheMNode> getNewChildBuffer();

  Map<String, ICacheMNode> getUpdatedChildBuffer();

  void loadChildrenFromDisk(Map<String, ICacheMNode> children);

  void addChildToCache(ICacheMNode node);

  void appendMNode(ICacheMNode node);

  void updateMNode(String name);

  void moveMNodeToCache(String name);

  void evictMNode(String name);

  static ICachedMNodeContainer getCachedMNodeContainer(ICacheMNode node) {
    IMNodeContainer<ICacheMNode> container = node.getChildren();
    if (container.equals(MNodeContainers.emptyMNodeContainer())) {
      container = new CachedMNodeContainer();
      node.setChildren(container);
    }
    return (ICachedMNodeContainer) container;
  }

  static ICachedMNodeContainer getBelongedContainer(ICacheMNode node) {
    return (ICachedMNodeContainer) node.getParent().getChildren();
  }
}
