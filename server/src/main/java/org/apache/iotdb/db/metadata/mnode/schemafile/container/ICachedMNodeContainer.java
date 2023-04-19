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
package org.apache.iotdb.db.metadata.mnode.schemafile.container;

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.schemafile.ICachedMNode;

import java.util.Iterator;
import java.util.Map;

public interface ICachedMNodeContainer extends IMNodeContainer<ICachedMNode> {

  long getSegmentAddress();

  void setSegmentAddress(long segmentAddress);

  boolean isVolatile();

  boolean isFull();

  boolean isExpelled();

  boolean hasChildInNewChildBuffer(String name);

  boolean hasChildInBuffer(String name);

  Iterator<ICachedMNode> getChildrenIterator();

  Iterator<ICachedMNode> getChildrenBufferIterator();

  Iterator<ICachedMNode> getNewChildBufferIterator();

  Map<String, ICachedMNode> getChildCache();

  Map<String, ICachedMNode> getNewChildBuffer();

  Map<String, ICachedMNode> getUpdatedChildBuffer();

  void loadChildrenFromDisk(Map<String, ICachedMNode> children);

  void addChildToCache(ICachedMNode node);

  void appendMNode(ICachedMNode node);

  void updateMNode(String name);

  void moveMNodeToCache(String name);

  void evictMNode(String name);

  static ICachedMNodeContainer getCachedMNodeContainer(ICachedMNode node) {
    IMNodeContainer<ICachedMNode> container = node.getChildren();
    if (container.equals(CachedMNodeContainer.emptyMNodeContainer())) {
      container = new CachedMNodeContainer();
      node.setChildren(container);
    }
    return (ICachedMNodeContainer) container;
  }

  static ICachedMNodeContainer getBelongedContainer(ICachedMNode node) {
    return (ICachedMNodeContainer) node.getParent().getChildren();
  }
}
