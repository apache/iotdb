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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.cache;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

/**
 * NodeCache is used to implement specific cache eviction strategy and help quickly locate the
 * target nodes tobe evicted. This interface defines the behaviours that an implementation shall
 * meet.
 */
public interface INodeCache {

  long getCacheNodeNum();

  void initCacheEntryForNode(ICachedMNode node);

  void updateCacheStatusAfterAccess(CacheEntry cacheEntry);

  void addToNodeCache(CacheEntry cacheEntry, ICachedMNode node);

  void removeFromNodeCache(CacheEntry cacheEntry);

  ICachedMNode getPotentialNodeTobeEvicted();

  void clear();
}
