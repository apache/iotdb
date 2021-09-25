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
package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.List;

/** this interface provides operations on cache */
public interface ICacheStrategy {

  /** get the size of the current cache */
  int getSize();

  /** used for mnode object occupation statistic to decide whether this mnode could be evict */
  void lockMNode(IMNode mNode);

  void unlockMNode(IMNode mNode);

  /** change the mnode's position in cache */
  void applyChange(IMNode mNode);

  /**
   * change the mnode's status in cache if a mnode in cache is modified, it will be collected when
   * eviction is triggered and need to be persisted
   */
  void setModified(IMNode mNode, boolean modified);

  /** remove a mnode from cache, so as its subtree */
  void remove(IMNode mNode);

  /**
   * evict a mnode and remove its subtree from the cache and collect the modified mnodes in cache
   * the evicted one will be the first one of the returned collection and the rest of the returned
   * collection need to be persisted
   */
  List<IMNode> evict();

  List<IMNode> collectModified(IMNode mNode);

  void clear();
}
