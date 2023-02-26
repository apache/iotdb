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
package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

import org.apache.iotdb.db.exception.metadata.cache.MNodeNotCachedException;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotPinnedException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;

import java.util.List;

public interface ICacheManager {

  void initRootStatus(IMNode root);

  void updateCacheStatusAfterMemoryRead(IMNode node) throws MNodeNotCachedException;

  void updateCacheStatusAfterDiskRead(IMNode node);

  void updateCacheStatusAfterAppend(IMNode node);

  void updateCacheStatusAfterUpdate(IMNode node);

  void updateCacheStatusAfterPersist(IMNode node);

  IStorageGroupMNode collectUpdatedStorageGroupMNodes();

  List<IMNode> collectVolatileMNodes();

  void remove(IMNode node);

  boolean evict();

  void pinMNode(IMNode node) throws MNodeNotPinnedException;

  boolean unPinMNode(IMNode node);

  long getBufferNodeNum();

  long getCacheNodeNum();

  void clear(IMNode root);
}
