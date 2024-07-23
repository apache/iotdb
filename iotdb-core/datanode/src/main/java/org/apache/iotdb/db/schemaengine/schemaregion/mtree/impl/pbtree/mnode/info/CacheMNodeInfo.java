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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.info;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.BasicMNodeInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.cache.CacheEntry;

@SuppressWarnings("java:S3077")
public class CacheMNodeInfo extends BasicMNodeInfo {

  private volatile CacheEntry cacheEntry;

  private volatile LockEntry lockEntry;

  public CacheMNodeInfo(String name) {
    super(name);
  }

  public CacheEntry getCacheEntry() {
    return cacheEntry;
  }

  public void setCacheEntry(CacheEntry cacheEntry) {
    this.cacheEntry = cacheEntry;
  }

  public LockEntry getLockEntry() {
    return lockEntry;
  }

  public void setLock(LockEntry lockEntry) {
    this.lockEntry = lockEntry;
  }

  @Override
  public int estimateSize() {
    // Estimated size of CacheEntry = 40
    return super.estimateSize() + 40;
  }
}
