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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.page;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr.PagePool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to manage the lifecycle of pages in memory. There are two kinds of pages in
 * memory: cached pages and volatile pages. There is a recommended capacity for both types of pages.
 * For example, total capacity is 5, recommended capacity for cached pages is 3, recommended
 * capacity for volatile pages is 2. When system requests a cached page:
 *
 * <ol>
 *   <li>If the current number of cached pages is less than 3, then we can allocate a new cached
 *       page. A volatile node needs to be flushed if the total capacity is exceeded.
 *   <li>If the current number of cached pages is greater than or equal to 3 and the total capacity
 *       is full, a cached page needs to be evicted for allocation.
 *   <li>If the current number of cache pages is greater than or equal to 3 and the total capacity
 *       is not full, you can borrow the space of a volatile page and allocate a cache page
 *       directly.
 * </ol>
 */
public class PageLifecycleManager {
  /* configuration */
  private int capacity;
  private int cachedCapacity;
  private int volatileCapacity;

  /* data */
  // TODO(PBTree-Page-Concurrent): 这里也可以不是 PagePool，改为 IPageManager 等其他数据结构
  private final Map<Integer, PagePool> regionToPagePoolMap = new ConcurrentHashMap<>();
  private final IPageContainer cachedContainer = new FIFOPageContainer();
  private final IPageContainer volatileContainer = new FIFOPageContainer();

  public void loadConfiguration(int capacity, int cachedCapacity, int volatileCapacity) {
    this.capacity = capacity;
    this.cachedCapacity = cachedCapacity;
    this.volatileCapacity = volatileCapacity;
  }

  public void registerPagePool(int regionId, PagePool pagePool) {
    regionToPagePoolMap.put(regionId, pagePool);
  }

  public void removePagePool(int regionId) {
    regionToPagePoolMap.remove(regionId);
  }

  // TODO(PBTree-Page-Concurrent): 有新cached页时，调一次这个方法
  public void putCachedPage(int regionId, ISchemaPage schemaPage) {
    cachedContainer.put(regionId, schemaPage);
    checkCapacity();
  }

  // TODO(PBTree-Page-Concurrent): 有新的脏页时，调一次这个方法
  public void putVolatilePage(int regionId, ISchemaPage schemaPage) {
    volatileContainer.put(regionId, schemaPage);
    checkCapacity();
  }

  // TODO(PBTree-Page-Concurrent): 读写结束以后，调一次这个方法
  public void checkCapacity() {
    if (cachedContainer.size() + volatileContainer.size() > capacity) {
      if (cachedContainer.size() > cachedCapacity) {
        // try to evict cached pages
        cachedContainer.iterateToRemove(
            i -> regionToPagePoolMap.get(i.left).evict(i.right), cachedCapacity);
      } else {
        // try to flush volatile pages
        volatileContainer.iterateToRemove(
            i -> regionToPagePoolMap.get(i.left).flush(i.right), volatileCapacity);
      }
    }
  }

  @TestOnly
  public IPageContainer getCachedContainer() {
    return cachedContainer;
  }

  @TestOnly
  public IPageContainer getVolatileContainer() {
    return volatileContainer;
  }

  @TestOnly
  public void clear() {
    regionToPagePoolMap.clear();
    cachedContainer.clear();
    volatileContainer.clear();
  }

  // singleton
  private PageLifecycleManager() {}

  private static class PageLifecycleManagerHolder {
    private static final PageLifecycleManager INSTANCE = new PageLifecycleManager();
  }

  public static PageLifecycleManager getInstance() {
    return PageLifecycleManager.PageLifecycleManagerHolder.INSTANCE;
  }
}
