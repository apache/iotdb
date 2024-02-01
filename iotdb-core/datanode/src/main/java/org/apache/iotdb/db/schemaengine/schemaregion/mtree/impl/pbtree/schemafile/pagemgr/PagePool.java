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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PagePool {
  private static final Logger logger = LoggerFactory.getLogger(PagePool.class);
  private final Map<Integer, ISchemaPage> pageInstCache;
  private final Lock cacheLock;
  private final Condition cacheFull;

  private final PageIndexSortBuckets pageIndexBuckets;

  PagePool() {
    this.pageInstCache =
        Collections.synchronizedMap(new LinkedHashMap<>(SchemaFileConfig.PAGE_CACHE_SIZE, 1, true));
    this.pageIndexBuckets = new PageIndexSortBuckets(SchemaFileConfig.SEG_SIZE_LST, pageInstCache);

    this.cacheLock = new ReentrantLock();
    this.cacheFull = this.cacheLock.newCondition();
  }

  /**
   * A rough cache size guardian, all threads passed this entrant check will not be limited with
   * cache size anymore. TODO A better guardian is based on constraint per thread.
   */
  public void cacheGuardian() {
    cacheLock.lock();
    try {
      while (pageInstCache.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
        try {
          // try to evict by LRU
          Iterator<ISchemaPage> iterator = pageInstCache.values().iterator();
          int pageSizeLimit = SchemaFileConfig.PAGE_CACHE_SIZE, size = pageInstCache.size();

          ISchemaPage p;
          while (iterator.hasNext()) {
            p = iterator.next();

            if (size <= pageSizeLimit) {
              break;
            }

            if (p.getRefCnt().get() == 0) {
              iterator.remove();
              size--;
            }
          }

          if (pageInstCache.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
            // wait until another operation finished and released pages
            cacheFull.await();
          }
        } catch (InterruptedException e) {
          logger.warn(
              "Interrupted during page cache eviction. Consider increasing cache size, "
                  + "reducing concurrency, or extending timeout");
        }
      }
    } finally {
      cacheLock.unlock();
    }
  }

  public void put(ISchemaPage page) {
    pageInstCache.put(page.getPageIndex(), page);
  }

  public void lock() {
    cacheLock.lock();
  }

  public void unlock() {
    cacheLock.unlock();
  }

  public ISchemaPage get(int index) {
    return pageInstCache.get(index);
  }

  public ISchemaPage getNearestFitPage(short expectedSize) {
    return pageIndexBuckets.getNearestFitPage(expectedSize, true);
  }

  public void remove(int index) {
    pageInstCache.remove(index);
  }

  public void clear() {
    pageInstCache.clear();
  }

  public void appendBucketIndex(SchemaPageContext cxt) {
    cxt.appendBucketIndex(pageIndexBuckets);
  }

  /** release referents and evict likely useless page if necessary */
  protected void releaseReferent(SchemaPageContext cxt) {
    for (ISchemaPage p : cxt.referredPages.values()) {
      p.decrementAndGetRefCnt();
    }

    if (pageInstCache.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
      cacheLock.lock();
      try {
        for (ISchemaPage p : cxt.referredPages.values()) {
          // unnecessary to evict the page object in context by 2 case:
          //  1. it is held by another thread, e.g., RefCnt != 0
          //  2. it had already been evicted, e.g., pageCache.get(id) != page
          if (p.getRefCnt().get() == 0 && pageInstCache.get(p.getPageIndex()) == p) {
            pageInstCache.remove(p.getPageIndex());
          }
        }

        if (pageInstCache.size() <= SchemaFileConfig.PAGE_CACHE_SIZE) {
          cacheFull.signal();
        }
      } finally {
        cacheLock.unlock();
      }
    }
  }
}
