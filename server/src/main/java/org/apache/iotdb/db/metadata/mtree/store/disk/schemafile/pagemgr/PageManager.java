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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.pagemgr;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.FILE_HEADER_SIZE;

@SuppressWarnings("Duplicates")
public abstract class PageManager implements IPageManager{
  protected final Map<Integer, ISchemaPage> pageInstCache;
  protected final Map<Integer, ISchemaPage> dirtyPages;

  protected final ReentrantLock evictLock;
  protected final PageLocks pageLocks;

  protected final AtomicInteger lastPageIndex;

  private final FileChannel channel;

  PageManager(FileChannel channel) {
    pageInstCache =
        Collections.synchronizedMap(new LinkedHashMap<>(SchemaPage.PAGE_CACHE_SIZE, 1, true));
    dirtyPages = new ConcurrentHashMap<>();
    evictLock = new ReentrantLock();
    pageLocks = new PageLocks();
    lastPageIndex = new AtomicInteger(0);
    this.channel = channel;
  }

  @Override
  public ISchemaPage getPageInstance(int pageIdx) throws IOException, MetadataException {
    if (pageIdx > lastPageIndex.get()) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    pageLocks.readLock(pageIdx);
    try {
      if (dirtyPages.containsKey(pageIdx)) {
        return dirtyPages.get(pageIdx);
      }

      if (pageInstCache.containsKey(pageIdx)) {
        return pageInstCache.get(pageIdx);
      }
    } finally {
      pageLocks.readUnlock(pageIdx);
    }

    try {
      pageLocks.writeLock(pageIdx);

      ByteBuffer newBuf = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);

      loadFromFile(newBuf, pageIdx);
      return addPageToCache(pageIdx, ISchemaPage.loadSchemaPage(newBuf));
    } finally {
      pageLocks.writeUnlock(pageIdx);
    }
  }

  private int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException {
    dst.clear();
    return channel.read(dst, getPageAddress(pageIndex));
  }

  @Override
  public void flushDirtyPages() throws IOException {
    for (ISchemaPage page : dirtyPages.values()) {
      page.flushPageToChannel(channel);
    }
    dirtyPages.clear();
  }

  @Override
  public ISchemaPage getMinApplSegmentedPageInMem(short size) {
    for (Map.Entry<Integer, ISchemaPage> entry : dirtyPages.entrySet()) {
      if (entry.getValue().getAsSegmentedPage() != null
          && entry.getValue().isCapableForSize(size)) {
        return dirtyPages.get(entry.getKey());
      }
    }

    for (Map.Entry<Integer, ISchemaPage> entry : pageInstCache.entrySet()) {
      if (entry.getValue().getAsSegmentedPage() != null
          && entry.getValue().isCapableForSize(size)) {
        dirtyPages.putIfAbsent(entry.getKey(), entry.getValue());
        return pageInstCache.get(entry.getKey());
      }
    }
    return allocateNewSegmentedPage();
  }

  private synchronized ISchemaPage allocateNewSegmentedPage() {
    lastPageIndex.incrementAndGet();
    ISchemaPage newPage =
        ISchemaPage.initSegmentedPage(ByteBuffer.allocate(SchemaPage.PAGE_LENGTH), lastPageIndex.get());
    dirtyPages.put(newPage.getPageIndex(), newPage);
    return addPageToCache(newPage.getPageIndex(), newPage);
  }

  private ISchemaPage addPageToCache(int pageIndex, ISchemaPage page) {
    pageInstCache.put(pageIndex, page);
    // only one thread evicts and flushes pages
    if (evictLock.tryLock()) {
      try {
        if (pageInstCache.size() > SchemaPage.PAGE_CACHE_SIZE) {
          int removeCnt =
              (int) (0.2 * pageInstCache.size()) > 0 ? (int) (0.2 * pageInstCache.size()) : 1;
          List<Integer> rmvIds = new ArrayList<>(pageInstCache.keySet()).subList(0, removeCnt);

          for (Integer id : rmvIds) {
            // dirty pages only flushed from dirtyPages
            if (pageLocks.findLock(id).writeLock().tryLock()) {
              try {
                pageInstCache.remove(id);
              } finally {
                pageLocks.findLock(id).writeLock().unlock();
              }
            }
          }
        }
      } finally {
        evictLock.unlock();
      }
    }
    return page;
  }

  public static long getPageAddress(int pageIndex) {
    return (SchemaPage.PAGE_INDEX_MASK & pageIndex) * SchemaPage.PAGE_LENGTH + FILE_HEADER_SIZE;
  }

  private static class PageLocks {
    /**
     * number of reentrant read write lock. Notice that this number should be a prime number for
     * uniform hash
     */
    private static final int NUM_OF_LOCKS = 1039;

    /** locks array */
    private ReentrantReadWriteLock[] locks;

    protected PageLocks() {
      locks = new ReentrantReadWriteLock[NUM_OF_LOCKS];
      for (int i = 0; i < NUM_OF_LOCKS; i++) {
        locks[i] = new ReentrantReadWriteLock();
      }
    }

    public void readLock(int hash) {
      findLock(hash).readLock().lock();
    }

    public void readUnlock(int hash) {
      findLock(hash).readLock().unlock();
    }

    public void writeLock(int hash) {
      findLock(hash).writeLock().lock();
    }

    public void writeUnlock(int hash) {
      findLock(hash).writeLock().unlock();
    }

    private ReentrantReadWriteLock findLock(int hash) {
      return locks[hash % NUM_OF_LOCKS];
    }
  }
}


