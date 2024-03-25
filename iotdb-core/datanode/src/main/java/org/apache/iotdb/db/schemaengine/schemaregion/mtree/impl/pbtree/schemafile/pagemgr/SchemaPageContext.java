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

import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISegmentedPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Thread local variables about write/update process. */
class SchemaPageContext {
  protected static final Logger logger = LoggerFactory.getLogger(SchemaPageContext.class);

  final long threadID;
  final PageIndexSortBuckets indexBuckets;
  // locked and dirty pages are all referred pages, they all reside in page cache
  final Map<Integer, ISchemaPage> referredPages;
  final Set<Integer> lockTraces;
  // track B+Tree traversal trace
  final int[] treeTrace;
  int dirtyCnt;
  int interleavedFlushCnt;

  // to report first reentrant lock detail
  static boolean lockFaultTrigger = true;

  // flush B+Tree leaf before operation finished since all records are ordered
  ISegmentedPage lastLeafPage;

  public SchemaPageContext() {
    threadID = Thread.currentThread().getId();
    referredPages = new HashMap<>();
    indexBuckets = new PageIndexSortBuckets(SchemaFileConfig.SEG_SIZE_LST, referredPages);
    treeTrace = new int[16];
    lockTraces = new HashSet<>();
    lastLeafPage = null;
    dirtyCnt = 0;
    interleavedFlushCnt = 0;
  }

  public void markDirty(ISchemaPage page) {
    markDirty(page, false);
  }

  public void markDirty(ISchemaPage page, boolean forceReplace) {
    if (!page.isDirtyPage()) {
      dirtyCnt++;
    }
    page.setDirtyFlag();
    refer(page);
    if (forceReplace && referredPages.containsKey(page.getPageIndex())) {

      // previous page is dirty, so it's not a new dirty page
      if (referredPages.get(page.getPageIndex()).isDirtyPage()) {
        dirtyCnt--;
      }
      // force to replace
      referredPages.put(page.getPageIndex(), page);
    }
  }

  public void traceLock(ISchemaPage page) throws SegmentNotFoundException {
    refer(page);
    if (lockTraces.contains(page.getPageIndex())) {
      // FIXME rough resolve for reentrant write lock
      if (referredPages.get(page.getPageIndex()) != page) {
        logger.error("Duplicate page instances with identical index: {}", page.getPageIndex());
      }
      // it's exactly twice-locked
      if (((ReentrantReadWriteLock) page.getLock()).getWriteHoldCount() > 1) {
        logger.warn(
            "Page [{}] had been locked {} times.",
            ((ReentrantReadWriteLock) page.getLock()).getWriteHoldCount());
        // had already been locked twice
        page.getLock().writeLock().unlock();

        if (lockFaultTrigger) {
          logger.warn(
              "Reentrant write locks on page {}, content detail:{}",
              page.getPageIndex(),
              page.inspect());
          lockFaultTrigger = false;
        } else {
          logger.warn("Reentrant write locks on page:{}", page.getPageIndex());
        }
        return;
      }
    }
    lockTraces.add(page.getPageIndex());
  }

  // referred pages will not be evicted until operation finished
  public void refer(ISchemaPage page) {
    if (!referredPages.containsKey(page.getPageIndex())) {
      page.incrementAndGetRefCnt();
      referredPages.put(page.getPageIndex(), page);
    }
  }

  /**
   * Since records are ordered for write operation, it is reasonable to flush those left siblings of
   * the active leaf page. The target page would be initiated at the first split within the
   * operation.
   *
   * @param page left leaf of the split
   */
  public void invokeLastLeaf(ISchemaPage page) {
    // only record at the first split
    if (lastLeafPage == null) {
      lastLeafPage = page.getAsSegmentedPage();
    }
  }

  public void appendBucketIndex(PageIndexSortBuckets pisb) {
    for (int i = 0; i < SchemaFileConfig.SEG_SIZE_LST.length; i++) {
      pisb.getBucket(i).addAll(indexBuckets.getBucket(i));
    }
  }
}
