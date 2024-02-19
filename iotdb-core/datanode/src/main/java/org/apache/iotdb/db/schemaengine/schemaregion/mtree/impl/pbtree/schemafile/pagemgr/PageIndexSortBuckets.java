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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;

/**
 * * Index buckets affiliated to a page collection. Indexes are sort into buckets according to spare
 * space of its corresponding page instance.
 */
class PageIndexSortBuckets {
  private final short[] bounds;
  private final ArrayDeque[] buckets;
  private final Map<Integer, ISchemaPage> pageContainer;

  public PageIndexSortBuckets(short[] borders, Map<Integer, ISchemaPage> container) {
    bounds = Arrays.copyOf(borders, borders.length);
    buckets = new ArrayDeque[borders.length];
    pageContainer = container;
    for (int i = 0; i < borders.length; i++) {
      buckets[i] = new ArrayDeque();
    }
  }

  public void clear() {
    for (ArrayDeque q : buckets) {
      q.clear();
    }
  }

  public void sortIntoBucket(ISchemaPage page, short newSegSize) {
    if (page.getAsSegmentedPage() == null) {
      return;
    }

    // actual space occupied by a segment includes both its own length and the length of its
    // offset. so available length for a segment is the spareSize minus the offset bytes
    short availableSize =
        newSegSize < 0
            ? (short) (page.getAsSegmentedPage().getSpareSize() - SchemaFileConfig.SEG_OFF_DIG)
            : (short)
                (page.getAsSegmentedPage().getSpareSize()
                    - newSegSize
                    - SchemaFileConfig.SEG_OFF_DIG);

    // too small to index
    if (availableSize <= SchemaFileConfig.SEG_HEADER_SIZE) {
      return;
    }

    // be like: SEG_HEADER < buckets[0] <= bounds[0] < buckets[1] <= ...
    for (int i = 0; i < bounds.length; i++) {
      // the last of SEG_SIZE_LST is the maximum page size, definitely larger than others
      if (availableSize <= bounds[i]) {
        buckets[i].add(page.getPageIndex());
        return;
      }
    }
  }

  public ArrayDeque<Integer> getBucket(int index) {
    return buckets[index];
  }

  /**
   * @param withLock set if page container is a global/shared object
   * @return the page index will be removed from the bucket.
   */
  public synchronized ISchemaPage getNearestFitPage(short size, boolean withLock) {
    ISchemaPage targetPage;
    int elemToCheck;
    for (int i = 0; i < buckets.length && pageContainer.size() > 0; i++) {
      // buckets[i] stores pages with spare space less than bounds[i]
      elemToCheck = buckets[i].size();
      while (size < bounds[i] && elemToCheck > 0) {
        // find roughly fit page
        targetPage = pageContainer.getOrDefault(buckets[i].poll(), null);
        elemToCheck--;

        if (targetPage == null || targetPage.getAsSegmentedPage() == null) {
          // act as lazy remove on buckets
          continue;
        }

        // seek in global container thus other page could be read locked
        if (withLock
            && targetPage.getAsSegmentedPage().isCapableForSegSize(size)
            && targetPage.getLock().writeLock().tryLock()) {
          if (targetPage.getAsSegmentedPage().isCapableForSegSize(size)) {
            return targetPage.getAsSegmentedPage();
          }
          targetPage.getLock().writeLock().unlock();
        }

        // only in local dirty pages which are always write locked by itself
        if (!withLock && targetPage.getAsSegmentedPage().isCapableForSegSize(size)) {
          return targetPage;
        }

        // not large as expected, fit into suitable bucket
        if (i > 0 && targetPage.getAsSegmentedPage().isCapableForSegSize(bounds[0])) {
          sortIntoBucket(targetPage, (short) -1);
        }
      }
    }
    return null;
  }
}
