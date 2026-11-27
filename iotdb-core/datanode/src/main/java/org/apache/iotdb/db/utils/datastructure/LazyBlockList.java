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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Objects;
import java.util.RandomAccess;

/**
 * Adaptive paged block container of one column of a memtable list.
 *
 * <p>Storage moves lazily through four states:
 *
 * <ul>
 *   <li>{@code EMPTY}: nothing is allocated;
 *   <li>{@code INLINE}: the column holds a single block, stored directly in {@code storage} and
 *       indexed by {@code state}, so one written block costs no array at all;
 *   <li>{@code SINGLE_PAGE}: every written block falls in one aligned page of {@link #PAGE_SIZE}
 *       blocks, so a write far ahead of the current block does not materialize the skipped prefix;
 *   <li>{@code PAGED}: a directory of fixed-size leaf pages. Leaf pages are never resized or
 *       copied; only the directory grows, and it holds one reference per {@code PAGE_SIZE} blocks.
 * </ul>
 *
 * <p>{@link #size()} is the number of blocks the list has opened, so readers address a column
 * exactly like a dense list, while {@link #ramCost()} reports the storage that is really retained.
 */
final class LazyBlockList<T> extends AbstractList<T> implements RandomAccess {

  static final int PAGE_SHIFT = 4;
  static final int PAGE_SIZE = 1 << PAGE_SHIFT;
  static final int PAGE_MASK = PAGE_SIZE - 1;
  static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
  private static final int MIN_DIRECTORY_LENGTH = 4;

  private static final int STATE_EMPTY = -1;
  private static final int STATE_PAGED = -2;

  /** A page {@code p} that holds the only materialized block is encoded as base - p. */
  private static final int STATE_SINGLE_PAGE_BASE = -3;

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LazyBlockList.class);
  private static final long PAGE_RAM_COST = RamUsageEstimator.sizeOfObjectArray(PAGE_SIZE);

  /** Number of blocks the column covers, including the implicit nulls before and after them. */
  private int blockCount;

  /** {@code EMPTY}/{@code PAGED}/{@code SINGLE_PAGE*} marker, or the inline block index. */
  private int state = STATE_EMPTY;

  private int allocatedPages;

  /** Inline element, single leaf page, or directory of leaf pages, depending on {@link #state}. */
  private Object storage;

  LazyBlockList(int blockCount, int preallocatedBlockNum) {
    this.blockCount = blockCount;
    if (preallocatedBlockNum > 0) {
      if (preallocatedBlockNum <= PAGE_SIZE) {
        storage = new Object[PAGE_SIZE];
        allocatedPages = 1;
        state = STATE_SINGLE_PAGE_BASE;
      } else {
        storage = new Object[requiredDirectoryLength(preallocatedBlockNum)][];
        state = STATE_PAGED;
      }
    }
  }

  @Override
  public int size() {
    return blockCount;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get(int index) {
    Objects.checkIndex(index, blockCount);
    int current = state;
    if (current >= 0) {
      return index == current ? (T) storage : null;
    }
    if (current == STATE_EMPTY) {
      return null;
    }
    int pageIndex = index >>> PAGE_SHIFT;
    if (current == STATE_PAGED) {
      Object[][] directory = (Object[][]) storage;
      if (pageIndex >= directory.length) {
        return null;
      }
      Object[] page = directory[pageIndex];
      return page == null ? null : (T) page[index & PAGE_MASK];
    }
    return pageIndex == (STATE_SINGLE_PAGE_BASE - current)
        ? (T) ((Object[]) storage)[index & PAGE_MASK]
        : null;
  }

  @Override
  public T set(int index, T element) {
    Objects.checkIndex(index, blockCount);
    return element == null ? clearSlot(index) : storeSlot(index, element);
  }

  @Override
  public void add(int index, T element) {
    Objects.checkIndex(index, blockCount + 1);
    if (index != blockCount) {
      // The memtable only appends blocks, so inserting inside the list is not supported.
      throw new UnsupportedOperationException("Only appending a block is supported");
    }
    if (element != null) {
      storeSlot(blockCount, element);
    }
    blockCount++;
  }

  @Override
  public void clear() {
    blockCount = 0;
    state = STATE_EMPTY;
    allocatedPages = 0;
    storage = null;
  }

  /** Memory retained by this container, excluding the values it points to. */
  long ramCost() {
    int current = state;
    if (current == STATE_EMPTY || current >= 0) {
      return INSTANCE_SIZE;
    }
    if (current <= STATE_SINGLE_PAGE_BASE) {
      return INSTANCE_SIZE + PAGE_RAM_COST;
    }
    Object[][] directory = (Object[][]) storage;
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOfObjectArray(directory.length)
        + (long) allocatedPages * PAGE_RAM_COST;
  }

  /** Retained memory of a container that did not allocate a slot yet. */
  static long instanceRamCost() {
    return INSTANCE_SIZE;
  }

  /** Highest index that can be read without materializing storage, plus one. */
  int materializedLength() {
    int current = state;
    if (current == STATE_EMPTY) {
      return 0;
    }
    if (current >= 0) {
      return current + 1;
    }
    if (current <= STATE_SINGLE_PAGE_BASE) {
      return (STATE_SINGLE_PAGE_BASE - current + 1) << PAGE_SHIFT;
    }
    return ((Object[][]) storage).length << PAGE_SHIFT;
  }

  /** Copy the block count and the allocated storage layout, leaving the slots empty. */
  LazyBlockList<T> copyStructure() {
    LazyBlockList<T> copy = new LazyBlockList<>(blockCount, 0);
    int current = state;
    if (current == STATE_EMPTY) {
      return copy;
    }
    if (current >= 0) {
      copy.state = current;
      return copy;
    }
    if (current <= STATE_SINGLE_PAGE_BASE) {
      copy.state = current;
      copy.allocatedPages = 1;
      copy.storage = new Object[PAGE_SIZE];
      return copy;
    }
    Object[][] sourceDirectory = (Object[][]) storage;
    Object[][] copiedDirectory = new Object[sourceDirectory.length][];
    for (int pageIndex = 0; pageIndex < sourceDirectory.length; pageIndex++) {
      if (sourceDirectory[pageIndex] != null) {
        copiedDirectory[pageIndex] = new Object[PAGE_SIZE];
        copy.allocatedPages++;
      }
    }
    copy.state = STATE_PAGED;
    copy.storage = copiedDirectory;
    return copy;
  }

  /**
   * Directory length that holds at least {@code minRequired} pages.
   *
   * <p>The length doubles from {@link #MIN_DIRECTORY_LENGTH}. It is computed in long arithmetic
   * because an almost full directory would overflow {@code int}, and it is capped at the longest
   * array the JVM can allocate: at that limit the exact minimum is returned, so the allocation
   * fails instead of silently dropping pages.
   */
  static int grownDirectoryLength(int currentLength, int minRequired) {
    long doubled = Math.max(MIN_DIRECTORY_LENGTH, (long) currentLength << 1);
    int capacity = (int) Math.min(Math.max(doubled, (long) minRequired), MAX_CAPACITY);
    return capacity < minRequired ? minRequired : capacity;
  }

  private static int requiredDirectoryLength(int blockCapacity) {
    int pages = ((blockCapacity - 1) >>> PAGE_SHIFT) + 1;
    return Math.max(MIN_DIRECTORY_LENGTH, pages);
  }

  @SuppressWarnings("unchecked")
  private T storeSlot(int index, T element) {
    int current = state;
    if (current == STATE_EMPTY) {
      state = index;
      storage = element;
      return null;
    }
    if (current >= 0) {
      if (index == current) {
        T previous = (T) storage;
        storage = element;
        return previous;
      }
      promoteInline(current, index, element);
      return null;
    }
    int pageIndex = index >>> PAGE_SHIFT;
    if (current <= STATE_SINGLE_PAGE_BASE) {
      int pageOfStorage = STATE_SINGLE_PAGE_BASE - current;
      if (pageIndex == pageOfStorage) {
        Object[] page = (Object[]) storage;
        int slot = index & PAGE_MASK;
        T previous = (T) page[slot];
        page[slot] = element;
        return previous;
      }
      promoteSinglePage(pageOfStorage, pageIndex, index & PAGE_MASK, element);
      return null;
    }
    Object[][] directory = (Object[][]) storage;
    if (pageIndex >= directory.length) {
      directory = Arrays.copyOf(directory, grownDirectoryLength(directory.length, pageIndex + 1));
      storage = directory;
    }
    Object[] page = directory[pageIndex];
    if (page == null) {
      page = new Object[PAGE_SIZE];
      directory[pageIndex] = page;
      allocatedPages++;
    }
    int slot = index & PAGE_MASK;
    T previous = (T) page[slot];
    page[slot] = element;
    return previous;
  }

  @SuppressWarnings("unchecked")
  private T clearSlot(int index) {
    int current = state;
    if (current == STATE_EMPTY) {
      return null;
    }
    if (current >= 0) {
      if (index != current) {
        return null;
      }
      T previous = (T) storage;
      state = STATE_EMPTY;
      storage = null;
      return previous;
    }
    int pageIndex = index >>> PAGE_SHIFT;
    if (current <= STATE_SINGLE_PAGE_BASE) {
      if (pageIndex != (STATE_SINGLE_PAGE_BASE - current)) {
        return null;
      }
      Object[] page = (Object[]) storage;
      int slot = index & PAGE_MASK;
      T previous = (T) page[slot];
      page[slot] = null;
      return previous;
    }
    Object[][] directory = (Object[][]) storage;
    if (pageIndex >= directory.length || directory[pageIndex] == null) {
      return null;
    }
    int slot = index & PAGE_MASK;
    T previous = (T) directory[pageIndex][slot];
    directory[pageIndex][slot] = null;
    return previous;
  }

  private void promoteInline(int inlineIndex, int newIndex, T newElement) {
    int inlinePage = inlineIndex >>> PAGE_SHIFT;
    int newPage = newIndex >>> PAGE_SHIFT;
    if (inlinePage == newPage) {
      Object[] page = new Object[PAGE_SIZE];
      page[inlineIndex & PAGE_MASK] = storage;
      page[newIndex & PAGE_MASK] = newElement;
      storage = page;
      allocatedPages = 1;
      state = STATE_SINGLE_PAGE_BASE - newPage;
      return;
    }
    Object[][] directory = new Object[grownDirectoryLength(0, Math.max(inlinePage, newPage) + 1)][];
    Object[] inlineLeaf = new Object[PAGE_SIZE];
    inlineLeaf[inlineIndex & PAGE_MASK] = storage;
    directory[inlinePage] = inlineLeaf;
    Object[] newLeaf = new Object[PAGE_SIZE];
    newLeaf[newIndex & PAGE_MASK] = newElement;
    directory[newPage] = newLeaf;
    storage = directory;
    allocatedPages = 2;
    state = STATE_PAGED;
  }

  private void promoteSinglePage(int pageOfStorage, int newPage, int newSlot, T newElement) {
    Object[][] directory =
        new Object[grownDirectoryLength(0, Math.max(pageOfStorage, newPage) + 1)][];
    // The existing leaf page is reused as is: promoting a state never copies its elements.
    directory[pageOfStorage] = (Object[]) storage;
    Object[] newLeaf = new Object[PAGE_SIZE];
    newLeaf[newSlot] = newElement;
    directory[newPage] = newLeaf;
    storage = directory;
    allocatedPages = 2;
    state = STATE_PAGED;
  }
}
