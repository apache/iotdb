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
 * The block container of one measurement column in an {@link AlignedTVList}.
 *
 * <p>An aligned TVList keeps one container per measurement column of the device, but a wide device
 * usually writes only a small subset of its columns, and a column is null in most blocks even when
 * it is written. Such a block slot therefore stays an <em>implicit</em> null: the backing array is
 * allocated by the first block that holds a value array and grows on demand. A column that is never
 * written costs only the container object, and a block that stays null costs nothing.
 *
 * <p>The list still reports the number of blocks the TVList has opened, so the rest of the memtable
 * addresses a column exactly like a dense list: {@code get(block)} yields null for an implicit
 * slot, {@code size()} equals the block count, and appending a null block keeps the slot implicit.
 *
 * <p>This matters because the container is created for every column of every device, while the
 * number of blocks a memtable column holds is small and unrelated to the global average series
 * length: pre-allocating one {@code Object[]} per column with that average wasted about 1.3 KB per
 * column and dominated the memory of wide devices.
 */
final class LazyValueColumn extends AbstractList<Object> implements RandomAccess {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LazyValueColumn.class);

  /** Value arrays of the materialized blocks; null until a block holds a value array. */
  private Object[] blocks;

  /** Number of blocks this column covers, including the implicit nulls before and after them. */
  private int blockCount;

  /**
   * Cover {@code blockCount} blocks that are already open, e.g. for a column added by schema, and
   * pre-allocate {@code preallocatedBlockNum} slots when it is positive.
   */
  LazyValueColumn(int blockCount, int preallocatedBlockNum) {
    this.blockCount = blockCount;
    if (preallocatedBlockNum > 0) {
      blocks = new Object[preallocatedBlockNum];
    }
  }

  @Override
  public int size() {
    return blockCount;
  }

  @Override
  public Object get(int index) {
    Objects.checkIndex(index, blockCount);
    return index < materializedLength() ? blocks[index] : null;
  }

  @Override
  public Object set(int index, Object element) {
    Objects.checkIndex(index, blockCount);
    if (element == null) {
      // Keep an implicit slot implicit; a materialized array keeps its (now null) slot.
      if (index >= materializedLength()) {
        return null;
      }
      Object previous = blocks[index];
      blocks[index] = null;
      return previous;
    }
    Object[] arrays = ensureCapacity(index + 1);
    Object previous = arrays[index];
    arrays[index] = element;
    return previous;
  }

  @Override
  public void add(int index, Object element) {
    Objects.checkIndex(index, blockCount + 1);
    if (index == blockCount) {
      if (element != null) {
        ensureCapacity(blockCount + 1)[blockCount] = element;
      }
      blockCount++;
      return;
    }
    // Inserting inside the list is not used by the memtable, but must stay correct.
    Object[] arrays = ensureCapacity(blockCount + 1);
    System.arraycopy(arrays, index, arrays, index + 1, blockCount - index);
    arrays[index] = element;
    blockCount++;
  }

  @Override
  public void clear() {
    blocks = null;
    blockCount = 0;
  }

  /** Memory retained by this container, excluding the value arrays themselves. */
  long ramCost() {
    return INSTANCE_SIZE
        + (blocks == null ? 0 : RamUsageEstimator.sizeOfObjectArray(blocks.length));
  }

  private int materializedLength() {
    return blocks == null ? 0 : blocks.length;
  }

  private Object[] ensureCapacity(int minLength) {
    if (blocks == null) {
      blocks = new Object[minLength];
    } else if (blocks.length < minLength) {
      // Grow geometrically so that appending one block stays amortized O(1).
      blocks = Arrays.copyOf(blocks, Math.max(minLength, blocks.length + (blocks.length >> 1)));
    }
    return blocks;
  }
}
