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
 * The block container of one column of a memtable list.
 *
 * <p>Internally it is an {@code ArrayList}-style growable array: a block count, a backing array and
 * 1.5x growth with {@link Arrays#copyOf}, and appending or replacing a slot keeps the same
 * semantics as {@code ArrayList}. Two properties are added on top:
 *
 * <ul>
 *   <li>{@link #ramCost()} reports the length of the array that is actually allocated, so the
 *       memtable accounting charges the memory that is really retained instead of assuming one
 *       reference per block;
 *   <li>the array only has to cover the blocks that were written: the slots above it stay implicit
 *       nulls, so a column that is never written allocates nothing and a null block costs nothing.
 *       {@link #size()} still reports every opened block, so readers address the column exactly
 *       like a dense list.
 * </ul>
 */
final class LazyBlockList<T> extends AbstractList<T> implements RandomAccess {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LazyBlockList.class);

  /** Longest array the JVM can allocate in practice; the growth arithmetic is capped below it. */
  private static final int MAX_BLOCK_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

  /** Slots backed by the array; the remaining blocks are implicit nulls. */
  private Object[] blocks;

  /** Number of blocks this column covers, including the implicit nulls before and after them. */
  private int blockCount;

  /**
   * Slots reserved when this column is written for the first time. A column that is never written
   * allocates nothing, so the many empty columns of a wide device stay free. Growth after the first
   * materialization follows the regular 1.5x policy.
   */
  private final int initialCapacity;

  LazyBlockList(int blockCount, int preallocatedBlockNum) {
    this.blockCount = blockCount;
    this.initialCapacity = Math.max(0, preallocatedBlockNum);
  }

  @Override
  public int size() {
    return blockCount;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get(int index) {
    Objects.checkIndex(index, blockCount);
    return index < materializedLength() ? (T) blocks[index] : null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T set(int index, T element) {
    Objects.checkIndex(index, blockCount);
    if (element == null) {
      if (index >= materializedLength()) {
        return null;
      }
      T previous = (T) blocks[index];
      blocks[index] = null;
      return previous;
    }
    Object[] arrays = ensureCapacity(index + 1);
    T previous = (T) arrays[index];
    arrays[index] = element;
    return previous;
  }

  @Override
  public void add(int index, T element) {
    Objects.checkIndex(index, blockCount + 1);
    if (index == blockCount) {
      // A null tail slot stays implicit, so appending a block does not allocate.
      if (element != null) {
        ensureCapacity(blockCount + 1)[blockCount] = element;
      }
      blockCount++;
      return;
    }
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

  /** Memory retained by this container, excluding the values it points to. */
  long ramCost() {
    return INSTANCE_SIZE
        + (blocks == null ? 0 : RamUsageEstimator.sizeOfObjectArray(blocks.length));
  }

  /** Retained memory of a container that did not allocate a slot yet. */
  static long instanceRamCost() {
    return INSTANCE_SIZE;
  }

  /** Number of slots backed by the array; the blocks above it are implicit nulls. */
  int materializedLength() {
    return blocks == null ? 0 : blocks.length;
  }

  /** Copy the block count and the allocated capacity, leaving the slots empty. */
  LazyBlockList<T> copyStructure() {
    LazyBlockList<T> copy = new LazyBlockList<>(blockCount, initialCapacity);
    if (blocks != null) {
      copy.blocks = new Object[blocks.length];
    }
    return copy;
  }

  /**
   * Array length that holds at least {@code minLength} slots of a container that currently holds
   * {@code currentLength}.
   *
   * <p>The length grows geometrically so that appending one block stays amortized O(1). It is
   * computed in long arithmetic because a nearly full array would overflow {@code int}, and it is
   * capped at the longest array the JVM can allocate: at that limit the exact minimum is returned,
   * so the allocation fails instead of silently dropping slots.
   */
  static int grownCapacity(int currentLength, int minLength) {
    long grownLength = currentLength + (long) (currentLength >> 1);
    int capacity = (int) Math.min(Math.max(grownLength, (long) minLength), MAX_BLOCK_ARRAY_LENGTH);
    return capacity < minLength ? minLength : capacity;
  }

  private Object[] ensureCapacity(int minLength) {
    if (blocks == null) {
      // First materialization of this column: honor the configured pre-allocation once. Later
      // growth always follows the regular policy.
      blocks = new Object[Math.max(minLength, initialCapacity)];
    } else if (blocks.length < minLength) {
      blocks = Arrays.copyOf(blocks, grownCapacity(blocks.length, minLength));
    }
    return blocks;
  }
}
