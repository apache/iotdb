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

package org.apache.iotdb.commons.binaryallocator.utils;

import org.apache.iotdb.commons.binaryallocator.config.AllocatorConfig;

/**
 * SizeClasses class manages different size classes of memory blocks in a memory allocator. It
 * optimizes the memory allocation process by precomputing the block sizes and mapping them to
 * indices.
 */
public final class SizeClasses {

  // Integer size in bits minus 1, used for log2 calculations
  private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;

  // Mapping from size class index to actual memory block size
  private final int[] sizeIdx2sizeTab;

  // Log2 value of the minimum memory block size
  private final int log2MinSize;

  // Log2 value of the size class group
  private final int log2SizeClassGroup;

  /**
   * Constructor that initializes the size class table based on the allocator configuration.
   *
   * @param allocatorConfig The allocator configuration containing minimum and maximum allocation
   *     sizes.
   */
  public SizeClasses(AllocatorConfig allocatorConfig) {
    this.log2SizeClassGroup = allocatorConfig.log2ClassSizeGroup;
    this.log2MinSize = log2(allocatorConfig.minAllocateSize);

    int maxSize = allocatorConfig.maxAllocateSize;
    int sizeClassGroupCount = log2(maxSize) - log2MinSize;

    // Initialize the sizeIdx2sizeTab array based on the number of size class groups
    sizeIdx2sizeTab = new int[(sizeClassGroupCount << log2SizeClassGroup) + 1];

    // Calculate the size of each size class and populate the table
    initializeSizeClasses(allocatorConfig.minAllocateSize, maxSize);
  }

  /**
   * Returns the memory block size for a given size class index.
   *
   * @param sizeIdx The index of the size class.
   * @return The memory block size corresponding to the size class index.
   */
  public int sizeIdx2size(int sizeIdx) {
    return sizeIdx2sizeTab[sizeIdx];
  }

  /**
   * Returns the size class index for a given memory block size.
   *
   * @param size The memory block size.
   * @return The corresponding size class index.
   */
  public int size2SizeIdx(int size) {
    int log2Size = log2((size << 1) - 1); // Calculate the approximate log2 value
    int shift = log2Size - log2MinSize - 1;

    // Calculate the size class group
    int group = shift << log2SizeClassGroup;
    int log2Delta = log2Size - 1 - log2SizeClassGroup;

    // Calculate the index within the size class group
    int mod = (size - 1) >> log2Delta & (1 << log2SizeClassGroup) - 1;
    return group + mod + 1;
  }

  /**
   * Returns the total number of size classes.
   *
   * @return The total number of size classes.
   */
  public int getSizeClassNum() {
    return sizeIdx2sizeTab.length;
  }

  /**
   * Calculates the memory block size for a given log2 group, delta, and log2 delta.
   *
   * @param log2Group The log2 value of the current size class group.
   * @param delta The delta value for the size class.
   * @param log2Delta The log2 value of the delta.
   * @return The calculated memory block size.
   */
  private static int calculateSize(int log2Group, int delta, int log2Delta) {
    return (1 << log2Group) + (delta << log2Delta);
  }

  /**
   * Calculates the log2 value of a given integer.
   *
   * @param val The value to calculate the log2 for.
   * @return The log2 value of the given integer.
   */
  private static int log2(int val) {
    return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
  }

  /**
   * Initializes the size class table by calculating the memory block sizes for each size class.
   *
   * @param minSize The minimum memory block size.
   * @param maxSize The maximum memory block size.
   */
  private void initializeSizeClasses(int minSize, int maxSize) {
    int nDeltaLimit = 1 << log2SizeClassGroup;
    int log2Group = log2MinSize;
    int log2Delta = log2MinSize - log2SizeClassGroup;

    int sizeCount = 0;
    int size = calculateSize(log2Group, 0, log2Delta);
    sizeIdx2sizeTab[sizeCount++] = size; // Initial size

    // Iterate through the remaining size classes and calculate their sizes
    for (; size < maxSize; log2Group++, log2Delta++) {
      for (int nDelta = 1; nDelta <= nDeltaLimit && size <= maxSize; nDelta++) {
        size = calculateSize(log2Group, nDelta, log2Delta);
        sizeIdx2sizeTab[sizeCount++] = size;
      }
    }
  }
}
