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

package org.apache.iotdb.commons.utils.binaryallocator;

public final class SizeClasses {

  // loookup table
  private int[] sizeIdx2sizeTab;
  final int lookupMaxSize;

  final int LOG2_QUANTUM;
  private int log2SizeClassGroup;
  private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;

  public SizeClasses(AllocatorConfig allocatorConfig) {
    this.lookupMaxSize = allocatorConfig.maxAllocateSize;
    this.log2SizeClassGroup = allocatorConfig.log2ClassSizeGroup;
    this.LOG2_QUANTUM = log2(allocatorConfig.minAllocateSize);
    int group = log2(lookupMaxSize) - LOG2_QUANTUM;

    sizeIdx2sizeTab = new int[(group << log2SizeClassGroup) + 1];

    int ndeltaLimit = 1 << log2SizeClassGroup;
    int log2Group = LOG2_QUANTUM;
    int log2Delta = LOG2_QUANTUM - log2SizeClassGroup;

    int nSizes = 0;
    int size = calculateSize(log2Group, 0, log2Delta);
    sizeIdx2sizeTab[nSizes++] = size;
    // All remaining groups, nDelta start at 1.
    for (; size < lookupMaxSize; log2Group++, log2Delta++) {
      for (int nDelta = 1; nDelta <= ndeltaLimit && size <= lookupMaxSize; nDelta++) {
        size = calculateSize(log2Group, nDelta, log2Delta);
        sizeIdx2sizeTab[nSizes++] = size;
      }
    }
  }

  public int sizeIdx2size(int sizeIdx) {
    return sizeIdx2sizeTab[sizeIdx];
  }

  public int size2SizeIdx(int size) {
    int x = log2((size << 1) - 1);

    int shift = x - LOG2_QUANTUM - 1;

    int group = shift << log2SizeClassGroup;

    int log2Delta = x - 1 - log2SizeClassGroup;

    int mod = size - 1 >> log2Delta & (1 << log2SizeClassGroup) - 1;

    return group + mod + 1;
  }

  public int getSizeClassNum() {
    return sizeIdx2sizeTab.length;
  }

  private static int calculateSize(int log2Group, int nDelta, int log2Delta) {
    return (1 << log2Group) + (nDelta << log2Delta);
  }

  private static int log2(int val) {
    return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
  }
}
