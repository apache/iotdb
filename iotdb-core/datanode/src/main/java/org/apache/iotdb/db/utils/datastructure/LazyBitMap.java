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

import org.apache.tsfile.utils.BitMap;

import java.util.ArrayList;

public class LazyBitMap {
  private final int startPosition;
  private final int endPosition;
  private final int blockSize;
  private final ArrayList<BitMap> blocks;

  public LazyBitMap(int startIndex, int appendSize, int endIndex) {
    if (endIndex < startIndex) {
      throw new IllegalArgumentException("endIndex must be >= startIndex");
    }
    if (appendSize <= 0) {
      throw new IllegalArgumentException("appendSize must be positive");
    }
    this.startPosition = startIndex;
    this.endPosition = endIndex;
    this.blockSize = appendSize;
    this.blocks = new ArrayList<>(2);
  }

  public void mark(int index) {
    if (index < startPosition) {
      throw new IndexOutOfBoundsException("Index below startPosition: " + index);
    }
    if (index > endPosition) {
      throw new IndexOutOfBoundsException("Index exceeds endPosition: " + index);
    }
    int blockIndex = getBlockIndex(index);
    ensureCapacity(blockIndex);
    BitMap block = blocks.get(blockIndex);
    if (block == null) {
      block = new BitMap(blockSize);
      blocks.set(blockIndex, block);
    }
    block.mark(getInnerIndex(index));
  }

  private void ensureCapacity(int blockIndex) {
    while (blockIndex >= blocks.size()) {
      blocks.add(null);
    }
  }

  public boolean isMarked(int index) {
    if (index < startPosition) {
      return false;
    }
    if (index > endPosition) {
      throw new IndexOutOfBoundsException("Index exceeds endPosition: " + index);
    }
    int blockIndex = getBlockIndex(index);
    if (blockIndex >= blocks.size()) {
      return false;
    }
    BitMap block = blocks.get(blockIndex);
    if (block == null) {
      return false;
    }
    return block.isMarked(getInnerIndex(index));
  }

  private int getBlockIndex(int index) {
    return (index - startPosition) / blockSize;
  }

  private int getInnerIndex(int index) {
    return (index - startPosition) % blockSize;
  }
}
