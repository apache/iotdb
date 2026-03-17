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

package org.apache.iotdb.commons.consensus.iotv2.consistency.ibf;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Invertible Bloom Filter (IBF) for set difference computation with O(d) communication complexity
 * where d is the number of differences. Uses k=3 hash functions to map (compositeKey, valueHash)
 * pairs into cells.
 */
public class InvertibleBloomFilter {

  private static final int DEFAULT_HASH_COUNT = 3;
  private static final long HASH_SEED_1 = 0x9E3779B97F4A7C15L;
  private static final long HASH_SEED_2 = 0x517CC1B727220A95L;
  private static final long HASH_SEED_3 = 0x6C62272E07BB0142L;
  private static final long[] SEEDS = {HASH_SEED_1, HASH_SEED_2, HASH_SEED_3};

  private final IBFCell[] cells;
  private final int cellCount;
  private final int hashCount;

  public InvertibleBloomFilter(int cellCount) {
    this(cellCount, DEFAULT_HASH_COUNT);
  }

  public InvertibleBloomFilter(int cellCount, int hashCount) {
    this.cellCount = Math.max(cellCount, 1);
    this.hashCount = hashCount;
    this.cells = new IBFCell[this.cellCount];
    for (int i = 0; i < this.cellCount; i++) {
      cells[i] = new IBFCell();
    }
  }

  private InvertibleBloomFilter(IBFCell[] cells, int hashCount) {
    this.cells = cells;
    this.cellCount = cells.length;
    this.hashCount = hashCount;
  }

  public void insert(long compositeKey, long valueHash) {
    for (int i = 0; i < hashCount; i++) {
      int idx = hashToIndex(compositeKey, i);
      cells[idx].add(compositeKey, valueHash);
    }
  }

  public void remove(long compositeKey, long valueHash) {
    for (int i = 0; i < hashCount; i++) {
      int idx = hashToIndex(compositeKey, i);
      cells[idx].remove(compositeKey, valueHash);
    }
  }

  /**
   * Subtract another IBF from this one (element-wise). The result IBF encodes the symmetric
   * difference: keys in this but not other (count=+1) and keys in other but not this (count=-1).
   */
  public InvertibleBloomFilter subtract(InvertibleBloomFilter other) {
    if (this.cellCount != other.cellCount) {
      throw new IllegalArgumentException(
          "IBF cell counts must match: " + this.cellCount + " vs " + other.cellCount);
    }
    IBFCell[] result = new IBFCell[cellCount];
    for (int i = 0; i < cellCount; i++) {
      result[i] =
          new IBFCell(this.cells[i].count, this.cells[i].keySum, this.cells[i].valueChecksum);
      result[i].subtract(other.cells[i]);
    }
    return new InvertibleBloomFilter(result, this.hashCount);
  }

  /**
   * Decode the IBF to recover all diff entries. The IBF should be the result of a subtraction
   * (IBF_Leader - IBF_Follower).
   *
   * @return decode result with success flag and decoded entries
   */
  public IBFDecodeResult decode() {
    List<DiffEntry> entries = new ArrayList<>();
    Deque<Integer> pureIndices = new ArrayDeque<>();

    // Initial scan for pure cells
    for (int i = 0; i < cellCount; i++) {
      if (cells[i].isPure()) {
        pureIndices.add(i);
      }
    }

    while (!pureIndices.isEmpty()) {
      int idx = pureIndices.poll();
      IBFCell cell = cells[idx];

      if (!cell.isPure()) {
        continue;
      }

      long key = cell.keySum;
      long valueHash = cell.valueChecksum;
      DiffEntry.DiffType type =
          cell.count == 1 ? DiffEntry.DiffType.LEADER_HAS : DiffEntry.DiffType.FOLLOWER_HAS;
      entries.add(new DiffEntry(key, valueHash, type));

      // Peel this entry from all cells it hashes to
      for (int i = 0; i < hashCount; i++) {
        int cellIdx = hashToIndex(key, i);
        if (cell.count == 1) {
          cells[cellIdx].remove(key, valueHash);
        } else {
          cells[cellIdx].add(key, valueHash);
        }
        if (cells[cellIdx].isPure()) {
          pureIndices.add(cellIdx);
        }
      }
    }

    // Check if all cells are empty (complete decode)
    boolean complete = true;
    for (IBFCell cell : cells) {
      if (!cell.isEmpty()) {
        complete = false;
        break;
      }
    }

    if (complete) {
      return IBFDecodeResult.success(entries);
    } else {
      return IBFDecodeResult.failure(entries, entries.size());
    }
  }

  /**
   * Hash a composite key to a cell index for the i-th hash function, using multiplicative hashing
   * with distinct seeds to minimize collisions.
   */
  private int hashToIndex(long key, int hashFunctionIndex) {
    long seed = SEEDS[hashFunctionIndex % SEEDS.length];
    if (hashFunctionIndex >= SEEDS.length) {
      seed ^= hashFunctionIndex * 0xDEADBEEFL;
    }
    long hash = key * seed;
    hash ^= hash >>> 33;
    hash *= 0xFF51AFD7ED558CCDL;
    hash ^= hash >>> 33;
    return (int) ((hash & 0x7FFFFFFFFFFFFFFFL) % cellCount);
  }

  public int getCellCount() {
    return cellCount;
  }

  public IBFCell[] getCells() {
    return cells;
  }

  /** Compute the optimal IBF size for an estimated diff count d with >99% decode probability. */
  public static int optimalCellCount(long estimatedDiffCount) {
    return (int) Math.max(Math.ceil(2.0 * estimatedDiffCount), 3);
  }

  public void serialize(DataOutputStream out) throws IOException {
    out.writeInt(cellCount);
    out.writeInt(hashCount);
    for (IBFCell cell : cells) {
      cell.serialize(out);
    }
  }

  public static InvertibleBloomFilter deserialize(ByteBuffer buffer) {
    int cellCount = buffer.getInt();
    int hashCount = buffer.getInt();
    IBFCell[] cells = new IBFCell[cellCount];
    for (int i = 0; i < cellCount; i++) {
      cells[i] = IBFCell.deserialize(buffer);
    }
    return new InvertibleBloomFilter(cells, hashCount);
  }

  /** Total serialized byte size: 4 (cellCount) + 4 (hashCount) + cellCount * 20 bytes */
  public int serializedSize() {
    return 8 + cellCount * IBFCell.SERIALIZED_SIZE;
  }
}
