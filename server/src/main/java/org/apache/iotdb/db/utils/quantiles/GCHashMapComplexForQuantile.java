package org.apache.iotdb.db.utils.quantiles; /*
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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GCHashMapComplexForQuantile {
  private final int maxSize = 1 << 14;
  private final int bucketBits = 16;
  int bitsOfValue, remainingBits, minBits;
  boolean isBucket;
  long[] bucket;
  double expectedRoughK;

  MutableLongLongMap hashMap;
  long hashMapCount;
  long[] value, count;
  private long deltaForUnsignedCompare;
  IntArrayList index;
  long DEBUG;
  int aggressiveLevel, aggressiveMaxLevel;
  GSHashMapAggressiveForQuantile[] aggressiveHashMap;
  boolean[] aggressiveFailed;

  public GCHashMapComplexForQuantile(
      int bits, int minBits) { // simply 16-bit bucket when remainingBits<=minBits.
    this.minBits = minBits;
    bitsOfValue = remainingBits = bits;
    isBucket = false;
    if (bits == 64) deltaForUnsignedCompare = 1L << 63; // unsigned long
    else deltaForUnsignedCompare = 0;
    index = new IntArrayList(maxSize);
    value = new long[maxSize];
    count = new long[maxSize];
    hashMap = new LongLongHashMap(maxSize);
    hashMapCount = 0;
  }

  public GCHashMapComplexForQuantile(int bits, int minBits, double expectedRoughK) {
    this(bits, minBits);
    this.expectedRoughK = expectedRoughK;
    aggressiveMaxLevel = Math.max(1, bitsOfValue / 16 - 1);
    aggressiveLevel = 0;
    aggressiveHashMap = new GSHashMapAggressiveForQuantile[aggressiveMaxLevel];
    aggressiveFailed = new boolean[aggressiveMaxLevel];
    Arrays.fill(aggressiveFailed, false);
  }

  private void turnToBucket() {
    //    System.out.println("[turnToBucket]+remaining:"+remainingBits+"  tot:"+hashMap.size());
    isBucket = true;
    if (bucket == null) bucket = new long[1 << bucketBits];
    else Arrays.fill(bucket, 0);
    hashMap.forEachKeyValue((k, v) -> bucket[(int) (k >>> (remainingBits - bucketBits))] += v);

    remainingBits = bucketBits;
  }

  private void constructAggressiveHashMap() {
    aggressiveHashMap[aggressiveLevel] =
        new GSHashMapAggressiveForQuantile(
            maxSize, bitsOfValue, this.expectedRoughK, value, count, index, hashMapCount);
    int size = hashMap.size();
    sortHashMap(size);
    aggressiveHashMap[aggressiveLevel].constructFromSortedArray(size);
    aggressiveLevel++;
  }

  private void rebuild() { // called when total size == maxSize
    //    System.out.println("[rebuild]+remaining:"+remainingBits+"  tot:"+totSize);
    if (remainingBits == bitsOfValue) {
      constructAggressiveHashMap();
    }
    int SHR = 1;
    if (remainingBits - SHR <= minBits) {
      turnToBucket();
      return;
    }
    deltaForUnsignedCompare = 0;
    MutableLongLongMap newMap = new LongLongHashMap(maxSize);
    hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> 1, v));

    while (newMap.size() == maxSize) {
      SHR++;
      if (remainingBits - SHR <= minBits) {
        turnToBucket();
        return;
      }
      newMap.clear();
      final int shr = SHR;
      hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> shr, v));
    }
    remainingBits -= SHR;
    hashMap = newMap;
  }

  public void insert(long num, long freq) {
    for (int level = 0; level < aggressiveLevel; level++)
      if (!aggressiveFailed[level])
        aggressiveFailed[level] = !aggressiveHashMap[level].update(num, freq);

    if (isBucket) {
      bucket[(int) (num >>> (bitsOfValue - remainingBits))] += freq;
    } else {
      hashMapCount += freq;
      hashMap.addToValue(num >>> (bitsOfValue - remainingBits), freq);
      if (hashMap.size() == maxSize) rebuild();
    }
  }

  public int getRemainingBits() {
    return remainingBits;
  }

  private void sortHashMap(int size) {
    index.size(size);
    for (int i = 0; i < size; i++) index.set(i, i);
    int tmp = 0;
    for (LongLongPair p : hashMap.keyValuesView()) {
      value[tmp] = p.getOne();
      count[tmp] = p.getTwo();
      tmp++;
    }
    index.sort(
        (x, y) ->
            Long.compare(value[x] ^ deltaForUnsignedCompare, value[y] ^ deltaForUnsignedCompare));
  }

  public List<Long> findResultIndex(long K1, long K2) {
    List<Long> result = new ArrayList<>(8);
    long sum = 0;

    if (isBucket) {
      for (int i = 0; i < (1 << bucketBits); i++) {
        sum += bucket[i];
        if (sum >= K1 && result.size() == 0) {
          result.add((long) i);
          result.add(sum - bucket[i]);
          result.add(bucket[i]);
        }
        if (sum >= K2 && result.size() == 3) {
          result.add((long) i);
          result.add(sum - bucket[i]);
          result.add(bucket[i]);
          break;
        }
      }
    } else {
      int x, hashMapSize = hashMap.size();
      sortHashMap(hashMapSize);
      for (int i = 0; i < hashMapSize; i++) {
        x = index.getInt(i);
        //      System.out.println(count[x] + "  " + value[x]);
        sum += count[x];
        if (sum >= K1 && result.size() == 0) {
          result.add(value[x]);
          result.add(sum - count[x]);
          result.add(count[x]);
        }
        if (sum >= K2 && result.size() == 3) {
          result.add(value[x]);
          result.add(sum - count[x]);
          result.add(count[x]);
          break;
        }
      }
    }
    for (int level = 0; level < aggressiveLevel; level++)
      if (!aggressiveFailed[level]) {
        DEBUG += aggressiveHashMap[level].DEBUG;
        List<Long> aggressiveResult = aggressiveHashMap[level].findResultIndex(K1, K2);
        if (aggressiveResult != null) {
          remainingBits = aggressiveHashMap[level].remainingBits;
          return aggressiveResult;
        }
      }
    return result;
  }

  public void reset(int bits, int minBits) {
    aggressiveLevel = 0;
    hashMapCount = 0;
    if (bits <= minBits) {
      bitsOfValue = bucketBits;
      remainingBits = bucketBits;
      isBucket = true;
      if (bucket == null) bucket = new long[(int) (1L << bucketBits)];
      else Arrays.fill(bucket, 0);
    } else {
      isBucket = false;
      bitsOfValue = remainingBits = bits;
      this.minBits = minBits;
      hashMap.clear();
    }
  }

  public void reset(int bits, int minBits, double expectedRoughK) {
    hashMapCount = 0;

    this.expectedRoughK = expectedRoughK;
    aggressiveMaxLevel = Math.max(1, bits / 16 - 1);
    aggressiveLevel = 0;
    aggressiveHashMap =
        new GSHashMapAggressiveForQuantile[aggressiveMaxLevel]; // TODO re-use memory
    aggressiveFailed = new boolean[aggressiveMaxLevel];
    Arrays.fill(aggressiveFailed, false);

    if (bits <= minBits) {
      bitsOfValue = bucketBits;
      remainingBits = bucketBits;
      isBucket = true;
      if (bucket == null) bucket = new long[(int) (1L << bucketBits)];
      else Arrays.fill(bucket, 0);
    } else {
      isBucket = false;
      bitsOfValue = remainingBits = bits;
      this.minBits = minBits;
      hashMap.clear();
    }
  }

  public int getHashMapSize() {
    return hashMap.size();
  }

  public RichIterable<LongLongPair> getKeyValuesView() {
    return hashMap.keyValuesView();
  }
}
