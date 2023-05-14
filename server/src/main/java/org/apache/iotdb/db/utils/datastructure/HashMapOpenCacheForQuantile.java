package org.apache.iotdb.db.utils.datastructure; /*
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * long-long hashmap with <= 65536 elements. rebuild (right shift keys and merge) to keep <=65536.
 * open address, linear detection
 */
public class HashMapOpenCacheForQuantile {
  private final int MaxSize = (1 << 16) + 1,
      HashMask = (1 << 17) - 1,
      vcSize = 1 << 18,
      vcIndexMask = (1 << 18) - 1;
  private final int bucketBits = 16;
  int bitsOfValue, remainingBits, minBits;

  int nonZeroSize;
  long[] valueCount; // friendly to cache
  long zeroCount;
  long randSeed;
  private long deltaForUnsignedCompare;
  IntArrayList index;
  long DEBUG;
  boolean isBucket;
  long[] bucket;

  public HashMapOpenCacheForQuantile(
      int bits, int minBits) { // simply 16-bit bucket when remainingBits<=minBits.
    this.minBits = minBits;
    bitsOfValue = remainingBits = bits;
    isBucket = false;
    index = new IntArrayList(MaxSize);

    nonZeroSize = 0;
    zeroCount = 0;
    valueCount = new long[vcSize + 2];
    randSeed = 233L; // System.currentTimeMillis();
    if (bits == 64) deltaForUnsignedCompare = 1L << 63; // unsigned long
    else deltaForUnsignedCompare = 0;
  }

  private long hash(long key) { // Thomas Wang's hash (64bit)
    key = (~key) + (key << 21);
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8);
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4);
    key = key ^ (key >>> 28);
    key = key + (key << 31);
    return key;
  }

  private void turnToBucket(long[] valueCount) {
    //        System.out.println("[turnToBucket]+remaining:"+remainingBits+"  tot:"+getTotSize());
    isBucket = true;
    if (bucket == null) bucket = new long[1 << bucketBits];
    else Arrays.fill(bucket, 0);
    bucket[0] = zeroCount;
    //    System.out.println("[turnToBucket] zeroCount:"+zeroCount);
    for (int valueIndex = 0; valueIndex < vcSize; valueIndex += 2)
      if (valueCount[valueIndex] != 0L)
        bucket[(int) (valueCount[valueIndex] >>> (remainingBits - bucketBits))] +=
            valueCount[valueIndex | 1];
    remainingBits = bucketBits;
  }

  private int getTotSize() {
    return zeroCount > 0 ? nonZeroSize + 1 : nonZeroSize;
  }

  private void clear() {
    nonZeroSize = 0;
    zeroCount = 0;
    Arrays.fill(valueCount, 0);
  }

  private void rebuild() { // called when total size == maxSize
    //        System.out.println("[rebuild]+remaining:"+remainingBits+"  tot:"+getTotSize()+"
    // zeroCount:"+zeroCount);

    long[] oldValueCount = Arrays.copyOf(valueCount, vcSize);
    long oldZeroCount = zeroCount;
    long SHR = 0;
    while (getTotSize() == MaxSize) {
      SHR++;
      if (remainingBits - SHR <= minBits) {
        turnToBucket(oldValueCount);
        return;
      }
      clear();
      deltaForUnsignedCompare = 0;
      zeroCount = oldZeroCount;
      for (int valueIndex = 0; valueIndex < vcSize; valueIndex += 2)
        if (oldValueCount[valueIndex] != 0L) {
          insertHashMap(oldValueCount[valueIndex] >>> SHR, oldValueCount[valueIndex | 1]);

          //          DEBUG-=oldValueCount[valueIndex | 1];
          //          if(oldValueCount[valueIndex]>>>SHR==0L) {
          //            System.out.println("??!!" + oldValueCount[valueIndex] + "," +
          // oldValueCount[valueIndex | 1]);
          //          }
        }
      //      System.out.println("\t\t[rebuild][SHR] zeroCount:"+zeroCount);
    }
    remainingBits -= SHR;
  }

  private boolean insertHashMap(final long num, final long freq) { // return TRUE if new element
    //    DEBUG+=freq;
    //    if(DEBUG<11)System.out.println("\t\t\t\t\t!!!DEBUG<11   "+num+" "+freq);
    if (num != 0L) {
      int valueIndex = (int) (hash(num) & HashMask) << 1;
      long value = valueCount[valueIndex];
      while (value != 0L) {
        if (value == num) {
          valueCount[valueIndex | 1] += freq;
          return false;
        }
        valueIndex = valueIndex + 2 & vcIndexMask;
        value = valueCount[valueIndex];
      }
      valueCount[valueIndex] = num;
      valueCount[valueIndex | 1] = freq;
      nonZeroSize++;
      return true;
    } else {
      //      System.out.println("??????????"+num+","+freq+"   debug:"+DEBUG);
      zeroCount += freq;
      return zeroCount == freq;
    }
  }

  public void insert(long num, long freq) {
    num >>>= bitsOfValue - remainingBits;
    if (isBucket) {
      bucket[(int) num] += freq;
    } else {
      if (insertHashMap(num, freq) && getTotSize() == MaxSize) rebuild();
    }
  }

  public int getRemainingBits() {
    return remainingBits;
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
        }
        if (sum >= K2 && result.size() == 2) {
          result.add((long) i);
          result.add(sum - bucket[i]);
          break;
        }
      }
    } else {
      index.size(getTotSize());
      int tmp = 0;
      for (int valueIndex = 0; valueIndex < vcSize; valueIndex += 2)
        if (valueCount[valueIndex] != 0L) {
          index.set(tmp, valueIndex);
          tmp++;
        }
      if (zeroCount > 0) {
        index.set(nonZeroSize, vcSize);
        valueCount[vcSize] = 0L;
        valueCount[vcSize | 1] = zeroCount;
      }
      index.sort(
          (x, y) ->
              Long.compare(
                  valueCount[x] ^ deltaForUnsignedCompare,
                  valueCount[y] ^ deltaForUnsignedCompare));
      int valueIndex;
      long value, count;
      for (int i = 0; i < tmp; i++) {
        valueIndex = index.getInt(i);
        value = valueCount[valueIndex];
        count = valueCount[valueIndex | 1];
        sum += count;
        if (sum >= K1 && result.size() == 0) {
          result.add(value);
          result.add(sum - count);
        }
        if (sum >= K2 && result.size() == 2) {
          result.add(value);
          result.add(sum - count);
          break;
        }
      }
    }
    //    System.out.println("[open][findResultIndex] remainingBits:"+remainingBits+"
    // result:"+result);
    return result;
  }

  public void reset(int bits, int minBits) {
    if (bits <= minBits) {
      bitsOfValue = bits;
      remainingBits = bucketBits;
      isBucket = true;
      if (bucket == null) bucket = new long[(int) (1L << bucketBits)];
      else Arrays.fill(bucket, 0);
    } else {
      isBucket = false;
      bitsOfValue = remainingBits = bits;
      this.minBits = minBits;
      clear();
    }
  }
}
