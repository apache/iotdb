package org.apache.iotdb.db.utils.quantiles;
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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * GS long-long hashmap with <= 65536 elements. rebuild (right shift keys and merge) to keep
 * <=65536.
 */
public class EclipseCollectionsHashMapForQuantile {
  private final int maxSize = (1 << 15);
  private final int bucketBits = 16;
  int bitsOfValue, remainingBits, minBits;
  boolean isBucket;
  long[] bucket;

  MutableLongLongMap hashMap;
  int totSize;
  long[] value, count;
  private long deltaForUnsignedCompare;
  IntArrayList index;
  long DEBUG;

  public EclipseCollectionsHashMapForQuantile(
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
  }

  private void turnToBucket() {
    //    System.out.println("[turnToBucket]+remaining:"+remainingBits+"  tot:"+totSize);
    isBucket = true;
    if (bucket == null) bucket = new long[1 << bucketBits];
    else Arrays.fill(bucket, 0);
    //    for (LongLongCursor c = hashMap.cursor(); c.moveNext();) {
    //      bucket[(int)(c.key()>>> (remainingBits - bucketBits))] +=c.value();
    //    }
    hashMap.forEachKeyValue((k, v) -> bucket[(int) (k >>> (remainingBits - bucketBits))] += v);
    //    for(LongLongPair p:hashMap.keyValuesView())
    //      bucket[(int)(p.getOne()>>> (remainingBits - bucketBits))] += p.getTwo();

    remainingBits = bucketBits;
  }

  private void rebuild() { // called when total size == maxSize
    //    System.out.println("[rebuild]+remaining:"+remainingBits+"  tot:"+totSize);
    int SHR = 1;
    if (remainingBits - SHR <= minBits) {
      turnToBucket();
      return;
    }
    deltaForUnsignedCompare = 0;
    MutableLongLongMap newMap = new LongLongHashMap(maxSize);
    //    for(LongLongPair p:hashMap.keyValuesView())
    //      newMap.addToValue(p.getOne()>>>SHR,p.getTwo());
    hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> 1, v));

    while (newMap.size() == maxSize) {
      SHR++;
      if (remainingBits - SHR <= minBits) {
        turnToBucket();
        return;
      }
      newMap.clear();
      //      for(LongLongPair p:hashMap.keyValuesView())
      //        newMap.addToValue(p.getOne()>>>SHR,p.getTwo());
      final int shr = SHR;
      hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> shr, v));
    }
    remainingBits -= SHR;
    hashMap = newMap;
  }

  public void insert(long num, long freq) {
    num >>>= bitsOfValue - remainingBits;
    if (isBucket) {
      bucket[(int) num] += freq;
    } else {
      hashMap.addToValue(num, freq);
      if (hashMap.size() == maxSize) rebuild();
    }
    //    for (LongLongCursor c = hashMap.cursor(); c.moveNext();)
    //      System.out.print("("+c.key()+","+c.value()+")");
    //    System.out.println();
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
          //        System.out.println("[GS find in bucket]"+i + "  " + bucket[i]+"
          // sum:"+(sum-bucket[i]));
        }
        if (sum >= K2 && result.size() == 2) {
          result.add((long) i);
          result.add(sum - bucket[i]);
          break;
        }
      }
    } else {
      totSize = hashMap.size();
      index.size(totSize);
      for (int i = 0; i < totSize; i++) index.set(i, i);
      int tmp = 0;
      for (LongLongPair p : hashMap.keyValuesView()) {
        value[tmp] = p.getOne();
        count[tmp] = p.getTwo();
        tmp++;
      }
      index.sort(
          (x, y) ->
              Long.compare(value[x] ^ deltaForUnsignedCompare, value[y] ^ deltaForUnsignedCompare));
      int x;
      for (int i = 0; i < totSize; i++) {
        x = index.getInt(i);
        //      System.out.println(count[x] + "  " + value[x]);
        sum += count[x];
        if (sum >= K1 && result.size() == 0) {
          result.add(value[x]);
          result.add(sum - count[x]);
          //          System.out.println(count[x] + "  " + value[x]+"   sum:"+(sum-count[x]));
        }
        if (sum >= K2 && result.size() == 2) {
          result.add(value[x]);
          result.add(sum - count[x]);
          break;
        }
      }
    }
    //    System.out.println("[GS][findResultIndex] remainingBits:"+remainingBits
    //        +" result:"+result);
    return result;
  }

  public void reset(int bits, int minBits) {
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
