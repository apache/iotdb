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
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.util.ArrayList;
import java.util.List;

public class GSHashMapAggressiveForQuantile {
  private final int maxSize;
  int bitsOfValue, remainingBits;
  private long deltaForUnsignedCompare;
  long DEBUG;
  MutableLongLongMap aggressiveHashMap;
  long hashMapCount;
  long L, R, countL, countR;
  double expectedRoughK;
  long[] value, count;
  IntArrayList index;

  public GSHashMapAggressiveForQuantile(
      int maxSize,
      int bits,
      double expectedRoughK,
      long[] value,
      long[] count,
      IntArrayList index,
      long hashMapCount) {
    this.maxSize = maxSize;
    bitsOfValue = bits;
    remainingBits = bits;
    aggressiveHashMap = new LongLongHashMap(maxSize);
    this.expectedRoughK = expectedRoughK;
    if (bits == 64) deltaForUnsignedCompare = 1L << 63; // unsigned long
    else deltaForUnsignedCompare = 0;
    this.value = value;
    this.count = count;
    this.index = index;
    countL = countR = 0;
    this.hashMapCount = hashMapCount;
  }

  public boolean update(long num, long freq) {
    //    System.out.println("\t aggressive update: "+ num);
    long val = num >>> (bitsOfValue - remainingBits);
    if ((val ^ deltaForUnsignedCompare) < (L ^ deltaForUnsignedCompare)) countL += freq;
    else if ((val ^ deltaForUnsignedCompare) > (R ^ deltaForUnsignedCompare)) countR += freq;
    else {
      hashMapCount += freq;
      aggressiveHashMap.addToValue(val, freq);
      if (aggressiveHashMap.size() == maxSize) {
        int size = aggressiveHashMap.size();
        sortHashMap(size);
        return constructFromSortedArray(size);
      }
    }
    return true;
  }

  public boolean constructFromSortedArray(int length) {
    DEBUG++;
    //    System.out.println("\t[aggressive constructFromSortedArray]");
    double totCount = (countL + countR + hashMapCount);
    //    System.out.println("\t\t\t totCount:"+totCount+"  countL,R,mid:"+countL+" "+countR+"
    // "+hashMapCount);
    if (expectedRoughK * totCount < countL || countL + hashMapCount < expectedRoughK * totCount)
      return false;
    double rate = 0.5;
    int expectPos = 0;
    long tmpCount = countL;
    for (int i = 0; i < length; i++) {
      tmpCount += count[index.getInt(i)];
      if (tmpCount <= expectedRoughK * totCount) expectPos = i;
      else break;
    }
    int expectPosL = (int) (expectPos - length * expectedRoughK * rate);
    int expectPosR = (int) (expectPos + length * (1 - expectedRoughK) * rate);
    double expectV = value[index.getInt(expectPos)] + (1L << (bitsOfValue - remainingBits)) / 2.0;
    expectPosL = Math.max(0, expectPosL);
    expectPosR = Math.min(length - 1, expectPosR);
    //    System.out.println("\t\t\t"+expectPosL+"..."+expectPosR+"  expV:"+expectV);
    //
    // System.out.println("\t\t\t"+value[index.getInt(expectPosL)]+"..."+value[index.getInt(expectPosR)]);
    double avg = 0, sum = 0, sum2 = 0, var = 0.0, v, c, totSize = 0, stdVar;
    int x;
    for (int i = 0; i < length; i++) {
      x = index.getInt(i);
      v = value[x] + (1L << (bitsOfValue - remainingBits)) / 2.0;
      sum += v * count[x];
      sum2 += v * v * count[x];
      totSize += count[x];
    }
    avg = sum / totSize;
    var = sum2 / totSize - avg * avg;
    stdVar = Math.sqrt(Math.max(0, var));
    L = R = -233L;
    aggressiveHashMap.clear();
    hashMapCount = 0;
    for (int i = 0; i < expectPosL; i++) countL += count[index.getInt(i)];
    for (int i = expectPosR + 1; i < length; i++) countR += count[index.getInt(i)];
    for (int i = expectPosL; i <= expectPosR; i++) {
      x = index.getInt(i);
      v = value[x] + (1L << (bitsOfValue - remainingBits)) / 2.0;
      if (stdVar < 1e-9 || Math.abs(v - expectV) <= 4.0 * stdVar) {
        if (aggressiveHashMap.isEmpty()) L = value[x];
        aggressiveHashMap.addToValue(value[x], count[x]);
        hashMapCount += count[x];
        R = value[x];
      } else {
        if (v < expectV) countL += count[x];
        else countR += count[x];
      }
    }
    //    System.out.println("\t\t\t aggressive rebuild over. L,R:"+L+"..."+R+"
    // countL,R,mid:"+countL+" "+countR+" "+hashMapCount);
    return true;
  }

  public int getRemainingBits() {
    return remainingBits;
  }

  private void sortHashMap(int size) {
    index.size(size);
    for (int i = 0; i < size; i++) index.set(i, i);
    int tmp = 0;
    for (LongLongPair p : aggressiveHashMap.keyValuesView()) {
      value[tmp] = p.getOne();
      count[tmp] = p.getTwo();
      tmp++;
    }
    index.sort(
        (x, y) ->
            Long.compare(value[x] ^ deltaForUnsignedCompare, value[y] ^ deltaForUnsignedCompare));
  }

  public List<Long> findResultIndex(long K1, long K2) {
    //    System.out.println("\t[aggressive find] K1,K2:"+K1+" "+K2+"    "+
    //        "L,R:"+L+"..."+R+"  countL,R,mid:"+countL+" "+countR+" "+hashMapCount);
    if (K2 <= countL) return null;
    List<Long> result = new ArrayList<>(8);
    long sum = countL;
    int x, hashMapSize = aggressiveHashMap.size();
    sortHashMap(hashMapSize);
    for (int i = 0; i < hashMapSize; i++) {
      x = index.getInt(i);
      //      System.out.println("\t!! "+count[x] + "  " + value[x]);
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
    return result.size() < 6 ? null : result;
  }
}
