package org.apache.iotdb.tsfile.utils;
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

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class GSHashMapForStat {
  public byte bitsOfValue, remainingBits;
  public int maxSize;

  MutableLongLongMap hashMap;

  public GSHashMapForStat(byte bits, int maxSize) {
    bitsOfValue = remainingBits = bits;
    hashMap = new LongLongHashMap(maxSize);
    this.maxSize = maxSize;
  }

  public GSHashMapForStat(byte bits, int maxSize, byte remaining) {
    this(bits, maxSize);
    remainingBits = remaining;
  }

  public GSHashMapForStat(InputStream inputStream) throws IOException {
    this.bitsOfValue = ReadWriteIOUtils.readByte(inputStream);
    int remaining = this.remainingBits = ReadWriteIOUtils.readByte(inputStream);
    this.maxSize = ReadWriteIOUtils.readInt(inputStream);
    hashMap = new LongLongHashMap(maxSize);
    int size = ReadWriteIOUtils.readInt(inputStream);
    //    System.out.println("\t\t\t\t???!!!"+maxSize+"  size:"+size);
    long bits, freq;
    for (int i = 0; i < size; i++) {
      bits = ReadWriteIOUtils.readLong(inputStream);
      freq = ReadWriteIOUtils.readLong(inputStream);
      insertLongBits(remaining, bits, freq);
    }
    //    System.out.println("\t\t\t\t???___"+hashMap.size()+"  remaining:"+remainingBits);
  }

  public GSHashMapForStat(ByteBuffer byteBuffer) { // deserialize
    this.bitsOfValue = ReadWriteIOUtils.readByte(byteBuffer);
    int remaining = this.remainingBits = ReadWriteIOUtils.readByte(byteBuffer);
    this.maxSize = ReadWriteIOUtils.readInt(byteBuffer);
    hashMap = new LongLongHashMap(maxSize);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    //    System.out.println("\t\t\t\t???!!!"+maxSize+"  size:"+size);
    long bits, freq;
    //    int mmp=0;
    for (int i = 0; i < size; i++) {
      bits = ReadWriteIOUtils.readLong(byteBuffer);
      freq = ReadWriteIOUtils.readLong(byteBuffer);
      insertLongBits(remaining, bits, freq);
      //      mmp++;
      //      if(mmp<10)
      //        System.out.println("\t\t\t\t???..."+bits+"  "+freq+"
      // //"+(bits>>>(bitsOfValue-remainingBits)));
    }
    //    System.out.println("\t\t\t\t???___"+hashMap.size()+"  remaining:"+remainingBits);
  }

  private void rebuild() {
    //    System.out.println("[rebuild]+remaining:"+remainingBits+"  tot:"+totSize);
    int SHR = 1;
    MutableLongLongMap newMap = new LongLongHashMap(maxSize);
    hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> 1, v));

    if (newMap.size() >= maxSize) {
      SHR = 0;
      for (int delta = Integer.highestOneBit(remainingBits - 3); delta > 0; delta >>>= 1)
        if (SHR + delta <= remainingBits - 3) {
          newMap.clear();
          final int shr = SHR + delta;
          hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> shr, v));
          if (newMap.size() >= maxSize) {
            SHR += delta;
          }
        }
      SHR++;
      newMap.clear();
      final int shr = SHR;
      hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> shr, v));
    }
    remainingBits -= SHR;
    hashMap = newMap;
  }

  public void rebuild(int limitedSize) {
    if (hashMap.size() < limitedSize) return;
    int SHR = 1;
    MutableLongLongMap newMap = new LongLongHashMap(maxSize);
    hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> 1, v));

    if (newMap.size() >= limitedSize) {
      SHR = 0;
      for (int delta = Integer.highestOneBit(remainingBits - 3); delta > 0; delta >>>= 1)
        if (SHR + delta <= remainingBits - 3) {
          newMap.clear();
          final int shr = SHR + delta;
          hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> shr, v));
          if (newMap.size() >= limitedSize) {
            SHR += delta;
          }
        }
      SHR++;
      newMap.clear();
      final int shr = SHR;
      hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> shr, v));
    }
    remainingBits -= SHR;
    hashMap = newMap;
  }

  //  public void insert(long num, long freq) {
  //    num >>>= bitsOfValue - remainingBits;
  //    hashMap.addToValue(num,freq);
  //    if (hashMap.size() == maxSize)
  //      rebuild();
  //  }
  private long doubleToLongBits(double data) {
    long longBits = Double.doubleToLongBits(data) + (1L << 63);
    return data >= 0d ? longBits : longBits ^ 0x7FF0000000000000L;
  }

  public void insertDouble(double data) {
    long longBits = doubleToLongBits(data);
    longBits >>>= bitsOfValue - remainingBits;
    hashMap.addToValue(longBits, 1);
    if (hashMap.size() == maxSize) rebuild();
  }

  private long longToLongBits(long data) {
    return data + (1L << 63);
  }

  public void insertLong(long data) {
    long longBits = longToLongBits(data);
    longBits >>>= bitsOfValue - remainingBits;
    hashMap.addToValue(longBits, 1);
    if (hashMap.size() == maxSize) rebuild();
  }

  public void insertLongBits(int remaining, long longBits, long freq) {
    hashMap.addToValue(longBits >>> (remaining - remainingBits), freq);
    if (hashMap.size() == maxSize) rebuild();
  }

  public int getRemainingBits() {
    return remainingBits;
  }

  public int getHashMapSize() {
    return hashMap.size();
  }

  public RichIterable<LongLongPair> getKeyValuesView() {
    return hashMap.keyValuesView();
  }

  public void merge(GSHashMapForStat b) {
    if (b.remainingBits < remainingBits) {
      final int delta = remainingBits - b.remainingBits;
      if (hashMap.size() > 0) {
        MutableLongLongMap newMap = new LongLongHashMap(maxSize);
        hashMap.forEachKeyValue((k, v) -> newMap.addToValue(k >>> delta, v));
        hashMap = newMap;
      }
      remainingBits -= delta;

      for (LongLongPair p : b.hashMap.keyValuesView()) {
        insertLongBits(b.remainingBits, p.getOne(), p.getTwo());
      }
    } else {
      for (LongLongPair p : b.hashMap.keyValuesView()) {
        insertLongBits(b.remainingBits, p.getOne(), p.getTwo());
      }
    }
  }

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(bitsOfValue, outputStream);
    byteLen += ReadWriteIOUtils.write(remainingBits, outputStream);
    byteLen += ReadWriteIOUtils.write(maxSize, outputStream);
    byteLen += ReadWriteIOUtils.write(hashMap.size(), outputStream);
    for (LongLongPair p : hashMap.keyValuesView()) {
      byteLen += ReadWriteIOUtils.write(p.getOne(), outputStream);
      byteLen += ReadWriteIOUtils.write(p.getTwo(), outputStream);
    }
    return byteLen;
  }
}
