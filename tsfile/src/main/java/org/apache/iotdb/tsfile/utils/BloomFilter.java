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
package org.apache.iotdb.tsfile.utils;

import java.util.BitSet;

public class BloomFilter {

  private static final int[] seeds = new int[]{5, 7, 11, 13, 31, 37, 61};
  private int size = 256;
  private BitSet bits;
  private HashFunction[] func = new HashFunction[seeds.length];

  // do not try to initialize the filter by construction method
  private BloomFilter(byte[] bytes, int size) {
    this.size = size;
    for (int i = 0; i < seeds.length; i++) {
      func[i] = new HashFunction(size, seeds[i]);
    }
    bits = BitSet.valueOf(bytes);
  }

  private BloomFilter(int size) {
    this.size = size;
    for (int i = 0; i < seeds.length; i++) {
      func[i] = new HashFunction(size, seeds[i]);
    }
    bits = new BitSet(size);
  }

  /**
   * get empty bloom filter
   *
   * @param size size of the filter
   * @return empty bloom
   */
  public static BloomFilter getEmptyBloomFilter(int size) {
    return new BloomFilter(size);
  }

  /**
   * build bloom filter by bytes
   *
   * @param bytes bytes of bits
   * @return bloom filter
   */
  public static BloomFilter buildBloomFilter(byte[] bytes, int size) {
    return new BloomFilter(bytes, size);
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public void add(String value) {
    int hash = value.hashCode();
    for (HashFunction f : func) {
      bits.set(f.hash(hash), true);
    }
  }

  public boolean contains(String value) {
    if (value == null) {
      return false;
    }
    boolean ret = true;
    int index = 0;
    int hash = value.hashCode();
    while(ret && index < seeds.length){
      ret = bits.get(func[index++].hash(hash));
    }

    return ret;
  }

  public byte[] serialize() {
    return bits.toByteArray();
  }

  private class HashFunction {

    private int cap;
    private int seed;

    private HashFunction(int cap, int seed) {
      this.cap = cap;
      this.seed = seed;
    }

    public int hash(int h) {
      return (cap - 1) & (h * seed);
    }
  }
}