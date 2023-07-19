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
package org.apache.iotdb.lsm.util;

import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.utils.Murmur128Hash;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class BloomFilter implements IDiskEntry {

  private static final int MINIMAL_SIZE = 256;
  private static final int MAXIMAL_HASH_FUNCTION_SIZE = 8;
  private static final int[] SEEDS = new int[] {5, 7, 11, 19, 31, 37, 43, 59};
  private int size;
  private int hashFunctionSize;
  private BitSet bits;
  private HashFunction[] func;

  // do not try to initialize the filter by construction method
  public BloomFilter() {}

  // do not try to initialize the filter by construction method
  private BloomFilter(byte[] bytes, int size, int hashFunctionSize) {
    this.size = size;
    this.hashFunctionSize = hashFunctionSize;
    func = new HashFunction[hashFunctionSize];
    for (int i = 0; i < hashFunctionSize; i++) {
      func[i] = new HashFunction(size, SEEDS[i]);
    }

    bits = BitSet.valueOf(bytes);
  }

  private BloomFilter(int size, int hashFunctionSize) {
    this.size = size;
    this.hashFunctionSize = hashFunctionSize;
    func = new HashFunction[hashFunctionSize];
    for (int i = 0; i < hashFunctionSize; i++) {
      func[i] = new HashFunction(size, SEEDS[i]);
    }

    bits = new BitSet(size);
  }

  /**
   * get empty bloom filter
   *
   * @param errorPercent the tolerant percent of error of the bloom filter
   * @param numOfString the number of string want to store in the bloom filter
   * @return empty bloom
   */
  public static BloomFilter getEmptyBloomFilter(double errorPercent, int numOfString) {
    errorPercent = Math.max(errorPercent, TSFileConfig.MIN_BLOOM_FILTER_ERROR_RATE);
    errorPercent = Math.min(errorPercent, TSFileConfig.MAX_BLOOM_FILTER_ERROR_RATE);

    double ln2 = Math.log(2);
    int size = (int) (-numOfString * Math.log(errorPercent) / ln2 / ln2) + 1;
    int hashFunctionSize = (int) (-Math.log(errorPercent) / ln2) + 1;
    return new BloomFilter(
        Math.max(MINIMAL_SIZE, size), Math.min(MAXIMAL_HASH_FUNCTION_SIZE, hashFunctionSize));
  }

  /**
   * build bloom filter by bytes
   *
   * @param bytes bytes of bits
   * @return bloom filter
   */
  public static BloomFilter buildBloomFilter(byte[] bytes, int size, int hashFunctionSize) {
    return new BloomFilter(bytes, size, Math.min(MAXIMAL_HASH_FUNCTION_SIZE, hashFunctionSize));
  }

  public int getHashFunctionSize() {
    return hashFunctionSize;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public void add(String value) {
    for (HashFunction f : func) {
      bits.set(f.hash(value), true);
    }
  }

  public void add(String tagKey, Collection<String> tagValues) {
    for (HashFunction f : func) {
      int keyHash = f.hash(tagKey) * 31;
      for (String value : tagValues) {
        bits.set(keyHash + f.hash(value));
      }
    }
  }

  public boolean contains(String tagKey, String tagValue) {
    if (tagKey == null || tagValue == null) {
      return false;
    }
    boolean ret = true;
    int index = 0;
    while (ret && index < hashFunctionSize) {
      ret = bits.get(func[index++].hash(tagKey, tagValue));
    }
    return ret;
  }

  public List<Boolean> contains(String tagKey, List<String> tagValues) {
    List<Boolean> results = new ArrayList<>();
    if (tagKey == null) {
      results.add(false);
      return results;
    } else {
      boolean allNull = true;
      for (String tagValue : tagValues) {
        if (tagValue == null) {
          results.add(false);
        } else {
          results.add(true);
          allNull = false;
        }
      }
      if (allNull) {
        return results;
      }
    }
    int tagValueSize = tagValues.size();
    for (int i = 0; i < hashFunctionSize; i++) {
      int currentTagValueIndex = 0;
      int keyHash = func[i].hash(tagKey) * 31;
      while (currentTagValueIndex < tagValueSize) {
        if (results.get(currentTagValueIndex)) {
          results.set(
              currentTagValueIndex,
              bits.get(keyHash + func[i].hash(tagValues.get(currentTagValueIndex))));
        }
        currentTagValueIndex++;
      }
    }
    return results;
  }

  public boolean contains(String value) {
    if (value == null) {
      return false;
    }
    boolean ret = true;
    int index = 0;
    while (ret && index < hashFunctionSize) {
      ret = bits.get(func[index++].hash(value));
    }
    return ret;
  }

  public int getBitCount() {
    int res = 0;
    for (int i = 0; i < size; i++) {
      res += bits.get(i) ? 1 : 0;
    }

    return res;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BloomFilter that = (BloomFilter) o;
    return size == that.size
        && hashFunctionSize == that.hashFunctionSize
        && Objects.equals(bits, that.bits)
        && Arrays.equals(func, that.func);
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, hashFunctionSize, bits, func);
  }

  private class HashFunction {

    private int cap;
    private int seed;

    HashFunction(int cap, int seed) {
      this.cap = cap;
      this.seed = seed;
    }

    public int hash(String value) {
      int res = Murmur128Hash.hash(value, seed);
      if (res == Integer.MIN_VALUE) {
        res = 0;
      }

      return Math.abs(res) % cap;
    }

    public int hash(String... values) {
      int res = 0;
      for (String value : values) {
        res = res * 31 + hash(value);
      }
      return res;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      HashFunction that = (HashFunction) o;
      return cap == that.cap && seed == that.seed;
    }

    @Override
    public int hashCode() {
      return Objects.hash(cap, seed);
    }
  }

  @Override
  public int serialize(DataOutputStream out) throws IOException {
    int len = 0;
    byte[] bytes = bits.toByteArray();
    len = bytes.length;
    len += ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, out);
    out.write(bytes);
    len += ReadWriteForEncodingUtils.writeUnsignedVarInt(size, out);
    len += ReadWriteForEncodingUtils.writeUnsignedVarInt(hashFunctionSize, out);
    return len;
  }

  @Override
  public IDiskEntry deserialize(DataInputStream input) throws IOException {
    int bytesLength = ReadWriteForEncodingUtils.readUnsignedVarInt(input);
    bits = BitSet.valueOf(ReadWriteIOUtils.readBytes(input, bytesLength));
    size = ReadWriteForEncodingUtils.readUnsignedVarInt(input);
    hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(input);
    func = new HashFunction[hashFunctionSize];
    for (int i = 0; i < hashFunctionSize; i++) {
      func[i] = new HashFunction(size, SEEDS[i]);
    }
    return this;
  }
}
