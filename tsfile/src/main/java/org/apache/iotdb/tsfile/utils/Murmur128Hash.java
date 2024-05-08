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

public class Murmur128Hash {

  private Murmur128Hash() {
    // util class
  }

  /**
   * get hashcode of value by seed
   *
   * @param value value
   * @param seed seed
   * @return hashcode of value
   */
  public static int hash(String value, int seed) {
    return (int) innerHash(value.getBytes(), 0, value.getBytes().length, seed);
  }

  /**
   * get hashcode of two values by seed
   *
   * @param value1 the first value
   * @param value2 the second value
   * @param seed seed
   * @return hashcode of value
   */
  public static int hash(String value1, long value2, int seed) {
    return (int)
        innerHash(
            BytesUtils.concatByteArray(value1.getBytes(), BytesUtils.longToBytes(value2)),
            0,
            value1.length() + 8,
            seed);
  }

  /** Methods to perform murmur 128 hash. */
  private static long getBlock(byte[] key, int offset, int index) {
    int i8 = index << 3;
    int blockOffset = offset + i8;
    return ((long) key[blockOffset] & 0xff)
        + (((long) key[blockOffset + 1] & 0xff) << 8)
        + (((long) key[blockOffset + 2] & 0xff) << 16)
        + (((long) key[blockOffset + 3] & 0xff) << 24)
        + (((long) key[blockOffset + 4] & 0xff) << 32)
        + (((long) key[blockOffset + 5] & 0xff) << 40)
        + (((long) key[blockOffset + 6] & 0xff) << 48)
        + (((long) key[blockOffset + 7] & 0xff) << 56);
  }

  private static long rotl64(long v, int n) {
    return ((v << n) | (v >>> (64 - n)));
  }

  private static long fmix(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }

  private static long innerHash(byte[] key, int offset, int length, long seed) {
    final int nblocks = length >> 4; // Process as 128-bit blocks.
    long h1 = seed;
    long h2 = seed;
    long c1 = 0x87c37b91114253d5L;
    long c2 = 0x4cf5ad432745937fL;
    // ----------
    // body
    for (int i = 0; i < nblocks; i++) {
      long k1 = getBlock(key, offset, i * 2);
      long k2 = getBlock(key, offset, i * 2 + 1);
      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
      h1 = rotl64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;
      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      h2 = rotl64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }
    // ----------
    // tail
    // Advance offset to the unprocessed tail of the data.
    offset += nblocks * 16;
    long k1 = 0;
    long k2 = 0;
    switch (length & 15) {
      case 15:
        k2 ^= ((long) key[offset + 14]) << 48;
        // fallthrough
      case 14:
        k2 ^= ((long) key[offset + 13]) << 40;
        // fallthrough
      case 13:
        k2 ^= ((long) key[offset + 12]) << 32;
        // fallthrough
      case 12:
        k2 ^= ((long) key[offset + 11]) << 24;
        // fallthrough
      case 11:
        k2 ^= ((long) key[offset + 10]) << 16;
        // fallthrough
      case 10:
        k2 ^= ((long) key[offset + 9]) << 8;
        // fallthrough
      case 9:
        k2 ^= key[offset + 8];
        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;
        // fallthrough
      case 8:
        k1 ^= ((long) key[offset + 7]) << 56;
        // fallthrough
      case 7:
        k1 ^= ((long) key[offset + 6]) << 48;
        // fallthrough
      case 6:
        k1 ^= ((long) key[offset + 5]) << 40;
        // fallthrough
      case 5:
        k1 ^= ((long) key[offset + 4]) << 32;
        // fallthrough
      case 4:
        k1 ^= ((long) key[offset + 3]) << 24;
        // fallthrough
      case 3:
        k1 ^= ((long) key[offset + 2]) << 16;
        // fallthrough
      case 2:
        k1 ^= ((long) key[offset + 1]) << 8;
        // fallthrough
      case 1:
        k1 ^= (key[offset]);
        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
        break;
      default: // 0
        // do nothing

    }
    // ----------
    // finalization
    h1 ^= length;
    h2 ^= length;
    h1 += h2;
    h2 += h1;
    h1 = fmix(h1);
    h2 = fmix(h2);
    h1 += h2;
    h2 += h1;
    return h1 + h2;
  }
}
