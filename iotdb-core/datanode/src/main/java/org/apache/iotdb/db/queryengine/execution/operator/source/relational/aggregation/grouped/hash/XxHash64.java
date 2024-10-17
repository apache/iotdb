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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.Long.rotateLeft;
import static org.apache.tsfile.utils.BytesUtils.bytesToInt;
import static org.apache.tsfile.utils.BytesUtils.bytesToLongFromOffset;

public class XxHash64 {
  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2b2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;
  private static final long DEFAULT_SEED = 0L;
  private final long seed;
  private static final int BUFFER_ADDRESS;
  private final byte[] buffer = new byte[32];
  private int bufferSize;
  private long bodyLength;
  private long v1;
  private long v2;
  private long v3;
  private long v4;

  public static final long TRUE_XX_HASH = XxHash64.hash(1);
  public static final long FALSE_XX_HASH = XxHash64.hash(0);

  public XxHash64() {
    this(0L);
  }

  // copy from
  // https://github.com/airlift/slice/blob/master/src/main/java/io/airlift/slice/XxHash64.java
  public XxHash64(long seed) {
    this.seed = seed;
    this.v1 = seed + PRIME64_1 + PRIME64_2;
    this.v2 = seed + PRIME64_2;
    this.v3 = seed;
    this.v4 = seed - PRIME64_1;
  }

  public XxHash64 update(byte[] data) {
    return this.update(data, 0, data.length);
  }

  public XxHash64 update(byte[] data, int offset, int length) {
    // Objects.checkFromIndexSize(offset, length, data.length);
    this.updateHash(data, BUFFER_ADDRESS + offset, length);
    return this;
  }

  public long hash() {
    long hash;
    if (this.bodyLength > 0L) {
      hash = this.computeBody();
    } else {
      hash = this.seed + PRIME64_5;
    }

    hash += this.bodyLength + (long) this.bufferSize;
    return updateTail(hash, this.buffer, BUFFER_ADDRESS, 0, this.bufferSize);
  }

  private long computeBody() {
    long hash =
        rotateLeft(this.v1, 1)
            + rotateLeft(this.v2, 7)
            + rotateLeft(this.v3, 12)
            + rotateLeft(this.v4, 18);
    hash = update(hash, this.v1);
    hash = update(hash, this.v2);
    hash = update(hash, this.v3);
    hash = update(hash, this.v4);
    return hash;
  }

  private void updateHash(byte[] base, int address, int length) {
    int index;
    if (this.bufferSize > 0) {
      index = Math.min(32 - this.bufferSize, length);
      System.arraycopy(base, address, this.buffer, BUFFER_ADDRESS + this.bufferSize, index);
      this.bufferSize += index;
      address += index;
      length -= index;
      if (this.bufferSize == 32) {
        this.updateBody(this.buffer, BUFFER_ADDRESS, this.bufferSize);
        this.bufferSize = 0;
      }
    }

    if (length >= 32) {
      index = this.updateBody(base, address, length);
      address += index;
      length -= index;
    }

    if (length > 0) {
      System.arraycopy(base, address, this.buffer, BUFFER_ADDRESS, length);
      this.bufferSize = length;
    }
  }

  private int updateBody(byte[] base, int address, int length) {
    int remaining;
    for (remaining = length; remaining >= 32; remaining -= 32) {
      this.v1 = mix(this.v1, bytesToLongFromOffset(base, Long.BYTES, address));
      this.v2 = mix(this.v2, bytesToLongFromOffset(base, Long.BYTES, address + 8));
      this.v3 = mix(this.v3, bytesToLongFromOffset(base, Long.BYTES, address + 16));
      this.v4 = mix(this.v4, bytesToLongFromOffset(base, Long.BYTES, address + 24));
      address += 32;
    }

    int index = length - remaining;
    this.bodyLength += index;
    return index;
  }

  public static long hash(long value) {
    return hash(0L, value);
  }

  public static long hash(long seed, long value) {
    long hash = seed + PRIME64_5 + 8L;
    hash = updateTail(hash, value);
    hash = finalShuffle(hash);
    return hash;
  }

  public static long hash(InputStream in) throws IOException {
    return hash(0L, in);
  }

  public static long hash(long seed, InputStream in) throws IOException {
    XxHash64 hash = new XxHash64(seed);
    byte[] buffer = new byte[8192];

    while (true) {
      int length = in.read(buffer);
      if (length == -1) {
        return hash.hash();
      }

      hash.update(buffer, 0, length);
    }
  }

  public static long hash(byte[] data) {
    return hash(data, 0, data.length);
  }

  public static long hash(long seed, byte[] data) {
    return hash(seed, data, 0, data.length);
  }

  public static long hash(byte[] data, int offset, int length) {
    return hash(0L, data, offset, length);
  }

  public static long hash(long seed, byte[] data, int offset, int length) {
    // Objects.checkFromIndexSize(offset, length, data.length());
    int address = BUFFER_ADDRESS + offset;
    long hash;
    if (length >= 32) {
      hash = updateBody(seed, data, address, length);
    } else {
      hash = seed + PRIME64_5;
    }

    hash += length;
    // round to the closest 32 byte boundary
    // this is the point up to which updateBody() processed
    int index = length & 0xFFFFFFE0;
    return updateTail(hash, data, address, index, length);
  }

  private static long updateTail(long hash, byte[] base, int address, int index, int length) {
    while (index <= length - 8) {
      hash = updateTail(hash, bytesToLongFromOffset(base, Long.BYTES, address + index));
      index += 8;
    }

    if (index <= length - 4) {
      hash = updateTail(hash, bytesToInt(base, address + index));
      index += 4;
    }

    while (index < length) {
      hash = updateTail(hash, base[address + index]);
      ++index;
    }

    hash = finalShuffle(hash);
    return hash;
  }

  private static long updateBody(long seed, byte[] base, int address, int length) {
    long v1 = seed + PRIME64_1 + PRIME64_2;
    long v2 = seed + PRIME64_2;
    long v3 = seed;
    long v4 = seed - PRIME64_1;

    for (int remaining = length; remaining >= 32; remaining -= 32) {
      v1 = mix(v1, bytesToLongFromOffset(base, Long.BYTES, address));
      v2 = mix(v2, bytesToLongFromOffset(base, Long.BYTES, address + 8));
      v3 = mix(v3, bytesToLongFromOffset(base, Long.BYTES, address + 16));
      v4 = mix(v4, bytesToLongFromOffset(base, Long.BYTES, address + 24));
      address += 32;
    }

    long hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
    hash = update(hash, v1);
    hash = update(hash, v2);
    hash = update(hash, v3);
    hash = update(hash, v4);
    return hash;
  }

  private static long mix(long current, long value) {
    return rotateLeft(current + value * PRIME64_2, 31) * PRIME64_1;
  }

  private static long update(long hash, long value) {
    long temp = hash ^ mix(0, value);
    return temp * PRIME64_1 + PRIME64_4;
  }

  private static long updateTail(long hash, long value) {
    long temp = hash ^ mix(0, value);
    return rotateLeft(temp, 27) * PRIME64_1 + PRIME64_4;
  }

  private static long updateTail(long hash, int value) {
    long unsigned = value & 0xFFFF_FFFFL;
    long temp = hash ^ (unsigned * PRIME64_1);
    return rotateLeft(temp, 23) * PRIME64_2 + PRIME64_3;
  }

  private static long updateTail(long hash, byte value) {
    int unsigned = value & 0xFF;
    long temp = hash ^ (unsigned * PRIME64_5);
    return rotateLeft(temp, 11) * PRIME64_1;
  }

  private static long finalShuffle(long hash) {
    hash ^= hash >>> 33;
    hash *= PRIME64_2;
    hash ^= hash >>> 29;
    hash *= PRIME64_3;
    hash ^= hash >>> 32;
    return hash;
  }

  static {
    BUFFER_ADDRESS = 0;
  }
}
