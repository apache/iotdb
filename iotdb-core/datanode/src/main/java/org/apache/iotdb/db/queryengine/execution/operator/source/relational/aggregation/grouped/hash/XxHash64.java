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

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;

public class XxHash64 {
  private static final long PRIME64_1 = -7046029288634856825L;
  private static final long PRIME64_2 = -4417276706812531889L;
  private static final long PRIME64_3 = 1609587929392839161L;
  private static final long PRIME64_4 = -8796714831421723037L;
  private static final long PRIME64_5 = 2870177450012600261L;
  private static final long DEFAULT_SEED = 0L;
  private final long seed;
  private static final long BUFFER_ADDRESS;
  private final byte[] buffer;
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

  public XxHash64(long seed) {
    this.buffer = new byte[32];
    this.seed = seed;
    this.v1 = seed + -7046029288634856825L + -4417276706812531889L;
    this.v2 = seed + -4417276706812531889L;
    this.v3 = seed;
    this.v4 = seed - -7046029288634856825L;
  }

  public XxHash64 update(byte[] data) {
    return this.update((byte[]) data, 0, data.length);
  }

  public XxHash64 update(byte[] data, int offset, int length) {
    // Objects.checkFromIndexSize(offset, length, data.length);
    this.updateHash(data, (long) (Unsafe.ARRAY_BYTE_BASE_OFFSET + offset), length);
    return this;
  }

  public long hash() {
    long hash;
    if (this.bodyLength > 0L) {
      hash = this.computeBody();
    } else {
      hash = this.seed + 2870177450012600261L;
    }

    hash += this.bodyLength + (long) this.bufferSize;
    return updateTail(hash, this.buffer, BUFFER_ADDRESS, 0, this.bufferSize);
  }

  private long computeBody() {
    long hash =
        Long.rotateLeft(this.v1, 1)
            + Long.rotateLeft(this.v2, 7)
            + Long.rotateLeft(this.v3, 12)
            + Long.rotateLeft(this.v4, 18);
    hash = update(hash, this.v1);
    hash = update(hash, this.v2);
    hash = update(hash, this.v3);
    hash = update(hash, this.v4);
    return hash;
  }

  private void updateHash(byte[] base, long address, int length) {
    int index;
    if (this.bufferSize > 0) {
      index = Math.min(32 - this.bufferSize, length);
      JvmUtils.unsafe.copyMemory(
          base, address, this.buffer, BUFFER_ADDRESS + (long) this.bufferSize, (long) index);
      this.bufferSize += index;
      address += (long) index;
      length -= index;
      if (this.bufferSize == 32) {
        this.updateBody(this.buffer, BUFFER_ADDRESS, this.bufferSize);
        this.bufferSize = 0;
      }
    }

    if (length >= 32) {
      index = this.updateBody(base, address, length);
      address += (long) index;
      length -= index;
    }

    if (length > 0) {
      JvmUtils.unsafe.copyMemory(base, address, this.buffer, BUFFER_ADDRESS, (long) length);
      this.bufferSize = length;
    }
  }

  private int updateBody(byte[] base, long address, int length) {
    int remaining;
    for (remaining = length; remaining >= 32; remaining -= 32) {
      this.v1 = mix(this.v1, JvmUtils.unsafe.getLong(base, address));
      this.v2 = mix(this.v2, JvmUtils.unsafe.getLong(base, address + 8L));
      this.v3 = mix(this.v3, JvmUtils.unsafe.getLong(base, address + 16L));
      this.v4 = mix(this.v4, JvmUtils.unsafe.getLong(base, address + 24L));
      address += 32L;
    }

    int index = length - remaining;
    this.bodyLength += (long) index;
    return index;
  }

  public static long hash(long value) {
    return hash(0L, value);
  }

  public static long hash(long seed, long value) {
    long hash = seed + 2870177450012600261L + 8L;
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

      hash.update((byte[]) buffer, 0, length);
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
    long address = (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + (long) offset;
    long hash;
    if (length >= 32) {
      hash = updateBody(seed, data, address, length);
    } else {
      hash = seed + 2870177450012600261L;
    }

    hash += length;
    int index = length & -32;
    return updateTail(hash, data, address, index, length);
  }

  private static long updateTail(long hash, byte[] base, long address, int index, int length) {
    while (index <= length - 8) {
      hash = updateTail(hash, JvmUtils.unsafe.getLong(base, address + (long) index));
      index += 8;
    }

    if (index <= length - 4) {
      hash = updateTail(hash, JvmUtils.unsafe.getInt(base, address + (long) index));
      index += 4;
    }

    while (index < length) {
      hash = updateTail(hash, JvmUtils.unsafe.getByte(base, address + (long) index));
      ++index;
    }

    hash = finalShuffle(hash);
    return hash;
  }

  private static long updateBody(long seed, byte[] base, long address, int length) {
    long v1 = seed + -7046029288634856825L + -4417276706812531889L;
    long v2 = seed + -4417276706812531889L;
    long v3 = seed;
    long v4 = seed - -7046029288634856825L;

    for (int remaining = length; remaining >= 32; remaining -= 32) {
      v1 = mix(v1, JvmUtils.unsafe.getLong(base, address));
      v2 = mix(v2, JvmUtils.unsafe.getLong(base, address + 8L));
      v3 = mix(v3, JvmUtils.unsafe.getLong(base, address + 16L));
      v4 = mix(v4, JvmUtils.unsafe.getLong(base, address + 24L));
      address += 32L;
    }

    long hash =
        Long.rotateLeft(v1, 1)
            + Long.rotateLeft(v2, 7)
            + Long.rotateLeft(v3, 12)
            + Long.rotateLeft(v4, 18);
    hash = update(hash, v1);
    hash = update(hash, v2);
    hash = update(hash, v3);
    hash = update(hash, v4);
    return hash;
  }

  private static long mix(long current, long value) {
    return Long.rotateLeft(current + value * -4417276706812531889L, 31) * -7046029288634856825L;
  }

  private static long update(long hash, long value) {
    long temp = hash ^ mix(0L, value);
    return temp * -7046029288634856825L + -8796714831421723037L;
  }

  private static long updateTail(long hash, long value) {
    long temp = hash ^ mix(0L, value);
    return Long.rotateLeft(temp, 27) * -7046029288634856825L + -8796714831421723037L;
  }

  private static long updateTail(long hash, int value) {
    long unsigned = (long) value & 4294967295L;
    long temp = hash ^ unsigned * -7046029288634856825L;
    return Long.rotateLeft(temp, 23) * -4417276706812531889L + 1609587929392839161L;
  }

  private static long updateTail(long hash, byte value) {
    int unsigned = value & 255;
    long temp = hash ^ (long) unsigned * 2870177450012600261L;
    return Long.rotateLeft(temp, 11) * -7046029288634856825L;
  }

  private static long finalShuffle(long hash) {
    hash ^= hash >>> 33;
    hash *= -4417276706812531889L;
    hash ^= hash >>> 29;
    hash *= 1609587929392839161L;
    hash ^= hash >>> 32;
    return hash;
  }

  static {
    BUFFER_ADDRESS = Unsafe.ARRAY_BYTE_BASE_OFFSET;
  }
}
