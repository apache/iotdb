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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.lang.Long.rotateLeft;

public class SpookyHashV2Utils {
  private static final long MAGIC_CONSTANT = 0xDEAD_BEEF_DEAD_BEEFL;
  private static final int SHORT_THRESHOLD = 192;
  public static final byte SIZE_OF_LONG = 8;

  public static long getLongFromBytesWithLittleEndian(byte[] data, int index) {

    // little endian
    return (data[index] & 0xFFL)
        | ((data[index + 1] & 0xFFL) << 8)
        | ((data[index + 2] & 0xFFL) << 16)
        | ((data[index + 3] & 0xFFL) << 24)
        | ((data[index + 4] & 0xFFL) << 32)
        | ((data[index + 5] & 0xFFL) << 40)
        | ((data[index + 6] & 0xFFL) << 48)
        | ((data[index + 7] & 0xFFL) << 56);
  }

  public static long getUnsignedIntFromBytesWithLittleEndian(byte[] data, int index) {
    return (data[index] & 0xFFL)
        | ((data[index + 1] & 0xFFL) << 8)
        | ((data[index + 2] & 0xFFL) << 16)
        | ((data[index + 3] & 0xFFL) << 24);
  }

  public static byte[] intToBytes(int value) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES); // Integer.BYTES == 4
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(value);
    return buffer.array();
  }

  public static byte[] longToBytes(long value) {

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES); // Long.BYTES == 8
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(value);
    return buffer.array();
  }

  private SpookyHashV2Utils() {}

  public static byte[] hash32(byte[] data) {
    return intToBytes((int) hash64(data, 0, data.length, 0));
  }

  public static byte[] hash64(byte[] data) {
    return longToBytes(hash64(data, 0, data.length, 0));
  }

  public static long hash64(byte[] data, int offset, int length, long seed) {
    if (length < SHORT_THRESHOLD) {
      return shortHash64(data, offset, length, seed);
    }

    return longHash64(data, offset, length, seed);
  }

  public static int hash32(byte[] data, int offset, int length, long seed) {
    return (int) hash64(data, offset, length, seed);
  }

  private static long shortHash64(byte[] data, int offset, int length, long seed) {
    int limit = offset + length;

    long h0 = seed;
    long h1 = seed;
    long h2 = MAGIC_CONSTANT;
    long h3 = MAGIC_CONSTANT;

    int current = offset;
    while (current <= limit - 32) {
      h2 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h3 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;

      // mix
      h2 = rotateLeft(h2, 50);
      h2 += h3;
      h0 ^= h2;
      h3 = rotateLeft(h3, 52);
      h3 += h0;
      h1 ^= h3;
      h0 = rotateLeft(h0, 30);
      h0 += h1;
      h2 ^= h0;
      h1 = rotateLeft(h1, 41);
      h1 += h2;
      h3 ^= h1;
      h2 = rotateLeft(h2, 54);
      h2 += h3;
      h0 ^= h2;
      h3 = rotateLeft(h3, 48);
      h3 += h0;
      h1 ^= h3;
      h0 = rotateLeft(h0, 38);
      h0 += h1;
      h2 ^= h0;
      h1 = rotateLeft(h1, 37);
      h1 += h2;
      h3 ^= h1;
      h2 = rotateLeft(h2, 62);
      h2 += h3;
      h0 ^= h2;
      h3 = rotateLeft(h3, 34);
      h3 += h0;
      h1 ^= h3;
      h0 = rotateLeft(h0, 5);
      h0 += h1;
      h2 ^= h0;
      h1 = rotateLeft(h1, 36);
      h1 += h2;
      h3 ^= h1;

      h0 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;

      h1 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
    }

    int remainder = limit - current;
    if (remainder >= 16) {
      h2 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      remainder -= SIZE_OF_LONG;

      h3 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      remainder -= SIZE_OF_LONG;

      // mix
      h2 = rotateLeft(h2, 50);
      h2 += h3;
      h0 ^= h2;
      h3 = rotateLeft(h3, 52);
      h3 += h0;
      h1 ^= h3;
      h0 = rotateLeft(h0, 30);
      h0 += h1;
      h2 ^= h0;
      h1 = rotateLeft(h1, 41);
      h1 += h2;
      h3 ^= h1;
      h2 = rotateLeft(h2, 54);
      h2 += h3;
      h0 ^= h2;
      h3 = rotateLeft(h3, 48);
      h3 += h0;
      h1 ^= h3;
      h0 = rotateLeft(h0, 38);
      h0 += h1;
      h2 ^= h0;
      h1 = rotateLeft(h1, 37);
      h1 += h2;
      h3 ^= h1;
      h2 = rotateLeft(h2, 62);
      h2 += h3;
      h0 ^= h2;
      h3 = rotateLeft(h3, 34);
      h3 += h0;
      h1 ^= h3;
      h0 = rotateLeft(h0, 5);
      h0 += h1;
      h2 ^= h0;
      h1 = rotateLeft(h1, 36);
      h1 += h2;
      h3 ^= h1;
    }

    // last 15 bytes
    h3 += ((long) length) << 56;
    switch (remainder) {
      case 15:
        h3 += (data[current + 14] & 0xFFL) << 48;
      case 14:
        h3 += (data[current + 13] & 0xFFL) << 40;
      case 13:
        h3 += (data[current + 12] & 0xFFL) << 32;
      case 12:
        h3 += getUnsignedIntFromBytesWithLittleEndian(data, current + 8);
        h2 += getLongFromBytesWithLittleEndian(data, current);
        break;
      case 11:
        h3 += (data[current + 10] & 0xFFL) << 16;
      case 10:
        h3 += (data[current + 9] & 0xFFL) << 8;
      case 9:
        h3 += (data[current + 8] & 0xFFL);
      case 8:
        h2 += getLongFromBytesWithLittleEndian(data, current);
        break;
      case 7:
        h2 += (data[current + 6] & 0xFFL) << 48;
      case 6:
        h2 += (data[current + 5] & 0xFFL) << 40;
      case 5:
        h2 += (data[current + 4] & 0xFFL) << 32;
      case 4:
        h2 += getUnsignedIntFromBytesWithLittleEndian(data, current);
        break;
      case 3:
        h2 += (data[current + 2] & 0xFFL) << 16;
      case 2:
        h2 += (data[current + 1] & 0xFFL) << 8;
      case 1:
        h2 += (data[current] & 0xFFL);
        break;
      case 0:
        h2 += MAGIC_CONSTANT;
        h3 += MAGIC_CONSTANT;
        break;
      default:
        throw new AssertionError("Unexpected value for remainder: " + remainder);
    }

    // end
    h3 ^= h2;
    h2 = rotateLeft(h2, 15);
    h3 += h2;
    h0 ^= h3;
    h3 = rotateLeft(h3, 52);
    h0 += h3;
    h1 ^= h0;
    h0 = rotateLeft(h0, 26);
    h1 += h0;
    h2 ^= h1;
    h1 = rotateLeft(h1, 51);
    h2 += h1;
    h3 ^= h2;
    h2 = rotateLeft(h2, 28);
    h3 += h2;
    h0 ^= h3;
    h3 = rotateLeft(h3, 9);
    h0 += h3;
    h1 ^= h0;
    h0 = rotateLeft(h0, 47);
    h1 += h0;
    h2 ^= h1;
    h1 = rotateLeft(h1, 54);
    h2 += h1;
    h3 ^= h2;
    h2 = rotateLeft(h2, 32);
    h3 += h2;
    h0 ^= h3;
    h3 = rotateLeft(h3, 25);
    h0 += h3;
    h1 ^= h0;
    h0 = rotateLeft(h0, 63);
    h1 += h0;

    return h0;
  }

  private static long longHash64(byte[] data, int offset, int length, long seed) {
    int limit = offset + length;

    long h0 = seed;
    long h1 = seed;
    long h2 = MAGIC_CONSTANT;
    long h3 = seed;
    long h4 = seed;
    long h5 = MAGIC_CONSTANT;
    long h6 = seed;
    long h7 = seed;
    long h8 = MAGIC_CONSTANT;
    long h9 = seed;
    long h10 = seed;
    long h11 = MAGIC_CONSTANT;

    int current = offset;
    while (current <= limit - 12 * SIZE_OF_LONG) {
      h0 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h2 ^= h10;
      h11 ^= h0;
      h0 = rotateLeft(h0, 11);
      h11 += h1;

      h1 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h3 ^= h11;
      h0 ^= h1;
      h1 = rotateLeft(h1, 32);
      h0 += h2;

      h2 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h4 ^= h0;
      h1 ^= h2;
      h2 = rotateLeft(h2, 43);
      h1 += h3;

      h3 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h5 ^= h1;
      h2 ^= h3;
      h3 = rotateLeft(h3, 31);
      h2 += h4;

      h4 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h6 ^= h2;
      h3 ^= h4;
      h4 = rotateLeft(h4, 17);
      h3 += h5;

      h5 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h7 ^= h3;
      h4 ^= h5;
      h5 = rotateLeft(h5, 28);
      h4 += h6;

      h6 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h8 ^= h4;
      h5 ^= h6;
      h6 = rotateLeft(h6, 39);
      h5 += h7;

      h7 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h9 ^= h5;
      h6 ^= h7;
      h7 = rotateLeft(h7, 57);
      h6 += h8;

      h8 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h10 ^= h6;
      h7 ^= h8;
      h8 = rotateLeft(h8, 55);
      h7 += h9;

      h9 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h11 ^= h7;
      h8 ^= h9;
      h9 = rotateLeft(h9, 54);
      h8 += h10;

      h10 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h0 ^= h8;
      h9 ^= h10;
      h10 = rotateLeft(h10, 22);
      h9 += h11;

      h11 += getLongFromBytesWithLittleEndian(data, current);
      current += SIZE_OF_LONG;
      h1 ^= h9;
      h10 ^= h11;
      h11 = rotateLeft(h11, 46);
      h10 += h0;
    }

    int remaining = limit - current;
    int sequences = remaining / SIZE_OF_LONG;

    // handle remaining whole 8-byte sequences
    switch (sequences) {
      case 11:
        h10 += getLongFromBytesWithLittleEndian(data, current + 10 * SIZE_OF_LONG);
      case 10:
        h9 += getLongFromBytesWithLittleEndian(data, current + 9 * SIZE_OF_LONG);
      case 9:
        h8 += getLongFromBytesWithLittleEndian(data, current + 8 * SIZE_OF_LONG);
      case 8:
        h7 += getLongFromBytesWithLittleEndian(data, current + 7 * SIZE_OF_LONG);
      case 7:
        h6 += getLongFromBytesWithLittleEndian(data, current + 6 * SIZE_OF_LONG);
      case 6:
        h5 += getLongFromBytesWithLittleEndian(data, current + 5 * SIZE_OF_LONG);
      case 5:
        h4 += getLongFromBytesWithLittleEndian(data, current + 4 * SIZE_OF_LONG);
      case 4:
        h3 += getLongFromBytesWithLittleEndian(data, current + 3 * SIZE_OF_LONG);
      case 3:
        h2 += getLongFromBytesWithLittleEndian(data, current + 2 * SIZE_OF_LONG);
      case 2:
        h1 += getLongFromBytesWithLittleEndian(data, current + SIZE_OF_LONG);
      case 1:
        h0 += getLongFromBytesWithLittleEndian(data, current);
      case 0:
        break;
      default:
        throw new AssertionError("Unexpected value for sequences: " + sequences);
    }

    current += SIZE_OF_LONG * sequences;

    // read the last sequence of 0-7 bytes
    long last = 0;
    switch (limit - current) {
      case 7:
        last |= (data[current + 6] & 0xFFL) << 48;
      case 6:
        last |= (data[current + 5] & 0xFFL) << 40;
      case 5:
        last |= (data[current + 4] & 0xFFL) << 32;
      case 4:
        last |= (data[current + 3] & 0xFFL) << 24;
      case 3:
        last |= (data[current + 2] & 0xFFL) << 16;
      case 2:
        last |= (data[current + 1] & 0xFFL) << 8;
      case 1:
        last |= (data[current] & 0xFFL);
      case 0:
        break;
      default:
        throw new AssertionError("Unexpected size for last sequence: " + (limit - current));
    }

    switch (sequences) {
      case 11:
        h11 += last;
        break;
      case 10:
        h10 += last;
        break;
      case 9:
        h9 += last;
        break;
      case 8:
        h8 += last;
        break;
      case 7:
        h7 += last;
        break;
      case 6:
        h6 += last;
        break;
      case 5:
        h5 += last;
        break;
      case 4:
        h4 += last;
        break;
      case 3:
        h3 += last;
        break;
      case 2:
        h2 += last;
        break;
      case 1:
        h1 += last;
        break;
      case 0:
        h0 += last;
        break;
      default:
        throw new AssertionError("Unexpected value for sequences: " + sequences);
    }

    // Place "remaining" as the value of the last byte of the block
    h11 += ((long) remaining) << 56;

    // end 1
    h11 += h1;
    h2 ^= h11;
    h1 = rotateLeft(h1, 44);
    h0 += h2;
    h3 ^= h0;
    h2 = rotateLeft(h2, 15);
    h1 += h3;
    h4 ^= h1;
    h3 = rotateLeft(h3, 34);
    h2 += h4;
    h5 ^= h2;
    h4 = rotateLeft(h4, 21);
    h3 += h5;
    h6 ^= h3;
    h5 = rotateLeft(h5, 38);
    h4 += h6;
    h7 ^= h4;
    h6 = rotateLeft(h6, 33);
    h5 += h7;
    h8 ^= h5;
    h7 = rotateLeft(h7, 10);
    h6 += h8;
    h9 ^= h6;
    h8 = rotateLeft(h8, 13);
    h7 += h9;
    h10 ^= h7;
    h9 = rotateLeft(h9, 38);
    h8 += h10;
    h11 ^= h8;
    h10 = rotateLeft(h10, 53);
    h9 += h11;
    h0 ^= h9;
    h11 = rotateLeft(h11, 42);
    h10 += h0;
    h1 ^= h10;
    h0 = rotateLeft(h0, 54);

    // end 2
    h11 += h1;
    h2 ^= h11;
    h1 = rotateLeft(h1, 44);
    h0 += h2;
    h3 ^= h0;
    h2 = rotateLeft(h2, 15);
    h1 += h3;
    h4 ^= h1;
    h3 = rotateLeft(h3, 34);
    h2 += h4;
    h5 ^= h2;
    h4 = rotateLeft(h4, 21);
    h3 += h5;
    h6 ^= h3;
    h5 = rotateLeft(h5, 38);
    h4 += h6;
    h7 ^= h4;
    h6 = rotateLeft(h6, 33);
    h5 += h7;
    h8 ^= h5;
    h7 = rotateLeft(h7, 10);
    h6 += h8;
    h9 ^= h6;
    h8 = rotateLeft(h8, 13);
    h7 += h9;
    h10 ^= h7;
    h9 = rotateLeft(h9, 38);
    h8 += h10;
    h11 ^= h8;
    h10 = rotateLeft(h10, 53);
    h9 += h11;
    h0 ^= h9;
    h11 = rotateLeft(h11, 42);
    h10 += h0;
    h1 ^= h10;
    h0 = rotateLeft(h0, 54);

    // end 3
    h11 += h1;
    h2 ^= h11;
    h1 = rotateLeft(h1, 44);
    h0 += h2;
    h3 ^= h0;
    h2 = rotateLeft(h2, 15);
    h1 += h3;
    h4 ^= h1;
    h3 = rotateLeft(h3, 34);
    h2 += h4;
    h5 ^= h2;
    h4 = rotateLeft(h4, 21);
    h3 += h5;
    h6 ^= h3;
    h5 = rotateLeft(h5, 38);
    h4 += h6;
    h7 ^= h4;
    h6 = rotateLeft(h6, 33);
    h5 += h7;
    h8 ^= h5;
    h7 = rotateLeft(h7, 10);
    h6 += h8;
    h9 ^= h6;
    h8 = rotateLeft(h8, 13);
    h7 += h9;
    h10 ^= h7;
    h9 = rotateLeft(h9, 38);
    h8 += h10;
    h11 ^= h8;
    h10 = rotateLeft(h10, 53);
    h9 += h11;
    h0 ^= h9;
    h11 = rotateLeft(h11, 42);
    h10 += h0;
    h1 ^= h10;
    h0 = rotateLeft(h0, 54);

    return h0;
  }
}
