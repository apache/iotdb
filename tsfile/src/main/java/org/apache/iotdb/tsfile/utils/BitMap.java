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

import java.util.Arrays;

public class BitMap {
  private static final byte[] BIT_UTIL = new byte[] {1, 2, 4, 8, 16, 32, 64, -128};
  private static final byte[] UNMARK_BIT_UTIL =
      new byte[] {
        (byte) 0XFE,
        (byte) 0XFD,
        (byte) 0XFB,
        (byte) 0XF7,
        (byte) 0XEF,
        (byte) 0XDF,
        (byte) 0XBF,
        (byte) 0X7F
      };

  private byte[] bits;
  private int size;

  public BitMap(int size) {
    this.size = size;
    bits = new byte[size / Byte.SIZE + 1];
    Arrays.fill(bits, (byte) 0);
  }

  public BitMap(int size, byte[] bits) {
    this.size = size;
    this.bits = bits;
  }

  public byte[] getByteArray() {
    return this.bits;
  }

  public int getSize() {
    return this.size;
  }

  /** returns the value of the bit with the specified index. */
  public boolean get(int position) {
    return (bits[position / Byte.SIZE] & BIT_UTIL[position % Byte.SIZE]) != 0;
  }

  /** mark as 1 at the given bit position */
  public void mark(int position) {
    bits[position / Byte.SIZE] |= BIT_UTIL[position % Byte.SIZE];
  }

  /** mark as 0 at all positions */
  public void reset() {
    Arrays.fill(bits, (byte) 0);
  }

  public void unmark(int position) {
    bits[position / Byte.SIZE] &= UNMARK_BIT_UTIL[position % Byte.SIZE];
  }

  /** whether all bits are zero */
  public boolean isAllZero() {
    int j;
    for (j = 0; j < size / Byte.SIZE; j++) {
      if (bits[j] != (byte) 0) {
        return false;
      }
    }
    for (j = 0; j < size % Byte.SIZE; j++) {
      if ((bits[size / Byte.SIZE] & BIT_UTIL[j]) != 0) {
        return false;
      }
    }
    return true;
  }

  /** whether all bits are one */
  public boolean isAllOne() {
    int j;
    for (j = 0; j < size / Byte.SIZE; j++) {
      if (bits[j] != (byte) 0XFF) {
        return false;
      }
    }
    for (j = 0; j < size % Byte.SIZE; j++) {
      if ((bits[size / Byte.SIZE] & BIT_UTIL[j]) == 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuffer res = new StringBuffer();
    for (int i = 0; i < size; i++) {
      res.append(get(i) ? 1 : 0);
    }
    return res.toString();
  }
}
