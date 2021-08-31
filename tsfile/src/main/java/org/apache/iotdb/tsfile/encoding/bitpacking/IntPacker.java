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

package org.apache.iotdb.tsfile.encoding.bitpacking;

/**
 * This class is used to encode(decode) Integer in Java with specified bit-width. User need to
 * guarantee that the length of every given Integer in binary mode is less than or equal to the
 * bit-width.
 *
 * <p>e.g., if bit-width is 4, then Integer '16'(10000)b is not allowed but '15'(1111)b is allowed.
 */
public class IntPacker {
  /*
   * For a full example, Width: 3 Input: 5 4 7 3 0 1 3 2
   * Output:
   * +-----------------------+ +-----------------------+ +-----------------------+
   * |1 |0 |1 |1 |0 |0 |1 |1 | |1 |0 |1 |1 |0 |0 |0 |0 | |0 |1 |0 |1 |1 |0 |1 |0 |
   * +-----------------------+ +-----------------------+ +-----------------------+
   * +-----+ +-----+ +---------+ +-----+ +-----+ +---------+ +-----+ +-----+ 5 4 7
   * 3 0 1 3 2
   */
  /** Number of Integers for each pack operation. */
  private static final int NUM_OF_INTS = 8;
  /** bit-width. */
  private int width;

  public IntPacker(int width) {
    this.width = width;
  }

  /**
   * Encode 8 ({@link IntPacker#NUM_OF_INTS}) Integers from the array 'values' with specified
   * bit-width to bytes.
   *
   * @param values - array where '8 Integers' are in
   * @param offset - the offset of first Integer to be encoded
   * @param buf - encoded bytes, buf size must be equal to ({@link IntPacker#NUM_OF_INTS} * {@link
   *     IntPacker#width} / 8)
   */
  public void pack8Values(int[] values, int offset, byte[] buf) {
    int bufIdx = 0;
    int valueIdx = offset;
    // remaining bits for the current unfinished Integer
    int leftBit = 0;

    while (valueIdx < NUM_OF_INTS + offset) {
      // buffer is used for saving 32 bits as a part of result
      int buffer = 0;
      // remaining size of bits in the 'buffer'
      int leftSize = 32;

      // encode the left bits of current Integer to 'buffer'
      if (leftBit > 0) {
        buffer |= (values[valueIdx] << (32 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < NUM_OF_INTS + offset) {
        // encode one Integer to the 'buffer'
        buffer |= (values[valueIdx] << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }
      // If the remaining space of the buffer can not save the bits for one Integer,
      if (leftSize > 0 && valueIdx < NUM_OF_INTS + offset) {
        // put the first 'leftSize' bits of the Integer into remaining space of the
        // buffer
        buffer |= (values[valueIdx] >>> (width - leftSize));
        leftBit = width - leftSize;
      }

      // put the buffer into the final result
      for (int j = 0; j < 4; j++) {
        buf[bufIdx] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
        bufIdx++;
        if (bufIdx >= width) {
          return;
        }
      }
    }
  }

  /**
   * decode Integers from byte array.
   *
   * @param buf - array where bytes are in.
   * @param offset - offset of first byte to be decoded in buf
   * @param values - decoded result , the length of 'values' should be @{link IntPacker#NUM_OF_INTS}
   */
  public void unpack8Values(byte[] buf, int offset, int[] values) {
    int byteIdx = offset;
    long buffer = 0;
    // total bits which have read from 'buf' to 'buffer'. i.e.,
    // number of available bits to be decoded.
    int totalBits = 0;
    int valueIdx = 0;

    while (valueIdx < NUM_OF_INTS) {
      // If current available bits are not enough to decode one Integer,
      // then add next byte from buf to 'buffer' until totalBits >= width
      while (totalBits < width) {
        buffer = (buffer << 8) | (buf[byteIdx] & 0xFF);
        byteIdx++;
        totalBits += 8;
      }

      // If current available bits are enough to decode one Integer,
      // then decode one Integer one by one until left bits in 'buffer' is
      // not enough to decode one Integer.
      while (totalBits >= width && valueIdx < 8) {
        values[valueIdx] = (int) (buffer >>> (totalBits - width));
        valueIdx++;
        totalBits -= width;
        buffer = buffer & ((1 << totalBits) - 1);
      }
    }
  }

  /**
   * decode all values from 'buf' with specified offset and length decoded result will be saved in
   * the array named 'values'.
   *
   * @param buf array where all bytes are in.
   * @param length length of bytes to be decoded in buf.
   * @param values decoded result.
   */
  public void unpackAllValues(byte[] buf, int length, int[] values) {
    int idx = 0;
    int k = 0;
    while (idx < length) {
      int[] tv = new int[8];
      // decode 8 values one time, current result will be saved in the array named 'tv'
      unpack8Values(buf, idx, tv);
      System.arraycopy(tv, 0, values, k, 8);
      idx += width;
      k += 8;
    }
  }

  public void setWidth(int width) {
    this.width = width;
  }
}
