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
 * This class is used to encode(decode) Long in Java with specified bit-width. User need to
 * guarantee that the length of every given Long in binary mode is less than or equal to the
 * bit-width.
 *
 * <p>e.g., if bit-width is 31, then Long '2147483648'(2^31) is not allowed but '2147483647'(2^31-1)
 * is allowed.
 */
public class LongPacker {
  /*
   * For a full example, Width: 3 Input: 5 4 7 3 0 1 3 2
   * Output:
   * +-----------------------+ +-----------------------+ +-----------------------+
   * |1 |0 |1 |1 |0 |0 |1 |1 | |1 |0 |1 |1 |0 |0 |0 |0 | |0 |1 |0 |1 |1 |0 |1 |0 |
   * +-----------------------+ +-----------------------+ +-----------------------+
   * +-----+ +-----+ +---------+ +-----+ +-----+ +---------+ +-----+ +-----+ 5 4 7
   * 3 0 1 3 2
   */
  /** Number of Long values for each pack operation. */
  private static final int NUM_OF_LONGS = 8;
  /** bit-width. */
  private int width;

  public LongPacker(int width) {
    this.width = width;
  }

  /**
   * Encode 8 ({@link LongPacker#NUM_OF_LONGS}) Longs from the array 'values' with specified
   * bit-width to bytes.
   *
   * @param values - array where '8 Longs' are in
   * @param offset - the offset of first Long to be encoded
   * @param buf - encoded bytes, buf size must be equal to ({@link LongPacker#NUM_OF_LONGS}} *
   *     {@link IntPacker#width} / 8)
   */
  public void pack8Values(long[] values, int offset, byte[] buf) {

    int bufIdx = 0;
    int valueIdx = offset;
    // remaining bits for the current unfinished Integer
    int leftBit = 0;

    while (valueIdx < NUM_OF_LONGS + offset) {
      // buffer is used for saving 64 bits as a part of result
      long buffer = 0;
      // remaining size of bits in the 'buffer'
      int leftSize = 64;

      // encode the left bits of current Long to 'buffer'
      if (leftBit > 0) {
        buffer |= (values[valueIdx] << (64 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < NUM_OF_LONGS + offset) {
        // encode one Long to the 'buffer'
        buffer |= (values[valueIdx] << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }
      // If the remaining space of the buffer can not save the bits for one Long
      if (leftSize > 0 && valueIdx < NUM_OF_LONGS + offset) {
        // put the first 'leftSize' bits of the Long into remaining space of the buffer
        buffer |= (values[valueIdx] >>> (width - leftSize));
        leftBit = width - leftSize;
      }

      // put the buffer into the final result
      for (int j = 0; j < 8; j++) {
        buf[bufIdx] = (byte) ((buffer >>> ((8 - j - 1) * 8)) & 0xFF);
        bufIdx++;
        if (bufIdx >= width * 8 / 8) {
          return;
        }
      }
    }
  }

  /**
   * decode values from byte array.
   *
   * @param buf - array where bytes are in.
   * @param offset - offset of first byte to be decoded in buf
   * @param values - decoded result , the size of values should be 8
   */
  public void unpack8Values(byte[] buf, int offset, long[] values) {
    int byteIdx = offset;
    int valueIdx = 0;
    // left bit(s) available for current byte in 'buf'
    int leftBits = 8;
    // bits that has been read for current long value which is to be decoded
    int totalBits = 0;

    // decode long value one by one
    while (valueIdx < 8) {
      // set all the 64 bits in current value to '0'
      values[valueIdx] = 0;
      // read until 'totalBits' is equal to width
      while (totalBits < width) {
        // If 'leftBits' in current byte belongs to current long value
        if (width - totalBits >= leftBits) {
          // then put left bits in current byte to current long value
          values[valueIdx] = values[valueIdx] << leftBits;
          values[valueIdx] = values[valueIdx] | (((1L << leftBits) - 1) & buf[byteIdx]);
          totalBits += leftBits;
          // get next byte
          byteIdx++;
          // set 'leftBits' in next byte to 8 because the next byte has not been used
          leftBits = 8;
          // Else take part of bits in 'leftBits' to current value.
        } else {
          // numbers of bits to be take
          int t = width - totalBits;
          values[valueIdx] = values[valueIdx] << t;
          values[valueIdx] =
              values[valueIdx] | (((1L << leftBits) - 1) & buf[byteIdx]) >>> (leftBits - t);
          leftBits -= t;
          totalBits += t;
        }
      }
      // Start to decode next long value
      valueIdx++;
      totalBits = 0;
    }
  }

  /**
   * decode all values from 'buf' with specified offset and length decoded result will be saved in
   * array named 'values'.
   *
   * @param buf array where all bytes are in.
   * @param length length of bytes to be decoded in buf.
   * @param values decoded result
   */
  public void unpackAllValues(byte[] buf, int length, long[] values) {
    int idx = 0;
    int k = 0;
    while (idx < length) {
      long[] tv = new long[8];
      // decode 8 values one time, current result will be saved in the array named
      // 'tv'
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
