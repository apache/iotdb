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

package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FloatBUFFEncoder extends Encoder {

  private static final int lenlen = 10;
  private static final int perlen = 5;
  private static final int maxlen = 10;
  private static final int[] len = {0, 5, 8, 11, 15, 18, 21, 25, 28, 31, 35};
  private static final float eps = (float) 1e-4;

  private int maxFloatLength;
  private boolean first;
  private int minValue, maxValue;
  private int countA, countB;
  private List<Float> li;
  private byte buffer = 0;
  protected int bitsLeft = Byte.SIZE;

  private void reset() {
    maxFloatLength = 0;
    first = true;
    li = new ArrayList<>();
    buffer = 0;
    bitsLeft = Byte.SIZE;
  }

  public FloatBUFFEncoder() {
    super(TSEncoding.BUFF);
    reset();
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    if (first) {
      minValue = (int) Math.floor(value);
      maxValue = (int) Math.ceil(value);
      first = false;
    } else {
      minValue = Math.min(minValue, (int) Math.floor(value));
      maxValue = Math.max(maxValue, (int) Math.ceil(value));
    }
    float tmp = value;
    tmp -= Math.floor(tmp + eps);
    int curFloatLength = 0;
    while (tmp > eps && curFloatLength < maxlen) {
      curFloatLength++;
      tmp *= 10;
      tmp -= Math.floor(tmp + eps);
    }
    maxFloatLength = Math.max(maxFloatLength, curFloatLength);
    li.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (first) {
      writeBits(li.size(), Integer.SIZE, out);
      flushBits(out);
      reset();
      return;
    }
    calc();
    writeBits(li.size(), Integer.SIZE, out);
    writeBits(countA, Integer.SIZE, out);
    writeBits(countB, Integer.SIZE, out);
    writeBits(minValue, Integer.SIZE, out);
    for (float value : li) {
      int partA = (int) Math.floor(value) - minValue;
      writeBits(partA, countA, out);
      float partB = (float) (value - Math.floor(value));
      for (int i = 0; i < countB; i++) {
        partB *= 2;
        if (partB >= 1) {
          writeBit(out);
          partB -= 1;
        } else skipBit(out);
      }
    }
    flushBits(out);
    reset();
  }

  protected void writeBits(int value, int len, ByteArrayOutputStream out) {
    if (len == 0) return;
    writeBits(value >>> 1, len - 1, out);
    if ((value & 1) == 0) skipBit(out);
    else writeBit(out);
  }

  protected void flushBits(ByteArrayOutputStream out) {
    while (bitsLeft != Byte.SIZE) skipBit(out);
  }

  /** Stores a 0 and increases the count of bits by 1 */
  protected void skipBit(ByteArrayOutputStream out) {
    bitsLeft--;
    flipByte(out);
  }

  /** Stores a 1 and increases the count of bits by 1 */
  protected void writeBit(ByteArrayOutputStream out) {
    buffer |= (1 << (bitsLeft - 1));
    bitsLeft--;
    flipByte(out);
  }

  protected void flipByte(ByteArrayOutputStream out) {
    if (bitsLeft == 0) {
      out.write(buffer);
      buffer = 0;
      bitsLeft = Byte.SIZE;
    }
  }

  @Override
  public int getOneItemMaxSize() {
    if (first) return 0;
    calc();
    return countA + countB;
  }

  @Override
  public long getMaxByteSize() {
    if (first) return 0;
    calc();
    return (countA + countB) * li.size() + Integer.SIZE * 3 + Integer.SIZE;
  }

  private void calc() {
    maxFloatLength = Math.min(maxFloatLength, maxlen);
    countA = Long.SIZE - Long.numberOfLeadingZeros(maxValue - minValue);
    if (maxFloatLength > lenlen) countB = len[lenlen] + perlen * (maxFloatLength - lenlen);
    else countB = len[maxFloatLength];
  }
}
