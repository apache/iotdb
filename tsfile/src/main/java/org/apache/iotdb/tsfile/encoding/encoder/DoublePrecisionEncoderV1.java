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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import java.io.ByteArrayOutputStream;

/** Encoder for int value using gorilla encoding. */
public class DoublePrecisionEncoderV1 extends GorillaEncoderV1 {

  private long preValue;

  public DoublePrecisionEncoderV1() {
    // do nothing
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    if (!flag) {
      // case: write first 8 byte value without any encoding
      flag = true;
      preValue = Double.doubleToLongBits(value);
      leadingZeroNum = Long.numberOfLeadingZeros(preValue);
      tailingZeroNum = Long.numberOfTrailingZeros(preValue);
      byte[] bufferLittle = new byte[8];

      for (int i = 0; i < 8; i++) {
        bufferLittle[i] = (byte) (((preValue) >> (i * 8)) & 0xFF);
      }
      out.write(bufferLittle, 0, bufferLittle.length);
    } else {
      long nextValue = Double.doubleToLongBits(value);
      long tmp = nextValue ^ preValue;
      if (tmp == 0) {
        // case: write '0'
        writeBit(false, out);
      } else {
        int leadingZeroNumTmp = Long.numberOfLeadingZeros(tmp);
        int tailingZeroNumTmp = Long.numberOfTrailingZeros(tmp);
        if (leadingZeroNumTmp >= leadingZeroNum && tailingZeroNumTmp >= tailingZeroNum) {
          // case: write '10' and effective bits without first leadingZeroNum '0'
          // and last tailingZeroNum '0'
          writeBit(true, out);
          writeBit(false, out);
          writeBits(
              tmp, out, TSFileConfig.VALUE_BITS_LENGTH_64BIT - 1 - leadingZeroNum, tailingZeroNum);
        } else {
          // case: write '11', leading zero num of value, effective bits len and effective
          // bit value
          writeBit(true, out);
          writeBit(true, out);
          writeBits(leadingZeroNumTmp, out, TSFileConfig.LEADING_ZERO_BITS_LENGTH_64BIT - 1, 0);
          writeBits(
              (long) TSFileConfig.VALUE_BITS_LENGTH_64BIT - leadingZeroNumTmp - tailingZeroNumTmp,
              out,
              TSFileConfig.DOUBLE_VALUE_LENGTH - 1,
              0);
          writeBits(
              tmp,
              out,
              TSFileConfig.VALUE_BITS_LENGTH_64BIT - 1 - leadingZeroNumTmp,
              tailingZeroNumTmp);
        }
      }
      preValue = nextValue;
      leadingZeroNum = Long.numberOfLeadingZeros(preValue);
      tailingZeroNum = Long.numberOfTrailingZeros(preValue);
    }
  }

  private void writeBits(long num, ByteArrayOutputStream out, int start, int end) {
    for (int i = start; i >= end; i--) {
      long bit = num & (1L << i);
      writeBit(bit, out);
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    encode(Double.NaN, out);
    clearBuffer(out);
    reset();
  }

  @Override
  public int getOneItemMaxSize() {
    // case '11'
    // 2bit + 6bit + 7bit + 64bit = 79bit
    return 10;
  }

  @Override
  public long getMaxByteSize() {
    // max(first 8 byte, case '11' 2bit + 6bit + 7bit + 64bit = 79bit )
    // + NaN(2bit + 6bit + 7bit + 64bit = 79bit) =
    // 158bit
    return 20;
  }
}
