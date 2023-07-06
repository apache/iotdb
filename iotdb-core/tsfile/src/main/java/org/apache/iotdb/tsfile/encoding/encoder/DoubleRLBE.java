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

import java.io.ByteArrayOutputStream;

public class DoubleRLBE extends RLBE {
  // delta values
  private final double[] diffValue = new double[blockSize + 1];

  // repeat times on length code
  private final long[] lengRLE = new long[blockSize + 1];

  // previous value of original value
  private double previousValue;

  // constructor of DoubleRLBE
  public DoubleRLBE() {
    super();
    reset();
  }

  protected void reset() {
    writeIndex = -1;
    LengthCode = new int[blockSize + 1];
    for (int i = 0; i < blockSize; i++) {
      diffValue[i] = 0;
      LengthCode[i] = 0;
      byteBuffer = 0;
      numberLeftInBuffer = 0;
      lengRLE[i] = 0;
    }
  }

  /**
   * calculate the binary code length of given long integer.
   *
   * @param val the long integer to calculate length
   * @return the length of val's binary code
   */
  private int calBinarylength(long val) {
    if (val == 0) {
      return 1;
    }
    int i = 64;
    while ((((long) 1 << (i - 1)) & val) == 0 && i > 0) {
      i--;
    }
    return i;
  }

  /**
   * calculate the binary code length of given double note: double is transfered into long first.
   *
   * @param v the double to calculate length
   * @return the length of val's binary code
   */
  private int calBinarylength(double v) {
    long val = Double.doubleToRawLongBits(v);
    if (val == 0) {
      return 1;
    }
    int i = 64;
    while ((((long) 1 << (i - 1)) & val) == 0 && i > 0) {
      i--;
    }
    return i;
  }

  /**
   * encode one input integer value.
   *
   * @param value the integer to be encoded
   * @param out the output stream to flush in when buffer is full
   */
  public void encodeValue(double value, ByteArrayOutputStream out) {
    if (writeIndex == -1) {
      // when the first value hasn't encoded yet
      diffValue[++writeIndex] = value;
      LengthCode[writeIndex] = calBinarylength(value);
      previousValue = value;
      return;
    }
    // calculate delta value
    diffValue[++writeIndex] = value - previousValue;
    // caldulate the length of delta value
    LengthCode[writeIndex] = calBinarylength(diffValue[writeIndex]);
    previousValue = value;
    if (writeIndex == blockSize - 1) {
      // when encoded number reach to blocksize
      flush(out);
    }
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    encodeValue(value, out);
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    flushBlock(out);
  }

  /**
   * calculate fibonacci code of input long integer.
   *
   * @param val the long integer to be fibonacci-encoded
   * @return the reverse fibonacci code of val in binary code
   */
  protected long calcFibonacci(long val) {
    // fibonacci values are stored in Fib
    long[] fib = new long[blockSize * 2 + 1];
    fib[0] = 1;
    fib[1] = 1;
    int i;
    // generate fibonacci values from 1 to the first one larger than val
    for (i = 2; fib[i - 1] <= val; i++) {
      fib[i] = fib[i - 1] + fib[i - 2];
    }

    i--;
    long valfib = 0;
    // calculate fibonacci code
    while (val > 0) {
      while (fib[i] > val && i >= 1) {
        i--;
      }
      valfib |= (1 << (i - 1));
      val -= fib[i];
    }
    return valfib;
  }

  /** run length on DiffValue then store length at the first index in Lengrle. */
  private void rleonlengthcode() {
    int i = 0;
    while (i <= writeIndex) {
      int j = i;
      int temprlecal = 0;
      while (LengthCode[j] == LengthCode[i] && j <= writeIndex) {
        j++;
        temprlecal++;
      }
      // store repeat time at the first repeating value's position
      lengRLE[i] = temprlecal;
      i = j;
    }
  }

  /**
   * flush all encoded values in a block to output stream.
   *
   * @param out the output stream to be flushed to
   */
  protected void flushBlock(ByteArrayOutputStream out) {
    if (writeIndex == -1) {
      return;
    }
    // store the number of values
    writewriteIndex(out);
    // calculate length code of delta binary length
    rleonlengthcode();
    for (int i = 0; i <= writeIndex; i++) {
      if (lengRLE[i] > 0) { // flush the adjacent same length delta values
        flushSegment(i, out);
      }
    }
    clearBuffer(out);
    reset();
  }

  /**
   * flush the adjacent same-length delta values.
   *
   * @param i the position of the first delta value
   * @param out output stream
   */
  private void flushSegment(int i, ByteArrayOutputStream out) {
    // write the first 6 bits: length code in binary words.
    for (int j = 6; j >= 0; j--) {
      if ((LengthCode[i] & (1 << j)) > 0) {
        writeBit(true, out);
      } else {
        writeBit(false, out);
      }
    }
    // write the fibonacci code in normal direction
    long fib = calcFibonacci(lengRLE[i]);
    int fiblen = calBinarylength(fib);
    for (int j = 0; j < fiblen; j++) {
      if ((fib & (1 << j)) > 0) {
        writeBit(true, out);
      } else {
        writeBit(false, out);
      }
    }
    // write '1' to note the end of fibonacci code
    writeBit(true, out);

    // write Binary code words
    int j = i;
    do {
      int tempDifflen = calBinarylength(diffValue[j]);
      long tempDiff = Double.doubleToRawLongBits(diffValue[j]);
      for (int k = tempDifflen - 1; k >= 0; k--) {
        if ((tempDiff & ((long) 1 << k)) > 0) {
          writeBit(true, out);
        } else {
          writeBit(false, out);
        }
      }
      j++;
    } while (lengRLE[j] == 0 && j <= writeIndex);
  }

  @Override
  public int getOneItemMaxSize() {
    return 4 * 4 * 2;
  }

  @Override
  public long getMaxByteSize() {
    return 5L * 4 * blockSize * 2;
  }

  /**
   * write one bit to byteBuffer, when byteBuffer is full, flush byteBuffer to output stream
   *
   * @param b the bit to be written
   * @param out output stream
   */
  protected void writeBit(boolean b, ByteArrayOutputStream out) {
    byteBuffer <<= 1;
    if (b) {
      byteBuffer |= 1;
    }

    numberLeftInBuffer++;
    if (numberLeftInBuffer == 8) {
      clearBuffer(out);
    }
  }

  /**
   * flush bits left in byteBuffer to output stream.
   *
   * @param out output stream
   */
  protected void clearBuffer(ByteArrayOutputStream out) {
    if (numberLeftInBuffer == 0) {
      return;
    }
    if (numberLeftInBuffer > 0) {
      byteBuffer <<= (8 - numberLeftInBuffer);
    }
    out.write(byteBuffer);
    numberLeftInBuffer = 0;
    byteBuffer = 0;
  }

  /**
   * write the number of encoded values to output stream
   *
   * @param out output stream
   */
  private void writewriteIndex(ByteArrayOutputStream out) {
    for (int i = 31; i >= 0; i--) {
      if ((writeIndex + 1 & (1 << i)) > 0) {
        writeBit(true, out);
      } else {
        writeBit(false, out);
      }
    }
  }
}
