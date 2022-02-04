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
import java.io.IOException;

public class IntRLBE extends RLBE {
  /** delta values */
  private int[] DiffValue = new int[blockSize + 1];

  /** repeat times on length code */
  private int[] Lengrle = new int[blockSize + 1];

  /** previous value of original value */
  private int previousvalue;

  /** constructor of IntRLBE */
  public IntRLBE() {
    super();
    reset();
  }

  protected void reset() {
    writeIndex = -1;
    LengthCode = new int[blockSize + 1];
    for (int i = 0; i < blockSize; i++) {
      DiffValue[i] = 0;
      LengthCode[i] = 0;
      byteBuffer = 0;
      numberLeftInBuffer = 0;
      Lengrle[i] = 0;
    }
  }

  /**
   * calculate the binary code length of given integer
   *
   * @param val the integer to calculate length
   * @return the length of val's binary code
   */
  private int calBinarylength(int val) {
    if (val == 0) return 1;
    int i = 32;
    while (((1 << (i - 1)) & val) == 0 && i > 0) i--;
    return i;
  }

  /**
   * encode one input integer value
   *
   * @param value the integer to be encoded
   * @param out the output stream to flush in when buffer is full
   */
  public void encodeValue(int value, ByteArrayOutputStream out) {
    if (writeIndex == -1) {
      // when the first value hasn't encoded yet
      DiffValue[++writeIndex] = value;
      LengthCode[writeIndex] = calBinarylength(value);
      previousvalue = value;
      return;
    }
    // calculate delta value
    DiffValue[++writeIndex] = value - previousvalue;
    // caldulate the length of delta value
    LengthCode[writeIndex] = calBinarylength(DiffValue[writeIndex]);
    previousvalue = value;
    if (writeIndex == blockSize - 1) {
      // when encoded number reach to blocksize
      flush(out);
    }
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    encodeValue(value, out);
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    flushBlock(out);
  }

  /**
   * calculate fibonacci code of input integer
   *
   * @param val the integer to be fibonacci-encoded
   * @return the reverse fibonacci code of val in binary code
   */
  protected int calcFibonacci(int val) {
    // fibonacci values are stored in Fib
    int[] Fib = new int[blockSize * 2 + 1];
    Fib[0] = 1;
    Fib[1] = 1;
    int i;
    // generate fibonacci values from 1 to the first one larger than val
    for (i = 2; Fib[i - 1] <= val; i++) {
      Fib[i] = Fib[i - 1] + Fib[i - 2];
    }

    i--;
    int valfib = 0;
    // calculate fibonacci code
    while (val > 0) {
      while (Fib[i] > val && i >= 1) i--;
      valfib |= (1 << (i - 1));
      val -= Fib[i];
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
      Lengrle[i] = temprlecal;
      i = j;
    }
  }

  /**
   * flush all encoded values in a block to output stream
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
      if (Lengrle[i] > 0) // flush the adjacent same length delta values
      try {
          flushsegment(i, out);
        } catch (IOException e) {
          logger.error("flush data to stream failed!", e);
        }
    }
    clearBuffer(out);
    reset();
  }

  /**
   * flush the adjacent same-length delta values
   *
   * @param i the position of the first delta value
   * @param out output stream
   * @throws IOException
   */
  private void flushsegment(int i, ByteArrayOutputStream out) throws IOException {
    // write the first 6 bits: length code in binary words.
    for (int j = 5; j >= 0; j--) {
      if ((LengthCode[i] & (1 << j)) > 0) writeBit(true, out);
      else writeBit(false, out);
    }
    // write the fibonacci code in normal direction
    int fib = calcFibonacci(Lengrle[i]);
    int fiblen = calBinarylength(fib);
    for (int j = 0; j < fiblen; j++) {
      if ((fib & (1 << j)) > 0) writeBit(true, out);
      else writeBit(false, out);
    }
    // write '1' to note the end of fibonacci code
    writeBit(true, out);

    // write Binary code words
    int j = i;
    do {
      int tempDifflen = calBinarylength(DiffValue[j]);
      for (int k = tempDifflen - 1; k >= 0; k--) {
        if ((DiffValue[j] & (1 << k)) > 0) writeBit(true, out);
        else writeBit(false, out);
      }
      j++;
    } while (Lengrle[j] == 0 && j <= writeIndex);
  }

  @Override
  public int getOneItemMaxSize() {
    return 4 * 4;
  }

  @Override
  public long getMaxByteSize() {
    return 5 * 4 * blockSize;
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
   * flush bits left in byteBuffer to output stream
   *
   * @param out output stream
   */
  protected void clearBuffer(ByteArrayOutputStream out) {
    if (numberLeftInBuffer == 0) return;
    if (numberLeftInBuffer > 0) byteBuffer <<= (8 - numberLeftInBuffer);
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
      if ((writeIndex + 1 & (1 << i)) > 0) writeBit(true, out);
      else writeBit(false, out);
    }
  }
}
