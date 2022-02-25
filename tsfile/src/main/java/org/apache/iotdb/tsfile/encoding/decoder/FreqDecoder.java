/*
 * Copyright 2021 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BitReader;

import org.jtransforms.dct.DoubleDCT_1D;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FreqDecoder extends Decoder {

  private double data[];

  private int readTotalCount = 0;

  private int nextReadIndex = 0;

  public FreqDecoder() {
    super(TSEncoding.FREQ);
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    if (nextReadIndex == readTotalCount) {
      loadBlock(buffer);
      nextReadIndex = 0;
    }
    return data[nextReadIndex++];
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    return (float) readDouble(buffer);
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    return (int) Math.round(readDouble(buffer));
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    return (long) Math.round(readDouble(buffer));
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (nextReadIndex < readTotalCount) || buffer.hasRemaining();
  }

  @Override
  public void reset() {
    nextReadIndex = 0;
    readTotalCount = 0;
  }

  private void loadBlock(ByteBuffer buffer) {
    BitReader reader = new BitReader(buffer);
    // Block size with 16 bits
    this.readTotalCount = (int) reader.next(16);
    // Number of reserved components with 16 bits
    int m = (int) reader.next(16);
    // Exponent of quantification level with 16 bits
    int beta = (short) reader.next(16);
    // Decode index sequence
    int[] index = decodeIndex(m, reader);
    // Decode value sequence
    long[] value = decodeValue(m, reader);
    reader.skip();
    // Quantification
    double eps = Math.pow(2, beta);
    this.data = new double[readTotalCount];
    for (int i = 0; i < m; i++) {
      data[index[i]] = value[i] * eps;
    }
    DoubleDCT_1D dct = new DoubleDCT_1D(readTotalCount);
    dct.inverse(data, true);
  }

  private long[] decodeValue(int m, BitReader reader) {
    if (m == 0) {
      return new long[0];
    }
    // Decode the encoded bit width of the first value with 8 bits
    int bits = (int) reader.next(8);
    // Decode min{|v|}
    long min = reader.next(bits);
    // Decode all values
    long value[] = new long[m];
    int symbol;
    for (int i = 0; i < m; i++) {
      symbol = (int) reader.next(1);
      value[i] = reader.next(bits);
      bits = getValueWidth(value[i]);
      value[i] += min;
      if (symbol == 1) { // Negative value
        value[i] = -value[i];
      }
    }
    return value;
  }

  private int[] decodeIndex(int m, BitReader reader) {
    int[] value = new int[m];
    int bitsWidth = getValueWidth(getValueWidth(readTotalCount - 1));
    for (int i = 0; i < m; i += 8) {
      int bits = (int) reader.next(bitsWidth);
      for (int j = i; j < Math.min(i + 8, m); j++) {
        value[j] = (int) reader.next(bits);
      }
    }
    return value;
  }

  /**
   * Get the valid bit width of x
   *
   * @param x
   * @return valid bit width
   */
  private int getValueWidth(long x) {
    return 64 - Long.numberOfLeadingZeros(x);
  }
}
