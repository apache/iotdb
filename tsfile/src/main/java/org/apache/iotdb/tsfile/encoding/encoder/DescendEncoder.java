/*
 * Copyright 2022 The Apache Software Foundation.
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
package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BitConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/** @author Wang Haoyu */
public class DescendEncoder extends Encoder {

  public static final String DESCEND_ENCODING_BLOCK_SIZE = "descend_encoding_block_size";
  protected static final int BLOCK_DEFAULT_SIZE = 1024;
  private static final Logger logger = LoggerFactory.getLogger(DescendEncoder.class);
  private int blockSize;
  protected int writeIndex = 0;
  private double[] dataBuffer;

  public DescendEncoder() {
    this(BLOCK_DEFAULT_SIZE);
  }

  public DescendEncoder(int size) {
    super(TSEncoding.DESCEND);
    this.blockSize = size;
    this.dataBuffer = new double[blockSize];
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    encode((double) value, out);
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    encode((double) value, out);
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    encode((double) value, out);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    dataBuffer[writeIndex] = value;
    writeIndex++;
    if (writeIndex == blockSize) {
      flush(out);
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      flushBlock(out);
    } catch (IOException e) {
      logger.error("flush data to stream failed!", e);
    }
  }

  @Override
  public int getOneItemMaxSize() {
    return 13;
  }

  @Override
  public long getMaxByteSize() {
    return 8 + 13 * writeIndex;
  }

  private void flushBlock(ByteArrayOutputStream out) throws IOException {
    if (writeIndex > 0) {
      byte[] data = encodeBlock();
      out.write(data);
      writeIndex = 0;
    }
  }

  public byte[] encodeBlock() {
    ArrayList<Point> list = new ArrayList();
    int beta = Integer.MAX_VALUE;
    for (int i = 0; i < writeIndex; i++) {
      if (dataBuffer[i] > 0) {
        Point point = new Point(i, dataBuffer[i]);
        list.add(point);
        beta = Math.min(beta, point.getExp());
      }
    }
    Point[] array = list.toArray(new Point[0]);
    Arrays.sort(array);
    int m = array.length;
    long[] index = new long[m];
    long[] value = new long[m];
    for (int i = 0; i < m; i++) {
      index[i] = array[i].index;
      value[i] = array[i].quantize(beta);
    }
    BitConstructor constructor = new BitConstructor();
    // Number of data points
    constructor.add(writeIndex, 32);
    // Number of reserved data points
    constructor.add(m, 32);
    // Quantization level
    constructor.add(beta, 32);
    // Group bit-backing
    encodeIndex(index, constructor);
    // Descend bit-packing
    encodeValue(value, constructor);
    constructor.pad();
    return constructor.toByteArray();
  }

  private void encodeIndex(long[] value, BitConstructor constructor) {
    int bitsWidth = getValueWidth(getValueWidth(writeIndex - 1));
    for (int i = 0; i < value.length; i += 8) {
      int bits = 0;
      for (int j = i; j < Math.min(value.length, i + 8); j++) {
        bits = Math.max(bits, getValueWidth(value[j]));
      }
      constructor.add(bits, bitsWidth);
      for (int j = i; j < Math.min(value.length, i + 8); j++) {
        constructor.add(value[j], bits);
      }
    }
  }

  private void encodeValue(long[] value, BitConstructor constructor) {
    if (value.length == 0) {
      return;
    }
    // Bit width of the first value
    int bits = getValueWidth(Math.abs(value[0]));
    constructor.add(bits, 8);
    // Encode all values
    for (int i = 0; i < value.length; i++) {
      constructor.add(value[i], bits);
      bits = getValueWidth(value[i]);
    }
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

  protected class Point implements Comparable<Point> {

    private final int index;
    private final double value;

    public Point(int index, double value) {
      this.index = index;
      this.value = value;
    }

    @Override
    public int compareTo(Point o) {
      return Double.compare(o.value, this.value);
    }

    /** @return the index */
    public int getIndex() {
      return index;
    }

    /** @return the value */
    public double getValue() {
      return value;
    }

    public long quantize(int beta) {
      double eps = Math.pow(2, beta);
      return Math.round(this.value / eps);
    }

    public int getExp() {
      long y = Double.doubleToRawLongBits(this.value);
      long exp = (y >>> 52) - 1023 + Long.numberOfTrailingZeros(y) - 52;
      return (int) exp;
    }
  }
}
