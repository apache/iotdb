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
package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BitConstructor;

import org.jtransforms.dct.DoubleDCT_1D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class FreqEncoder extends Encoder {

  public static final String FREQ_ENCODING_SNR = "freq_encoding_snr";
  public static final String FREQ_ENCODING_BLOCK_SIZE = "freq_encoding_block_size";
  protected static final int BLOCK_DEFAULT_SIZE = 1024;
  protected static final double DEFAULT_SNR = 40;
  private static final Logger logger = LoggerFactory.getLogger(FreqEncoder.class);
  private int blockSize;
  protected int writeIndex = 0;
  private double threshold = 1e-4;
  private int beta;
  private double[] dataBuffer;
  private DoubleDCT_1D transformer;

  public FreqEncoder() {
    this(BLOCK_DEFAULT_SIZE);
  }

  public FreqEncoder(int size) {
    this(size, DEFAULT_SNR);
  }

  public FreqEncoder(int size, double snr) {
    super(TSEncoding.FREQ);
    this.blockSize = size;
    this.transformer = new DoubleDCT_1D(blockSize);
    this.dataBuffer = new double[blockSize];
    snr = Math.max(snr, 0);
    this.threshold = Math.pow(10, -snr / 10);
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
  public void encode(float value, ByteArrayOutputStream out) {
    encode((double) value, out);
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    encode((double) value, out);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    encode((double) value, out);
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
      dct();
      ArrayList<Point> list = selectPoints(dataBuffer);
      byte[] data = encodeBlock(list);
      out.write(data);
      writeIndex = 0;
    }
  }

  private void dct() {
    DoubleDCT_1D dct =
        (writeIndex == this.blockSize) ? this.transformer : new DoubleDCT_1D(writeIndex);
    dct.forward(dataBuffer, true);
  }

  private byte[] encodeBlock(ArrayList<Point> list) {
    // Quantification
    int m = list.size();
    int[] index = new int[m];
    long[] value = new long[m];
    double eps = Math.pow(2, beta);
    for (int i = 0; i < m; i++) {
      Point p = list.get(i);
      index[i] = p.getIndex();
      value[i] = Math.round(p.getValue() / eps);
    }
    BitConstructor constructor = new BitConstructor(9 + 13 * m);
    // Block size with 16 bits
    constructor.add(writeIndex, 16);
    // Number of reserved components with 16 bits
    constructor.add(m, 16);
    // Exponent of quantification level with 16 bits
    constructor.add(beta, 16);
    // Encode the index sequence
    encodeIndex(index, constructor);
    // Encode the value sequence
    encodeValue(value, constructor);
    constructor.pad();
    // return the encoded bytes
    return constructor.toByteArray();
  }

  private void encodeIndex(int[] value, BitConstructor constructor) {
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
    // Encode the encoded bit width of the first value with 8 bits
    int bits = getValueWidth(Math.abs(value[0]));
    constructor.add(bits, 8);
    // Encode min{|v|}
    long min = Math.abs(value[value.length - 1]);
    constructor.add(min, bits);
    // Encode all values
    for (int i = 0; i < value.length; i++) {
      constructor.add(value[i] >= 0 ? 0 : 1, 1); // Symbol bit
      value[i] = Math.abs(value[i]) - min;
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

  private int initBeta(double sum2) {
    double temp = Math.sqrt(threshold * sum2 / (writeIndex * writeIndex));
    return (int) Math.max(max2Power(temp), Math.log(sum2) / (2 * Math.log(2)) - 60);
  }

  /**
   * Returns the exponent of the largest power of 2 that is less than or equal to x.<br>
   * max{y|2^y &le x, y is an integer}
   *
   * @param x
   * @return the exponent of the largest power of 2 that is less than or equal to x
   */
  private int max2Power(double x) {
    double ans = 1;
    int exponent = 0;
    if (x > 1) {
      while (ans * 2 <= x) {
        ans = ans * 2;
        exponent++;
      }
    } else {
      while (ans > x) {
        ans = ans / 2;
        exponent--;
      }
    }
    return exponent;
  }

  private ArrayList<Point> selectPoints(double a[]) {
    // Keep the components with priority queue in the descending order of energy
    double sum2 = 0;
    Point point;
    PriorityQueue<Point> queue = new PriorityQueue<>(writeIndex);
    for (int i = 0; i < writeIndex; i++) {
      point = new Point(i, a[i]);
      queue.add(point);
      sum2 += point.getPower();
    }
    // Add components to keepList
    this.beta = initBeta(sum2);
    double systemError = sum2;
    ArrayList<Point> keepList = new ArrayList<>();
    int m = 0; // Number of reserved components
    double roundingError = 0;
    double reduceBits = Double.MAX_VALUE;
    boolean first = true;
    do {
      while (systemError + roundingError > threshold * sum2) {
        point = queue.poll();
        if (point == null) {
          systemError = 0;
          break;
        }
        keepList.add(point);
        systemError = systemError - point.getPower();
        roundingError = Math.pow(2, this.beta * 2) * keepList.size();
      }
      double increaseBits = estimateIncreaseBits(keepList, m);
      if (reduceBits <= increaseBits || systemError + roundingError > threshold * sum2) {
        if (!first) {
          keepList = new ArrayList(keepList.subList(0, m));
          this.beta--;
        }
        break;
      }
      // Increase quantification level
      first = false;
      m = keepList.size();
      reduceBits = m;
      this.beta++;
      roundingError = Math.pow(2, this.beta * 2) * m;
    } while (true);
    return keepList;
  }

  /**
   * Estimate the number of increased bits by reserving more components
   *
   * @param list The list of reserved components in this turn
   * @param m The number of resereved components in last turn
   * @return Estimated number of bits
   */
  private double estimateIncreaseBits(ArrayList<Point> list, int m) {
    double bits = 0;
    double eps = Math.pow(2, beta);
    for (int i = m; i < list.size(); i++) {
      bits += getValueWidth(writeIndex - 1); // Index
      bits += getValueWidth(Math.round(Math.abs(list.get(i).getValue()) / eps)); // Value
      bits += 1; // Symbol
    }
    return bits;
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
      return Double.compare(o.getPower(), this.getPower());
    }

    /** @return the index */
    public int getIndex() {
      return index;
    }

    /** @return the value */
    public double getValue() {
      return value;
    }

    public double getPower() {
      return value * value;
    }
  }
}
