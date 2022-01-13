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

import org.jtransforms.fft.DoubleFFT_1D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

/** @author Wang Haoyu */
public class FreqEncoder extends Encoder {

  protected static final int BLOCK_DEFAULT_SIZE = 1024;
  private static final Logger logger = LoggerFactory.getLogger(FreqEncoder.class);
  private ByteArrayOutputStream out;
  private int blockSize;
  protected int writeIndex = 0;
  private double threshold = 1e-4;
  private int base;
  private double[] dataBuffer;
  private DoubleFFT_1D transformer;

  public FreqEncoder() {
    this(BLOCK_DEFAULT_SIZE);
  }

  public FreqEncoder(int size) {
    super(TSEncoding.FREQ);
    this.blockSize = size;
    this.transformer = new DoubleFFT_1D(blockSize);
    this.dataBuffer = new double[2 * blockSize];
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    dataBuffer[writeIndex * 2] = value; // Real part
    dataBuffer[writeIndex * 2 + 1] = 0; // Imaginary part
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
      fft();
      ArrayList<Point> list = selectPoints();
      //                        System.out.println(list.size());
      byte[] data = encodeBlock(list);
      //            System.out.println(data.length);
      out.write(data);
      writeIndex = 0;
    }
  }

  private byte[] encodeBlock(ArrayList<Point> list) {
    // 序列离散化
    int m = list.size();
    int[] index = new int[m];
    int[] amptitude = new int[m];
    double[] angle = new double[m];
    double eps1 = Math.pow(2, base); // eps2 = Math.PI / 128;
    for (int i = 0; i < m; i++) {
      Point p = list.get(i);
      index[i] = p.getIndex();
      amptitude[i] = (int) Math.round(p.getAmp() / eps1);
      angle[i] = p.getAngle();
      //            angle[i] = (byte) (Math.round(p.getAngle() / eps2) + 128);
    }
    BitConstructor constructor = new BitConstructor(8 + 13 * m);
    // 16位原始数据长度
    constructor.add(writeIndex, 16);
    // 16位数据点个数
    constructor.add(m, 16);
    // 32位基底
    constructor.add(base, 32);
    // 以TS_2DIFF格式编码index序列
    encodeTS2DIFF(index, constructor);
    constructor.pad();
    //        System.out.println(constructor.sizeInBytes());
    // 以降序格式编码amplitude序列
    encodeDescend(amptitude, angle, constructor);
    constructor.pad();
    //        System.out.println(constructor.sizeInBytes());
    // 以原始格式编码angle序列
    //        constructor.add(angle);
    //            System.out.println(constructor.sizeInBytes());
    // 返回编码后的字节流
    return constructor.toByteArray();
  }

  private void encodeDescend(int[] value, double[] angle, BitConstructor constructor) {
    if (value.length == 0) {
      return;
    }
    // 8位，第一个数的位数
    int bits = getValueWidth(value[0]);
    constructor.add(bits, 8);
    //        System.out.println(bits);
    // 存储所有数据
    for (int i = 0; i < value.length; i++) {
      constructor.add(value[i], bits);
      int a = (int) Math.round((angle[i] + Math.PI) / (2 * Math.PI) * (1 << bits));
      constructor.add(a, bits);
      bits = getValueWidth(value[i]);
    }
  }

  private void encodeTS2DIFF(int[] value, BitConstructor constructor) {
    if (value.length == 0) {
      return;
    }
    // 差分
    int diff[] = new int[value.length];
    diff[0] = value[0];
    for (int i = 1; i < value.length; i++) {
      diff[i] = value[i] - value[i - 1];
    }
    // 正数化
    int minDiff = Integer.MAX_VALUE;
    for (int i = 0; i < diff.length; i++) {
      minDiff = Math.min(minDiff, diff[i]);
    }
    for (int i = 0; i < diff.length; i++) {
      diff[i] -= minDiff;
    }
    constructor.add(minDiff, 32);
    // 计算每个值需要的位数（数据宽度）
    int bits = 0;
    for (int i = 0; i < diff.length; i++) {
      bits = Math.max(bits, getValueWidth(diff[i]));
    }
    constructor.add(bits, 8);
    // 保存所有差值
    for (int i = 0; i < diff.length; i++) {
      constructor.add(diff[i], bits);
    }
  }

  /**
   * 计算x的数据宽度
   *
   * @param x
   * @return 数据宽度
   */
  private int getValueWidth(long x) {
    return 64 - Long.numberOfLeadingZeros(x);
  }

  private int getBase(int m, double sum2) {
    double temp = Math.sqrt(threshold * sum2 / (m * writeIndex));
    return max2Power(temp);
  }

  /**
   * 返回小于等于x的最大的2的幂（包括负数次幂）的指数
   *
   * @param x
   * @return 小于等于x的最大的2的幂的指数
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

  private ArrayList<Point> selectPoints() {
    int n = this.writeIndex;
    // 利用优先队列（堆）来维护信息量大的数据点
    double sum2 = 0;
    Point point;
    PriorityQueue<Point> queue = new PriorityQueue<>(n / 2);
    for (int i = 0; i <= n / 2; i++) {
      point = new Point(i, dataBuffer[2 * i], dataBuffer[2 * i + 1]);
      queue.add(point);
      sum2 += point.getPower();
    }
    // 挑选数据量大的数据点加入keepList
    double temp = sum2;
    ArrayList<Point> keepList = new ArrayList<>();
    while (temp < threshold * sum2) {
      point = queue.poll();
      keepList.add(point);
      temp = temp + point.getPower();
    }
    this.base = getBase(keepList.size(), sum2);
    return keepList;
  }

  private void fft() {
    DoubleFFT_1D fft =
        (writeIndex == this.blockSize) ? this.transformer : new DoubleFFT_1D(writeIndex);
    fft.complexForward(dataBuffer);
  }

  private class Point implements Comparable<Point> {

    private final double real;
    private final double imag;
    private final int index;
    private final double power;

    Point(int index, double real, double imag) {
      this.index = index;
      this.real = real;
      this.imag = imag;
      int n = FreqEncoder.this.writeIndex;
      if (this.index == 0 || ((n % 2 == 0) && (this.index == n / 2))) {
        this.power = real * real + imag * imag;
      } else {
        this.power = 2 * (real * real + imag * imag);
      }
    }

    @Override
    public int compareTo(Point o) {
      return Double.compare(o.power, this.power);
    }

    public double getPower() {
      return this.power;
    }

    /** @return the amp */
    public double getAmp() {
      return Math.sqrt(this.power);
    }

    /** @return the angle */
    public double getAngle() {
      return Math.atan2(this.imag, this.real);
    }

    /** @return the real */
    public double getReal() {
      return real;
    }

    /** @return the imag */
    public double getImag() {
      return imag;
    }

    /** @return the index */
    public int getIndex() {
      return index;
    }
  }
}
