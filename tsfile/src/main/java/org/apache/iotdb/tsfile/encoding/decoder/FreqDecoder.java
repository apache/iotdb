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

import org.jtransforms.fft.DoubleFFT_1D;

import java.io.IOException;
import java.nio.ByteBuffer;

/** @author Wang Haoyu */
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
    // 16位原始数据长度
    this.readTotalCount = (int) reader.next(16);
    //            System.out.println(readTotalCount);
    // 16位数据点个数
    int m = (int) reader.next(16);
    //            System.out.println(m);
    // 32位基底
    int base = (int) reader.next(32);
    // 以TS_2DIFF格式解码index序列
    int[] index = decodeTS2DIFF(m, reader);
    reader.skip();
    //             System.out.println(buffer.remaining());
    // 以降序格式解码amplitude序列
    int[] amplitude = decodeDescend(m, reader);
    reader.skip();
    //             System.out.println(buffer.remaining());
    // 以原始格式解码angle序列
    byte[] angle = reader.nextBytes(m);
    //            System.out.println(buffer.remaining());
    // 序列反离散化
    double a[] = new double[readTotalCount * 2];
    double eps1 = Math.pow(2, base), eps2 = Math.PI / 128;
    for (int i = 0; i < m; i++) {
      double amp = Math.sqrt(amplitude[i] * eps1);
      double theta = (angle[i] - 128) * eps2;
      int k = index[i];
      a[k * 2] = amp * Math.cos(theta);
      a[k * 2 + 1] = amp * Math.sin(theta);
      if (k > 0) {
        k = this.readTotalCount - k;
        a[k * 2] = amp * Math.cos(theta);
        a[k * 2 + 1] = -amp * Math.sin(theta);
      }
    }
    DoubleFFT_1D fft = new DoubleFFT_1D(readTotalCount);
    fft.complexInverse(a, true);
    this.data = new double[readTotalCount];
    for (int i = 0; i < readTotalCount; i++) {
      this.data[i] = a[i * 2];
    }
  }

  private int[] decodeTS2DIFF(int m, BitReader reader) {
    int minDiff = (int) reader.next(32); // 32位最小差分
    int bits = (int) reader.next(8); // 8位数据宽度
    // 读取所有差值
    int diff[] = new int[m];
    for (int i = 0; i < m; i++) {
      diff[i] = (int) (reader.next(bits) + minDiff);
    }
    // 去差分
    int value[] = new int[m];
    value[0] = diff[0];
    for (int i = 1; i < m; i++) {
      value[i] = value[i - 1] + diff[i];
    }
    return value;
  }

  private int[] decodeDescend(int m, BitReader reader) {
    // 8位，第一个数的位数
    int bits = (int) reader.next(8);
    // 读取所有数据
    int value[] = new int[m];
    for (int i = 0; i < m; i++) {
      value[i] = (int) reader.next(bits);
      bits = getValueWidth(value[i]);
    }
    return value;
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
}
