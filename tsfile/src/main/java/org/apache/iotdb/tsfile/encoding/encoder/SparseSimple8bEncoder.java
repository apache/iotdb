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
public class SparseSimple8bEncoder extends Encoder {

  public static final String SPARSE_SIMPLE8B_ENCODING_BLOCK_SIZE =
      "sparse_simple8b_encoding_block_size";
  protected static final int BLOCK_DEFAULT_SIZE = 1024;
  private static final Logger logger = LoggerFactory.getLogger(SparseSimple8bEncoder.class);
  private int blockSize;
  protected int writeIndex = 0;
  private long[] dataBuffer;
  Simple8bEncoder encoder = new Simple8bEncoder();

  public SparseSimple8bEncoder() {
    this(BLOCK_DEFAULT_SIZE);
  }

  public SparseSimple8bEncoder(int size) {
    super(TSEncoding.SIMPLE8B_SPARSE);
    this.blockSize = size;
    this.dataBuffer = new long[blockSize];
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    System.out.println("Sparse Simple8B");
    dataBuffer[writeIndex] = value;
    writeIndex++;
    if (writeIndex == blockSize) {
      flush(out);
    }
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    encode((long) value, out);
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
      encodeBlock(out);
      writeIndex = 0;
    }
  }

  public void encodeBlock(ByteArrayOutputStream out) throws IOException {
    ArrayList<Point> list = new ArrayList();
    for (int i = 0; i < writeIndex; i++) {
      if (dataBuffer[i] > 0) {
        Point point = new Point(i, dataBuffer[i]);
        list.add(point);
      }
    }
    Point[] array = list.toArray(new Point[0]);
    Arrays.sort(array);
    int m = array.length;
    long[] index = new long[m];
    long[] value = new long[m];
    for (int i = 0; i < m; i++) {
      index[i] = array[i].index;
      value[i] = array[i].value;
    }
    BitConstructor constructor = new BitConstructor();
    // 32位数据点个数
    constructor.add(writeIndex, 32);
    // 32位有效数据点个数
    constructor.add(m, 32);
    // 分组位压缩编码index序列
    encodeIndex(index, constructor);
    constructor.pad();
    out.write(constructor.toByteArray());
    // 降序位压缩编码value序列
    encodeValue(value, out);
  }

  private void encodeIndex(long[] value, BitConstructor constructor) {
    int bitsWidth = getValueWidth(getValueWidth(value.length - 1));
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

  private void encodeValue(long[] value, ByteArrayOutputStream out) throws IOException {
    for (int i = 0; i < value.length; i++) {
      encoder.encode(value[i], out);
    }
    encoder.flush(out);
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
    private final long value;

    public Point(int index, long value) {
      this.index = index;
      this.value = value;
    }

    @Override
    public int compareTo(Point o) {
      return Long.compare(o.value, this.value);
    }

    /** @return the index */
    public int getIndex() {
      return index;
    }

    /** @return the value */
    public long getValue() {
      return value;
    }
  }
}
