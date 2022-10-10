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
import java.util.HashMap;
import java.util.Map;

/** @author Wang Haoyu */
public class BuffEncoder extends Encoder {

  protected static final int BLOCK_DEFAULT_SIZE = 1024;
  protected static final int DEFAULT_PRECISION = 0;
  private static final Logger logger = LoggerFactory.getLogger(FreqEncoder.class);
  private int blockSize;
  protected int writeIndex = 0;
  private final double sparseThreshold = 0.9;
  private int precision = 0;
  private double[] dataBuffer;

  public BuffEncoder() {
    this(BLOCK_DEFAULT_SIZE, DEFAULT_PRECISION);
  }

  public BuffEncoder(int size, int precision) {
    super(TSEncoding.BUFF);
    this.blockSize = size;
    this.precision = precision;
    this.dataBuffer = new double[blockSize];
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

  private void flushBlock(ByteArrayOutputStream out) throws IOException {
    if (writeIndex > 0) {
      encodeBlock(out);
      writeIndex = 0;
    }
  }

  public void encodeBlock(ByteArrayOutputStream out) throws IOException {
    // Get the range of integer part
    int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
    for (int i = 0; i < writeIndex; i++) {
      min = Math.min(min, (int) Math.floor(dataBuffer[i]));
      max = Math.max(max, (int) Math.floor(dataBuffer[i]));
    }
    // Meta
    BitConstructor constructor = new BitConstructor();
    constructor.add(writeIndex, 32);
    constructor.add(this.precision, 32);
    constructor.add(min, 32);
    constructor.add(max, 32);
    // Floating point to fixed point
    long[] fixed = new long[writeIndex];
    double eps = Math.pow(2, -precision);
    for (int i = 0; i < fixed.length; i++) {
      fixed[i] = (long) (Math.round((dataBuffer[i] - min) / eps));
    }
    // Store by subcolumns
    int[] masks = {0, 0x0001, 0x0003, 0x0007, 0x000f, 0x001f, 0x003f, 0x007f, 0x00ff};
    int totalWidth = getValueWidth(max - min) + precision;
    for (int i = totalWidth; i > 0; i -= 8) {
      int shift = Math.max(0, i - 8), len = Math.min(8, i);
      byte[] bytes = new byte[fixed.length];
      for (int j = 0; j < fixed.length; j++) {
        bytes[j] = (byte) ((fixed[j] >> shift) & masks[len]);
      }
      encodeSubColumn(constructor, bytes, len);
    }
    constructor.pad();
    out.write(constructor.toByteArray());
  }

  private void encodeSubColumn(BitConstructor constructor, byte[] bytes, int len) {
    // Count and decide whether sparse
    Byte frequentValue = count(bytes, new HashMap<>());
    if (frequentValue == null) {
      constructor.add(0, 8);
      encodeDenseSubColumn(constructor, bytes, len);
    } else {
      constructor.add(1, 8);
      encodeSparseSubColumn(constructor, bytes, frequentValue, len);
    }
  }

  private void encodeDenseSubColumn(BitConstructor constructor, byte[] bytes, int len) {
    for (byte b : bytes) {
      constructor.add(b, len);
    }
  }

  private void encodeSparseSubColumn(
      BitConstructor constructor, byte[] bytes, byte frequentValue, int len) {
    // Mode
    constructor.add(frequentValue, len);
    // RLE
    BitConstructor rle = new BitConstructor();
    int cnt = encodeRLEVector(rle, bytes, frequentValue);
    byte[] rleBytes = rle.toByteArray();
    constructor.pad();
    constructor.add(rleBytes.length, 32);
    constructor.add(rleBytes);
    // Outliers
    constructor.add(cnt, 32);
    for (byte b : bytes) {
      if (b != frequentValue) {
        constructor.add(b, len);
      }
    }
  }

  private int encodeRLEVector(BitConstructor constructor, byte[] bytes, byte frequentValue) {
    int width = getValueWidth(bytes.length);
    boolean outlier = false;
    int run = 0;
    int cnt = 0;
    for (byte b : bytes) {
      if ((b != frequentValue) == outlier) {
        run++;
      } else {
        outlier = !outlier;
        constructor.add(run, width);
        run = 1;
      }
      if (b != frequentValue) {
        cnt++;
      }
    }
    constructor.add(run, width);
    return cnt;
  }

  private Byte count(byte[] bytes, HashMap<Byte, Integer> map) {
    for (byte x : bytes) {
      map.put(x, map.getOrDefault(x, 0) + 1);
    }
    Byte maxByte = null;
    int maxTimes = 0;
    for (Map.Entry<Byte, Integer> entry : map.entrySet()) {
      Byte key = entry.getKey();
      Integer value = entry.getValue();
      if (value > maxTimes) {
        maxTimes = value;
        maxByte = key;
      }
    }
    if (maxTimes > sparseThreshold * bytes.length) {
      return maxByte;
    } else {
      return null;
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

  /**
   * Get the valid bit width of x
   *
   * @param x
   * @return valid bit width
   */
  public int getValueWidth(long x) {
    return 64 - Long.numberOfLeadingZeros(x);
  }
}
