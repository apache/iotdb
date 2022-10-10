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
package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BitReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/** @author Wang Haoyu */
public class BuffDecoder extends Decoder {

  private double data[];

  private int readTotalCount = 0;

  private int nextReadIndex = 0;

  public BuffDecoder() {
    super(TSEncoding.BUFF);
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
    // Meta
    int n = (int) reader.next(32);
    int p = (int) reader.next(32);
    int min = (int) reader.next(32);
    int max = (int) reader.next(32);
    // Subcolumns
    long[] fixed = new long[n];
    int totalWidth = getValueWidth(max - min) + p;
    for (int i = totalWidth; i > 0; i -= 8) {
      int len = Math.min(8, i);
      byte[] bytes = decodeSubColumn(reader, n, len);
      for (int j = 0; j < n; j++) {
        fixed[j] = (fixed[j] << len) | (bytes[j] & 0xff);
      }
    }
    reader.skip();
    // Fiexed point to floating point
    data = new double[n];
    double eps1 = Math.pow(2, -p);
    for (int i = 0; i < n; i++) {
      data[i] = fixed[i] * eps1 + min;
    }
    this.readTotalCount = n;
  }

  private byte[] decodeSubColumn(BitReader reader, int n, int len) {
    int sparseFlag = (int) reader.next(8);
    switch (sparseFlag) {
      case 0:
        return decodeDenseSubColumn(reader, n, len);
      case 1:
        return decodeSparseSubColumn(reader, n, len);
      default:
        throw new RuntimeException("It cannot be reached.");
    }
  }

  private byte[] decodeDenseSubColumn(BitReader reader, int n, int len) {
    byte[] bytes = new byte[n];
    for (int i = 0; i < n; i++) {
      bytes[i] = (byte) reader.next(len);
    }
    return bytes;
  }

  private byte[] decodeSparseSubColumn(BitReader reader, int n, int len) {
    // Mode
    byte frequentValue = (byte) reader.next(len);
    // RLE
    reader.skip();
    int rleByteSize = (int) reader.next(32);
    boolean vector[] = decodeRLEVector(reader.nextBytes(rleByteSize), n);
    // Outliers
    int cnt = (int) reader.next(32);
    byte[] bytes = new byte[n];
    for (int i = 0; i < n; i++) {
      if (vector[i]) {
        bytes[i] = (byte) reader.next(len);
      } else {
        bytes[i] = frequentValue;
      }
    }
    return bytes;
  }

  private boolean[] decodeRLEVector(byte[] bytes, int n) {

    BitReader reader = new BitReader(ByteBuffer.wrap(bytes));
    boolean[] vector = new boolean[n];
    int width = getValueWidth(n);
    int i = 0;
    boolean bit = false;
    while (i < n) {
      int run = (int) reader.next(width);
      int j = i + run;
      for (; i < j; i++) {
        vector[i] = bit;
      }
      bit = !bit;
    }
    return vector;
  }

  public int getValueWidth(long x) {
    return 64 - Long.numberOfLeadingZeros(x);
  }
}
