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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is a decoder for decoding the byte array that encoded by {@code
 * DeltaBinaryEncoder}.DeltaBinaryDecoder just supports integer and long values.<br>
 * .
 *
 * @see DeltaBinaryEncoder
 */
public abstract class DeltaBinaryDecoder extends Decoder {

  protected static final int[] MASK =
      new int[] {0xFF, 0xFF >> 1, 0xFF >> 2, 0xFF >> 3, 0xFF >> 4, 0xFF >> 5, 0xFF >> 6, 0xFF >> 7};

  protected byte[] deltaBuf;

  protected int nextReadIndex = 0;
  /** max bit length of all value in a pack. */
  protected int packWidth;
  /** data number in this pack. */
  protected int packNum;

  /** how many bytes data takes after encoding. */
  protected int encodingLength;

  protected DeltaBinaryDecoder() {
    super(TSEncoding.TS_2DIFF);
  }

  /**
   * calculate the bytes length containing v bits.
   *
   * @param v - number of bits
   * @return number of bytes
   */
  protected int ceil(int v) {
    return (int) Math.ceil(v / 8.0);
  }

  /**
   * if remaining data has been run out, load next pack from InputStream.
   *
   * @param buffer ByteBuffer
   */
  protected void loadBatch(ByteBuffer buffer) {
    packNum = ReadWriteIOUtils.readInt(buffer);
    packWidth = ReadWriteIOUtils.readInt(buffer);
    readHeader(buffer);

    encodingLength = ceil(packNum * packWidth);
    deltaBuf = new byte[encodingLength];
    buffer.get(deltaBuf);
    allocateDataArray();

    nextReadIndex = 0;
    readPack();
  }

  protected abstract void readHeader(ByteBuffer buffer);

  protected abstract void allocateDataArray();

  protected abstract void readPack();

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (nextReadIndex < packNum) || buffer.remaining() > 0;
  }

  public static class IntDeltaDecoder extends DeltaBinaryDecoder {

    private int firstValue;
    private int[] data;
    private int previous;
    /** minimum value for all difference. */
    private int minDeltaBase;

    public IntDeltaDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    @Override
    public int readInt(ByteBuffer buffer) {
      if (nextReadIndex == packNum) {
        loadBatch(buffer);
        return firstValue;
      }
      return data[nextReadIndex++];
    }

    @Override
    public void readInt(ByteBuffer buffer, int[] data, int length) {
      int offset = 0;
      while (offset < length) {
        packNum = ReadWriteIOUtils.readInt(buffer);
        packWidth = ReadWriteIOUtils.readInt(buffer);
        readHeader(buffer);
        data[offset++] = firstValue;
        encodingLength = ceil(packNum * packWidth);
        deltaBuf = new byte[encodingLength];
        buffer.get(deltaBuf);

        int value;
        int index = 0;
        int currentByteOffset = 0;
        int width;

        for (int i = 0; i < packNum; i++) {
          value = 0;
          width = packWidth;
          while (width > 0) {
            int m = width + currentByteOffset >= 8 ? 8 - currentByteOffset : width;
            width -= m;
            value = value << m;
            int y = deltaBuf[index] & MASK[currentByteOffset];
            currentByteOffset += m;
            y >>>= (8 - currentByteOffset);
            value |= y;
            if (currentByteOffset == 8) {
              currentByteOffset = 0;
              ++index;
            }
          }
          data[offset] = data[offset - 1] + minDeltaBase + value;
          ++offset;
        }
      }
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readInt(buffer);
      firstValue = ReadWriteIOUtils.readInt(buffer);
      previous = firstValue;
    }

    @Override
    protected void allocateDataArray() {
      data = new int[packNum];
    }

    @Override
    protected void readPack() {
      int value;
      int index = 0;
      int currentByteOffset = 0;
      int width;

      for (int i = 0; i < packNum; i++) {
        value = 0;
        width = packWidth;
        while (width > 0) {
          int m = width + currentByteOffset >= 8 ? 8 - currentByteOffset : width;
          width -= m;
          value = value << m;
          int y = deltaBuf[index] & MASK[currentByteOffset];
          currentByteOffset += m;
          y >>>= (8 - currentByteOffset);
          value |= y;
          if (currentByteOffset == 8) {
            currentByteOffset = 0;
            index++;
          }
        }
        data[i] = previous + minDeltaBase + value;
        previous = data[i];
      }
    }

    @Override
    public void reset() {
      // do nothing
    }
  }

  public static class LongDeltaDecoder extends DeltaBinaryDecoder {

    private long firstValue;
    private long[] data;
    private long previous;
    /** minimum value for all difference. */
    private long minDeltaBase;

    public LongDeltaDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return long value
     */
    @Override
    public long readLong(ByteBuffer buffer) {
      if (nextReadIndex == packNum) {
        loadBatch(buffer);
        return firstValue;
      }
      return data[nextReadIndex++];
    }

    @Override
    public void readLong(ByteBuffer buffer, long[] data, int length) {
      int offset = 0;
      while (offset < length) {
        packNum = ReadWriteIOUtils.readInt(buffer);
        packWidth = ReadWriteIOUtils.readInt(buffer);
        readHeader(buffer);
        data[offset++] = firstValue;
        encodingLength = ceil(packNum * packWidth);
        deltaBuf = new byte[encodingLength];
        buffer.get(deltaBuf);

        long value;
        int index = 0;
        int currentByteOffset = 0;
        int width;

        for (int i = 0; i < packNum; i++) {
          value = 0;
          width = packWidth;
          while (width > 0) {
            int m = width + currentByteOffset >= 8 ? 8 - currentByteOffset : width;
            width -= m;
            value = value << m;
            int y = deltaBuf[index] & MASK[currentByteOffset];
            currentByteOffset += m;
            y >>>= (8 - currentByteOffset);
            value |= y;
            if (currentByteOffset == 8) {
              currentByteOffset = 0;
              ++index;
            }
          }
          data[offset] = data[offset - 1] + minDeltaBase + value;
          ++offset;
        }
      }
    }

    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readLong(buffer);
      firstValue = ReadWriteIOUtils.readLong(buffer);
    }

    protected void allocateDataArray() {
      data = new long[packNum];
    }

    @Override
    protected void readPack() {
      long value;
      int index = 0;
      int currentByteOffset = 0;
      int width;

      for (int i = 0; i < packNum; i++) {
        value = 0;
        width = packWidth;
        while (width > 0) {
          int m = width + currentByteOffset >= 8 ? 8 - currentByteOffset : width;
          width -= m;
          value = value << m;
          int y = deltaBuf[index] & MASK[currentByteOffset];
          currentByteOffset += m;
          y >>>= (8 - currentByteOffset);
          value |= y;
          if (currentByteOffset == 8) {
            currentByteOffset = 0;
            index++;
          }
        }

        data[i] = previous + minDeltaBase + value;
        previous = data[i];
      }
    }

    @Override
    public void reset() {
      // do nothing
    }
  }
}
