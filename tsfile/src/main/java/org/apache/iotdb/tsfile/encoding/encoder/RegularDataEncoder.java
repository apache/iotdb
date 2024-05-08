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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.BitSet;

/**
 * RegularDataEncoder is an encoder for compressing data in type of integer and long. We adapt a
 * hypothesis that the difference between each data point is the same, which it means the data is
 * regular.
 *
 * <p>To encode the regular data, we first create an array as a block to store the data loaded into
 * the encoder. While it reach the default block size, start calculating the delta between each data
 * point in this block in order to checkout whether there are missing points exist in the data. If
 * there is, create a bitmap for this block to denote the position of missing points. Next, store
 * the data info (the data size, the minimum delta value and the first data point of this block) and
 * the bitmap with its info into the result byte array output stream.
 */
public abstract class RegularDataEncoder extends Encoder {

  protected static final int BLOCK_DEFAULT_SIZE = 128;
  private static final Logger LOGGER = LoggerFactory.getLogger(RegularDataEncoder.class);
  protected ByteArrayOutputStream out;
  protected int blockSize;
  protected boolean isMissingPoint;

  protected int writeIndex = -1;

  protected int dataTotal;

  /**
   * constructor of RegularDataEncoder.
   *
   * @param size - the number how many numbers to be packed into a block.
   */
  public RegularDataEncoder(int size) {
    super(TSEncoding.REGULAR);
    blockSize = size;
  }

  protected abstract void writeHeader() throws IOException;

  protected abstract void reset();

  protected abstract void checkMissingPoint(ByteArrayOutputStream out) throws IOException;

  protected abstract void writeBitmap(ByteArrayOutputStream out) throws IOException;

  protected void writeHeaderToBytes() throws IOException {
    out.write(BytesUtils.intToBytes(writeIndex));
    writeHeader();
  }

  protected void flushBlockBuffer(ByteArrayOutputStream out) throws IOException {
    if (writeIndex == -1) {
      return;
    }
    this.out = out;

    // check if the missing point exists
    checkMissingPoint(out);
    // write identifier
    out.write(BytesUtils.boolToBytes(isMissingPoint));
    // write bitmap if missing points exist
    if (isMissingPoint) {
      writeBitmap(out);
    }
    // write header
    writeHeaderToBytes();

    reset();
    writeIndex = -1;
  }

  /** calling this method to flush all values which haven't encoded to result byte array. */
  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      flushBlockBuffer(out);
    } catch (IOException e) {
      LOGGER.error("flush data to stream failed!", e);
    }
  }

  public static class IntRegularEncoder extends RegularDataEncoder {

    private int[] data;
    private int firstValue;
    private int previousValue;
    private int minDeltaBase;
    private int newBlockSize;
    private BitSet bitmap;

    /** constructor of IntRegularEncoder which is a sub-class of RegularDataEncoder. */
    public IntRegularEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of RegularDataEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public IntRegularEncoder(int size) {
      super(size);
      reset();
    }

    @Override
    protected void reset() {
      minDeltaBase = Integer.MAX_VALUE;
      isMissingPoint = false;
      firstValue = 0;
      previousValue = 0;
    }

    @Override
    protected void writeHeader() throws IOException {
      out.write(BytesUtils.intToBytes(minDeltaBase));
      out.write(BytesUtils.intToBytes(firstValue));
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      try {
        encodeValue(value, out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int getOneItemMaxSize() {
      return 4;
    }

    @Override
    public long getMaxByteSize() {
      // The meaning of 20 is:
      // identifier(4)+bitmapLength(4)+index(4)+minDeltaBase(4)+firstValue(4)
      return (long) 20 + (writeIndex * 2 / 8) + (writeIndex * 4);
    }

    @Override
    protected void checkMissingPoint(ByteArrayOutputStream out) throws IOException {
      // get the new regular data if the missing point exists in the original data
      if (writeIndex > 1) {
        previousValue = data[0];
        minDeltaBase = data[1] - data[0];
        // calculate minimum elapsed of the data and check whether the missing point
        // exists
        for (int i = 1; i < writeIndex; i++) {
          int delta = data[i] - previousValue; // calculate delta
          if (delta != minDeltaBase) {
            isMissingPoint = true;
          }
          if (delta < minDeltaBase) {
            minDeltaBase = delta;
          }
          previousValue = data[i];
        }
      }
      firstValue = data[0];
      if (isMissingPoint) {
        dataTotal = writeIndex;
        newBlockSize = ((data[writeIndex - 1] - data[0]) / minDeltaBase) + 1;
        writeIndex = newBlockSize;
      }
    }

    @Override
    protected void writeBitmap(ByteArrayOutputStream out) throws IOException {
      // generate bitmap
      data2Diff(data);
      byte[] bsArr = bitmap.toByteArray();
      out.write(BytesUtils.intToBytes(bsArr.length));
      out.write(bsArr);
    }

    /**
     * input a integer or long value.
     *
     * @param value value to encode
     * @param out - the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(int value, ByteArrayOutputStream out) throws IOException {
      if (writeIndex == -1) {
        data = new int[blockSize];
        writeIndex = 0;
      }
      data[writeIndex++] = value;
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    private void data2Diff(int[] missingPointData) {
      bitmap = new BitSet(newBlockSize);
      bitmap.flip(0, newBlockSize);
      int offset = 0;
      for (int i = 1; i < dataTotal; i++) {
        int delta = missingPointData[i] - missingPointData[i - 1];
        if (delta != minDeltaBase) {
          int missingPointNum = (int) (delta / minDeltaBase) - 1;
          for (int j = 0; j < missingPointNum; j++) {
            bitmap.set(i + (offset++), false);
          }
        }
      }
    }
  }

  public static class LongRegularEncoder extends RegularDataEncoder {

    private long[] data;
    private long firstValue;
    private long previousValue;
    private long minDeltaBase;
    private int newBlockSize;
    private BitSet bitmap;

    public LongRegularEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of LongRegularEncoder which is a sub-class of RegularDataEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public LongRegularEncoder(int size) {
      super(size);
      reset();
    }

    @Override
    protected void reset() {
      minDeltaBase = Long.MAX_VALUE;
      isMissingPoint = false;
      firstValue = 0L;
      previousValue = 0L;
    }

    @Override
    protected void writeHeader() throws IOException {
      out.write(BytesUtils.longToBytes(minDeltaBase));
      out.write(BytesUtils.longToBytes(firstValue));
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
      try {
        encodeValue(value, out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int getOneItemMaxSize() {
      return 8;
    }

    @Override
    public long getMaxByteSize() {
      // The meaning of 20 is:
      // identifier(4)+bitmapLength(4)+index(4)+minDeltaBase(8)+firstValue(8)
      return (long) 28 + (writeIndex * 2 / 8) + (writeIndex * 8);
    }

    @Override
    protected void checkMissingPoint(ByteArrayOutputStream out) throws IOException {
      // get the new regular data if the missing point exists in the original data
      if (writeIndex > 1) {
        previousValue = data[0];
        minDeltaBase = data[1] - data[0];
        // calculate minimum elapsed of the data and check whether the missing point
        // exists
        for (int i = 1; i < writeIndex; i++) {
          long delta = data[i] - previousValue; // calculate delta
          if (delta != minDeltaBase) {
            isMissingPoint = true;
          }
          if (delta < minDeltaBase) {
            minDeltaBase = delta;
          }
          previousValue = data[i];
        }
      }
      firstValue = data[0];
      if (isMissingPoint) {
        dataTotal = writeIndex;
        newBlockSize = (int) (((data[writeIndex - 1] - data[0]) / minDeltaBase) + 1);
        writeIndex = newBlockSize;
      }
    }

    @Override
    protected void writeBitmap(ByteArrayOutputStream out) throws IOException {
      // generate bitmap
      data2Diff(data);
      byte[] bsArr = bitmap.toByteArray();
      out.write(BytesUtils.intToBytes(bsArr.length));
      out.write(bsArr);
    }

    /**
     * input a integer or long value.
     *
     * @param value value to encode
     * @param out - the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(long value, ByteArrayOutputStream out) throws IOException {
      if (writeIndex == -1) {
        data = new long[blockSize];
        writeIndex = 0;
      }
      data[writeIndex++] = value;
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    private void data2Diff(long[] missingPointData) {
      bitmap = new BitSet(newBlockSize);
      bitmap.flip(0, newBlockSize);
      int offset = 0;
      for (int i = 1; i < dataTotal; i++) {
        long delta = missingPointData[i] - missingPointData[i - 1];
        if (delta != minDeltaBase) {
          int missingPointNum = (int) (delta / minDeltaBase) - 1;
          for (int j = 0; j < missingPointNum; j++) {
            bitmap.set(i + (offset++), false);
          }
        }
      }
    }
  }
}
