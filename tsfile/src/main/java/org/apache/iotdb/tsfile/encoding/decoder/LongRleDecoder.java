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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.encoding.bitpacking.LongPacker;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Decoder for long value using rle or bit-packing. */
public class LongRleDecoder extends RleDecoder {

  private static final Logger logger = LoggerFactory.getLogger(LongRleDecoder.class);

  /** current value for rle repeated value. */
  private long currentValue;

  /** buffer to save all values in group using bit-packing. */
  private long[] currentBuffer;

  /** packer for unpacking long value. */
  private LongPacker packer;

  public LongRleDecoder() {
    super();
    currentValue = 0;
  }

  /**
   * read a long value from InputStream.
   *
   * @param buffer - InputStream
   * @return value - current valid value
   */
  @Override
  public long readLong(ByteBuffer buffer) {
    if (!isLengthAndBitWidthReaded) {
      // start to read a new rle+bit-packing pattern
      readLengthAndBitWidth(buffer);
    }

    if (currentCount == 0) {
      try {
        readNext();
      } catch (IOException e) {
        logger.error(
            "tsfile-encoding IntRleDecoder: error occurs when reading all encoding number, length "
                + "is {}, bit width is {}",
            length,
            bitWidth,
            e);
      }
    }
    --currentCount;
    long result;
    switch (mode) {
      case RLE:
        result = currentValue;
        break;
      case BIT_PACKED:
        result = currentBuffer[bitPackingNum - currentCount - 1];
        break;
      default:
        throw new TsFileDecodingException(
            String.format("tsfile-encoding LongRleDecoder: not a valid mode %s", mode));
    }

    if (!hasNextPackage()) {
      isLengthAndBitWidthReaded = false;
    }
    return result;
  }

  @Override
  protected void initPacker() {
    packer = new LongPacker(bitWidth);
  }

  @Override
  protected void readNumberInRle() throws IOException {
    currentValue =
        ReadWriteForEncodingUtils.readLongLittleEndianPaddedOnBitWidth(byteCache, bitWidth);
  }

  @Override
  protected void readBitPackingBuffer(int bitPackedGroupCount, int lastBitPackedNum) {
    currentBuffer = new long[bitPackedGroupCount * TSFileConfig.RLE_MIN_REPEATED_NUM];
    byte[] bytes = new byte[bitPackedGroupCount * bitWidth];
    int bytesToRead = bitPackedGroupCount * bitWidth;
    bytesToRead = Math.min(bytesToRead, byteCache.remaining());
    byteCache.get(bytes, 0, bytesToRead);

    // save all long values in currentBuffer
    packer.unpackAllValues(bytes, bytesToRead, currentBuffer);
  }
}
