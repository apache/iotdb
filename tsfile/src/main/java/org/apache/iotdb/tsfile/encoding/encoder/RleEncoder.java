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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes values using a combination of run length encoding and bit packing, according to the
 * following grammar:
 *
 * <pre>{@code
 * rle-bit-packing-hybrid: <length> <bitwidth> <encoded-data>
 * length := length of the <bitwidth> <encoded-data> in bytes stored as 4 bytes little endian
 * bitwidth := bitwidth for all encoded data in <encoded-data>
 * encoded-data := <run>*
 * run := <bit-packed-run> | <rle-run>
 * bit-packed-run := <bit-packed-header> <lastBitPackedNum> <bit-packed-values>
 * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
 * lastBitPackedNum := the number of useful value in last bit-pack may be less than 8, so
 * lastBitPackedNum indicates how many values are useful
 * bit-packed-values :=  bit packed
 * rle-run := <rle-header> <repeated-value>
 * rle-header := varint-encode( (number of times repeated) << 1)
 * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
 * }</pre>
 *
 * .
 *
 * @param <T> data type T for RLE
 */
public abstract class RleEncoder<T extends Comparable<T>> extends Encoder {

  private static final Logger logger = LoggerFactory.getLogger(RleEncoder.class);

  /** we save all value in a list and calculate its bitwidth. */
  protected List<T> values;

  /** the bit width used for bit-packing and rle. */
  protected int bitWidth;

  /** for a given value now buffered, how many times it occurs. */
  protected int repeatCount;

  /** the number of group which using bit packing, it is saved in header. */
  protected int bitPackedGroupCount;

  /** the number of buffered value in array. */
  protected int numBufferedValues;

  /**
   * we will write all bytes using bit-packing to OutputStream once. Before that, all bytes are
   * saved in list.
   */
  protected List<byte[]> bytesBuffer;

  /** flag which indicate encoding mode false -- rle true -- bit-packing. */
  protected boolean isBitPackRun;

  /** previous value written, used to detect repeated values. */
  protected T preValue;

  /** array to buffer values temporarily. */
  protected T[] bufferedValues;

  protected boolean isBitWidthSaved;

  /** output stream to buffer {@code <bitwidth> <encoded-data>}. */
  protected ByteArrayOutputStream byteCache;

  protected TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  /** constructor. */
  protected RleEncoder() {
    super(TSEncoding.RLE);
    bytesBuffer = new ArrayList<>();
    isBitPackRun = false;
    isBitWidthSaved = false;
    byteCache = new ByteArrayOutputStream();
  }

  protected void reset() {
    numBufferedValues = 0;
    repeatCount = 0;
    bitPackedGroupCount = 0;
    bytesBuffer.clear();
    isBitPackRun = false;
    isBitWidthSaved = false;
    byteCache.reset();
    values.clear();
  }

  /**
   * Write all values buffered in cache to OutputStream.
   *
   * @param out - byteArrayOutputStream
   * @throws IOException cannot flush to OutputStream
   */
  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    int lastBitPackedNum = numBufferedValues;
    if (repeatCount >= TSFileConfig.RLE_MIN_REPEATED_NUM) {
      try {
        writeRleRun();
      } catch (IOException e) {
        logger.error(
            "tsfile-encoding RleEncoder : error occurs when writing nums to OutputStram "
                + "when flushing left nums. "
                + "numBufferedValues {}, repeatCount {}, bitPackedGroupCount{}, "
                + "isBitPackRun {}, isBitWidthSaved {}",
            numBufferedValues,
            repeatCount,
            bitPackedGroupCount,
            isBitPackRun,
            isBitWidthSaved,
            e);
        throw e;
      }
    } else if (numBufferedValues > 0) {
      clearBuffer();
      writeOrAppendBitPackedRun();
      endPreviousBitPackedRun(lastBitPackedNum);
    } else {
      endPreviousBitPackedRun(TSFileConfig.RLE_MIN_REPEATED_NUM);
    }
    // write length
    ReadWriteForEncodingUtils.writeUnsignedVarInt(byteCache.size(), out);
    byteCache.writeTo(out);
    reset();
  }

  /**
   * Write bytes to OutputStream using rle. rle format: {@code [header][value] header: (repeated
   * value) << 1}
   *
   * @throws IOException cannot write RLE run
   */
  protected abstract void writeRleRun() throws IOException;

  /** Start a bit-packing run transform values to bytes and buffer them in cache. */
  public void writeOrAppendBitPackedRun() {
    if (bitPackedGroupCount >= TSFileConfig.RLE_MAX_BIT_PACKED_NUM) {
      // we've packed as many values as we can for this run,
      // end it and start a new one
      endPreviousBitPackedRun(TSFileConfig.RLE_MIN_REPEATED_NUM);
    }
    if (!isBitPackRun) {
      isBitPackRun = true;
    }

    convertBuffer();

    numBufferedValues = 0;
    repeatCount = 0;
    ++bitPackedGroupCount;
  }

  /**
   * End a bit-packing run write all bit-packing group to OutputStream bit-packing format: {@code
   * [header][lastBitPackedNum][bit-packing group]+ [bit-packing group]+ are saved in List<byte[]>
   * bytesBuffer }.
   *
   * @param lastBitPackedNum - in last bit-packing group, it may have useful values less than 8.
   *     This param indicates how many values are useful
   */
  protected void endPreviousBitPackedRun(int lastBitPackedNum) {
    if (!isBitPackRun) {
      return;
    }
    byte bitPackHeader = (byte) ((bitPackedGroupCount << 1) | 1);
    byteCache.write(bitPackHeader);
    byteCache.write(lastBitPackedNum);
    for (byte[] bytes : bytesBuffer) {
      byteCache.write(bytes, 0, bytes.length);
    }
    bytesBuffer.clear();
    isBitPackRun = false;
    bitPackedGroupCount = 0;
  }

  /**
   * Encode T value using rle or bit-packing. It may not write to OutputStream immediately
   *
   * @param value - value to encode
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  protected void encodeValue(T value) {
    if (!isBitWidthSaved) {
      // save bit width in header,
      // perpare for read
      byteCache.write(bitWidth);
      isBitWidthSaved = true;
    }
    if (value.equals(preValue)) {
      repeatCount++;
      if (repeatCount >= TSFileConfig.RLE_MIN_REPEATED_NUM
          && repeatCount <= TSFileConfig.RLE_MAX_REPEATED_NUM) {
        // value occurs more than RLE_MIN_REPEATED_NUM times but less than
        // EncodingConfig.RLE_MAX_REPEATED_NUM
        // we'll use rle, so just keep on counting repeats for now
        // we'll write current value to OutputStream when we encounter a different value
        return;
      } else if (repeatCount == TSFileConfig.RLE_MAX_REPEATED_NUM + 1) {
        // value occurs more than EncodingConfig.RLE_MAX_REPEATED_NUM
        // we'll write current rle run to stream and keep on counting current value
        repeatCount = TSFileConfig.RLE_MAX_REPEATED_NUM;
        try {
          writeRleRun();
          logger.debug("tsfile-encoding RleEncoder : write full rle run to stream");
        } catch (IOException e) {
          logger.error(
              " error occurs when writing full rle run to OutputStram when repeatCount = {}."
                  + "numBufferedValues {}, repeatCount {}, bitPackedGroupCount{}, "
                  + "isBitPackRun {}, isBitWidthSaved {}",
              TSFileConfig.RLE_MAX_REPEATED_NUM + 1,
              numBufferedValues,
              repeatCount,
              bitPackedGroupCount,
              isBitPackRun,
              isBitWidthSaved,
              e);
        }
        repeatCount = 1;
        preValue = value;
      }

    } else {
      // we encounter a differnt value
      if (repeatCount >= TSFileConfig.RLE_MIN_REPEATED_NUM) {
        try {
          writeRleRun();
        } catch (IOException e) {
          logger.error(
              "tsfile-encoding RleEncoder : error occurs when writing num to OutputStram "
                  + "when repeatCount > {}."
                  + "numBufferedValues {}, repeatCount {}, bitPackedGroupCount{}, isBitPackRun {}, "
                  + "isBitWidthSaved {}",
              TSFileConfig.RLE_MIN_REPEATED_NUM,
              numBufferedValues,
              repeatCount,
              bitPackedGroupCount,
              isBitPackRun,
              isBitWidthSaved,
              e);
        }
      }
      repeatCount = 1;
      preValue = value;
    }
    bufferedValues[numBufferedValues] = value;
    numBufferedValues++;
    // if none of value we encountered occurs more MAX_REPEATED_NUM times
    // we'll use bit-packing
    if (numBufferedValues == TSFileConfig.RLE_MIN_REPEATED_NUM) {
      writeOrAppendBitPackedRun();
    }
  }

  /** clean all useless value in bufferedValues and set 0. */
  protected abstract void clearBuffer();

  protected abstract void convertBuffer();

  @Override
  public void encode(boolean value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }

  @Override
  public void encode(short value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }

  @Override
  public void encode(BigDecimal value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(getClass().getName());
  }
}
