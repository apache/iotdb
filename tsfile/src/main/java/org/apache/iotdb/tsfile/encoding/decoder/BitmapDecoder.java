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

import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decoder switch or enums value using bitmap, bitmap-encoding:. {@code <length> <num>
 * <encoded-data>}
 *
 * @deprecated (2019.1.25, The bitmap data type has been removed., We can reserve this class, and
 *     reuse it later.)
 */
@Deprecated
public class BitmapDecoder extends Decoder {

  private static final Logger logger = LoggerFactory.getLogger(BitmapDecoder.class);

  /** how many bytes for all encoded data in input stream. */
  private int length;

  /** number of encoded data. */
  private int number;

  /** number of data left for reading in current buffer. */
  private int currentCount;

  /**
   * each time decoder receives a inputstream, decoder creates a buffer to save all encoded data.
   */
  private ByteBuffer byteCache;

  /** decoder reads all bitmap index from byteCache and save in Map(value, bitmap index). */
  private Map<Integer, byte[]> buffer;

  /** BitmapDecoder constructor. */
  public BitmapDecoder() {
    super(TSEncoding.BITMAP);
    this.reset();
    logger.debug("tsfile-encoding BitmapDecoder: init bitmap decoder");
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    if (currentCount == 0) {
      reset();
      getLengthAndNumber(buffer);
      readNext();
    }
    int result = 0;
    int index = (number - currentCount) / 8;
    int offset = 7 - ((number - currentCount) % 8);
    for (Map.Entry<Integer, byte[]> entry : this.buffer.entrySet()) {
      byte[] tmp = entry.getValue();
      if (((tmp[index] & 0xff) & ((byte) 1 << offset)) != 0) {
        result = entry.getKey();
      }
    }
    currentCount--;
    return result;
  }

  private void getLengthAndNumber(ByteBuffer buffer) {
    this.length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    this.number = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    // TODO maybe this.byteCache = buffer is faster, but not safe
    byte[] tmp = new byte[length];
    buffer.get(tmp, 0, length);
    this.byteCache = ByteBuffer.wrap(tmp);
  }

  /** Decode all data from buffer and save them. */
  private void readNext() {
    int len = (this.number + 7) / 8;
    while (byteCache.remaining() > 0) {
      int value = ReadWriteForEncodingUtils.readUnsignedVarInt(byteCache);
      byte[] tmp = new byte[len];
      byteCache.get(tmp, 0, len);
      buffer.put(value, tmp);
    }
    currentCount = number;
  }

  @Override
  public void reset() {
    this.length = 0;
    this.number = 0;
    this.currentCount = 0;
    if (this.byteCache == null) {
      this.byteCache = ByteBuffer.allocate(0);
    } else {
      this.byteCache.position(0);
    }
    if (this.buffer == null) {
      this.buffer = new HashMap<>();
    } else {
      this.buffer.clear();
    }
  }

  /**
   * For special value in page list, get its bitmap index.
   *
   * @param target value to get its bitmap index
   * @param pageList input page list
   * @return List(Pair of ( length, bitmap index) )
   */
  public List<Pair<Integer, byte[]>> decodeAll(int target, List<ByteBuffer> pageList) {
    List<Pair<Integer, byte[]>> resultList = new ArrayList<>();
    for (ByteBuffer bufferPage : pageList) {
      reset();
      getLengthAndNumber(bufferPage);
      int byteArrayLength = (this.number + 7) / 8;
      byte[] tmp = new byte[byteArrayLength];
      while (byteCache.remaining() > 0) {
        int value = ReadWriteForEncodingUtils.readUnsignedVarInt(byteCache);
        if (value == target) {
          byteCache.get(tmp, 0, byteArrayLength);
          break;
        } else {
          byteCache.position(byteCache.position() + byteArrayLength);
        }
      }

      resultList.add(new Pair<>(this.number, tmp));
      logger.debug(
          "tsfile-encoding BitmapDecoder: number {} in current page, byte length {}",
          this.number,
          byteArrayLength);
    }
    return resultList;
  }

  /**
   * Check whether there is number left for reading.
   *
   * @param buffer : decoded data saved in InputStream
   * @return true or false to indicate whether there is number left
   * @throws IOException cannot read next value
   */
  @Override
  public boolean hasNext(ByteBuffer buffer) {
    if (currentCount > 0 || buffer.remaining() > 0) {
      return true;
    }
    return false;
  }

  /**
   * In current version, boolean value is equal to Enums value in schema.
   *
   * @param buffer : decoded data saved in InputStream
   * @throws TsFileDecodingException cannot read next value
   */
  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBoolean is not supported by BitmapDecoder");
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readShort is not supported by BitmapDecoder");
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readLong is not supported by BitmapDecoder");
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readFloat is not supported by BitmapDecoder");
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readDouble is not supported by BitmapDecoder");
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBinary is not supported by BitmapDecoder");
  }

  @Override
  public BigDecimal readBigDecimal(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBigDecimal is not supported by BitmapDecoder");
  }
}
