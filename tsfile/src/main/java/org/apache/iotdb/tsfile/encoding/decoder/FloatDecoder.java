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

import org.apache.iotdb.tsfile.encoding.encoder.FloatEncoder;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decoder for float or double value using rle or two diff. For more info about encoding pattern,
 * see{@link FloatEncoder}
 */
public class FloatDecoder extends Decoder {

  private static final Logger logger = LoggerFactory.getLogger(FloatDecoder.class);
  private Decoder decoder;

  /** maxPointValue = 10^(maxPointNumer). maxPointNumber can be read from the stream. */
  private double maxPointValue;

  /** flag that indicates whether we have read maxPointNumber and calculated maxPointValue. */
  private boolean isMaxPointNumberRead;

  public FloatDecoder(TSEncoding encodingType, TSDataType dataType) {
    super(encodingType);
    if (encodingType == TSEncoding.RLE) {
      if (dataType == TSDataType.FLOAT) {
        decoder = new IntRleDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using int-rle and float");
      } else if (dataType == TSDataType.DOUBLE) {
        decoder = new LongRleDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using long-rle and double");
      } else {
        throw new TsFileDecodingException(
            String.format("data type %s is not supported by FloatDecoder", dataType));
      }
    } else if (encodingType == TSEncoding.TS_2DIFF) {
      if (dataType == TSDataType.FLOAT) {
        decoder = new DeltaBinaryDecoder.IntDeltaDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using int-delta and float");
      } else if (dataType == TSDataType.DOUBLE) {
        decoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using long-delta and double");
      } else {
        throw new TsFileDecodingException(
            String.format("data type %s is not supported by FloatDecoder", dataType));
      }
    } else {
      throw new TsFileDecodingException(
          String.format("%s encoding is not supported by FloatDecoder", encodingType));
    }
    isMaxPointNumberRead = false;
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    readMaxPointValue(buffer);
    int value = decoder.readInt(buffer);
    double result = value / maxPointValue;
    return (float) result;
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    readMaxPointValue(buffer);
    long value = decoder.readLong(buffer);
    return value / maxPointValue;
  }

  private void readMaxPointValue(ByteBuffer buffer) {
    if (!isMaxPointNumberRead) {
      int maxPointNumber = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      if (maxPointNumber <= 0) {
        maxPointValue = 1;
      } else {
        maxPointValue = Math.pow(10, maxPointNumber);
      }
      isMaxPointNumberRead = true;
    }
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (decoder == null) {
      return false;
    }
    return decoder.hasNext(buffer);
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBinary is not supproted by FloatDecoder");
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBoolean is not supproted by FloatDecoder");
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readShort is not supproted by FloatDecoder");
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readInt is not supproted by FloatDecoder");
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readLong is not supproted by FloatDecoder");
  }

  @Override
  public void reset() {
    this.decoder.reset();
    this.isMaxPointNumberRead = false;
  }
}
