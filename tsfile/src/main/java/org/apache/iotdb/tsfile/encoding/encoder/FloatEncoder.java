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

import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Encoder for float or double value using rle or two-diff according to following grammar.
 *
 * <pre>{@code
 * float encoder: <maxPointvalue> <encoded-data>
 * maxPointvalue := number for accuracy of decimal places, store as unsigned var int
 * encoded-data := same as encoder's pattern
 * }</pre>
 */
public class FloatEncoder extends Encoder {

  private Encoder encoder;

  /** number for accuracy of decimal places. */
  private int maxPointNumber;

  /** maxPointValue = 10^(maxPointNumber). */
  private double maxPointValue;

  /** flag to check whether maxPointNumber is saved in the stream. */
  private boolean isMaxPointNumberSaved;

  public FloatEncoder(TSEncoding encodingType, TSDataType dataType, int maxPointNumber) {
    super(encodingType);
    this.maxPointNumber = maxPointNumber;
    calculateMaxPonitNum();
    isMaxPointNumberSaved = false;
    if (encodingType == TSEncoding.RLE) {
      if (dataType == TSDataType.FLOAT) {
        encoder = new IntRleEncoder();
      } else if (dataType == TSDataType.DOUBLE) {
        encoder = new LongRleEncoder();
      } else {
        throw new TsFileEncodingException(
            String.format("data type %s is not supported by FloatEncoder", dataType));
      }
    } else if (encodingType == TSEncoding.TS_2DIFF) {
      if (dataType == TSDataType.FLOAT) {
        encoder = new DeltaBinaryEncoder.IntDeltaEncoder();
      } else if (dataType == TSDataType.DOUBLE) {
        encoder = new DeltaBinaryEncoder.LongDeltaEncoder();
      } else {
        throw new TsFileEncodingException(
            String.format("data type %s is not supported by FloatEncoder", dataType));
      }
    } else {
      throw new TsFileEncodingException(
          String.format("%s encoding is not supported by FloatEncoder", encodingType));
    }
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    saveMaxPointNumber(out);
    int valueInt = convertFloatToInt(value);
    encoder.encode(valueInt, out);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    saveMaxPointNumber(out);
    long valueLong = convertDoubleToLong(value);
    encoder.encode(valueLong, out);
  }

  private void calculateMaxPonitNum() {
    if (maxPointNumber <= 0) {
      maxPointNumber = 0;
      maxPointValue = 1;
    } else {
      maxPointValue = Math.pow(10, maxPointNumber);
    }
  }

  private int convertFloatToInt(float value) {
    return (int) Math.round(value * maxPointValue);
  }

  private long convertDoubleToLong(double value) {
    return Math.round(value * maxPointValue);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    encoder.flush(out);
    reset();
  }

  private void reset() {
    isMaxPointNumberSaved = false;
  }

  private void saveMaxPointNumber(ByteArrayOutputStream out) {
    if (!isMaxPointNumberSaved) {
      ReadWriteForEncodingUtils.writeUnsignedVarInt(maxPointNumber, out);
      isMaxPointNumberSaved = true;
    }
  }

  @Override
  public int getOneItemMaxSize() {
    return encoder.getOneItemMaxSize();
  }

  @Override
  public long getMaxByteSize() {
    return encoder.getMaxByteSize();
  }
}
