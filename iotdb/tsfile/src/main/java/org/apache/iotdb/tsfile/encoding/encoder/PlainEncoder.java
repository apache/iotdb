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
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

public class PlainEncoder extends Encoder {

  private static final Logger logger = LoggerFactory.getLogger(PlainEncoder.class);
  private TSDataType dataType;
  private int maxStringLength;

  public PlainEncoder(TSDataType dataType, int maxStringLength) {
    super(TSEncoding.PLAIN);
    this.dataType = dataType;
    this.maxStringLength = maxStringLength;
  }

  @Override
  public void encode(boolean value, ByteArrayOutputStream out) {
    if (value) {
      out.write(1);
    } else {
      out.write(0);
    }
  }

  @Override
  public void encode(short value, ByteArrayOutputStream out) {
    out.write((value >> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    ReadWriteForEncodingUtils.writeVarInt(value, out);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    for (int i = 7; i >= 0; i--) {
      out.write((byte) (((value) >> (i * 8)) & 0xFF));
    }
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    int floatInt = Float.floatToIntBits(value);
    out.write((floatInt >> 24) & 0xFF);
    out.write((floatInt >> 16) & 0xFF);
    out.write((floatInt >> 8) & 0xFF);
    out.write(floatInt & 0xFF);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    encode(Double.doubleToLongBits(value), out);
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    try {
      // write the length of the bytes
      encode(value.getLength(), out);
      // write value
      out.write(value.getValues());
    } catch (IOException e) {
      logger.error(
          "tsfile-encoding PlainEncoder: error occurs when encode Binary value {}", value, e);
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    // This is an empty function.
  }

  @Override
  public int getOneItemMaxSize() {
    switch (dataType) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 4;
      case INT64:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case TEXT:
        // refer to encode(Binary,ByteArrayOutputStream)
        return 4 + TSFileConfig.BYTE_SIZE_PER_CHAR * maxStringLength;
      default:
        throw new UnsupportedOperationException(dataType.toString());
    }
  }

  @Override
  public long getMaxByteSize() {
    return 0;
  }

  @Override
  public void encode(BigDecimal value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        "tsfile-encoding PlainEncoder: current version does not support BigDecimal value encoding");
  }
}
