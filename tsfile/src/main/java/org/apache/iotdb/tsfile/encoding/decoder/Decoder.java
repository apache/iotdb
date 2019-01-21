/**
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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class Decoder {

  public TSEncoding type;

  public Decoder(TSEncoding type) {
    this.type = type;
  }

  /**
   * get Decoder object by type.
   *
   * @param type TSEncoding type
   * @param dataType TSDataType
   * @return Decoder object
   */
  public static Decoder getDecoderByType(TSEncoding type, TSDataType dataType) {
    // PLA and DFT encoding are not supported in current version
    if (type == TSEncoding.PLAIN) {
      return new PlainDecoder(EndianType.LITTLE_ENDIAN);
    } else if (type == TSEncoding.RLE && dataType == TSDataType.BOOLEAN) {
      return new IntRleDecoder(EndianType.LITTLE_ENDIAN);
    } else if (type == TSEncoding.TS_2DIFF && dataType == TSDataType.INT32) {
      return new DeltaBinaryDecoder.IntDeltaDecoder();
    } else if (type == TSEncoding.TS_2DIFF && dataType == TSDataType.INT64) {
      return new DeltaBinaryDecoder.LongDeltaDecoder();
    } else if (type == TSEncoding.RLE && dataType == TSDataType.INT32) {
      return new IntRleDecoder(EndianType.LITTLE_ENDIAN);
    } else if (type == TSEncoding.RLE && dataType == TSDataType.INT64) {
      return new LongRleDecoder(EndianType.LITTLE_ENDIAN);
    } else if ((dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE)
        && (type == TSEncoding.RLE || type == TSEncoding.TS_2DIFF)) {
      return new FloatDecoder(TSEncoding.valueOf(type.toString()), dataType);
    } else if (type == TSEncoding.GORILLA && dataType == TSDataType.FLOAT) {
      return new SinglePrecisionDecoder();
    } else if (type == TSEncoding.GORILLA && dataType == TSDataType.DOUBLE) {
      return new DoublePrecisionDecoder();
    } else {
      throw new TsFileDecodingException(
          "Decoder not found:" + type + " , DataType is :" + dataType);
    }
  }

  public int readInt(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readInt is not supproted by Decoder");
  }

  public boolean readBoolean(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBoolean is not supproted by Decoder");
  }

  public short readShort(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readShort is not supproted by Decoder");
  }

  public long readLong(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readLong is not supproted by Decoder");
  }

  public float readFloat(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readFloat is not supproted by Decoder");
  }

  public double readDouble(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readDouble is not supproted by Decoder");
  }

  public Binary readBinary(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBinary is not supproted by Decoder");
  }

  public BigDecimal readBigDecimal(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBigDecimal is not supproted by Decoder");
  }

  public abstract boolean hasNext(ByteBuffer buffer) throws IOException;

  public abstract void reset();

}
