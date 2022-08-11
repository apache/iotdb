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
package org.apache.iotdb.tool.core.service;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.nio.ByteBuffer;

public class TsFileValuePageReader {

  private static final int MASK = 0x80;

  private final PageHeader pageHeader;

  private final TSDataType dataType;

  /** decoder for value column */
  private final Decoder valueDecoder;

  private byte[] bitmap;

  private int size;

  /** value column in memory */
  protected ByteBuffer valueBuffer;

  public TsFileValuePageReader(
      PageHeader pageHeader, ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.pageHeader = pageHeader;
    if (pageData != null) {
      splitDataToBitmapAndValue(pageData);
    }
    this.valueBuffer = pageData;
  }

  private void splitDataToBitmapAndValue(ByteBuffer pageData) {
    if (!pageData.hasRemaining()) { // Empty Page
      return;
    }
    this.size = ReadWriteIOUtils.readInt(pageData);
    this.bitmap = new byte[(size + 7) / 8];
    pageData.get(bitmap);
    this.valueBuffer = pageData.slice();
  }

  /**
   * return the value array of the corresponding time, if this sub sensor don't have a value in a
   * time, just fill it with null
   */
  public TsPrimitiveType[] allValueBatch() {
    TsPrimitiveType[] valueBatch = new TsPrimitiveType[size];
    if (valueBuffer == null) {
      return valueBatch;
    }
    for (int i = 0; i < size; i++) {
      if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          valueBatch[i] = new TsPrimitiveType.TsBoolean(aBoolean);
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          valueBatch[i] = new TsPrimitiveType.TsInt(anInt);
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          valueBatch[i] = new TsPrimitiveType.TsLong(aLong);
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          valueBatch[i] = new TsPrimitiveType.TsFloat(aFloat);
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          valueBatch[i] = new TsPrimitiveType.TsDouble(aDouble);
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          valueBatch[i] = new TsPrimitiveType.TsBinary(aBinary);
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return valueBatch;
  }

  public TSDataType getDataType() {
    return dataType;
  }
}
