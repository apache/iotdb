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
package org.apache.iotdb.tsfile.read.reader.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

public class PageReader {

  private TSDataType dataType;

  /** decoder for value column */
  private Decoder valueDecoder;

  /** decoder for time column */
  private Decoder timeDecoder;

  /** time column in memory */
  private ByteBuffer timeBuffer;

  /** value column in memory */
  private ByteBuffer valueBuffer;

  private Filter filter;

  /** Data whose timestamp <= deletedAt should be considered deleted(not be returned). */
  private long deletedAt = Long.MIN_VALUE;

  public PageReader(ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder,
      Decoder timeDecoder, Filter filter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.filter = filter;
    splitDataToTimeStampAndValue(pageData);
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /**
   * @return the returned BatchData may be empty, but never be null
   */
  public BatchData getAllSatisfiedPageData() throws IOException {

    BatchData pageData = new BatchData(dataType);

    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);

      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aBoolean))) {
            pageData.putTime(timestamp);
            pageData.putBoolean(aBoolean);
          }
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, anInt))) {
            pageData.putTime(timestamp);
            pageData.putInt(anInt);
          }
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aLong))) {
            pageData.putTime(timestamp);
            pageData.putLong(aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aFloat))) {
            pageData.putTime(timestamp);
            pageData.putFloat(aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aDouble))) {
            pageData.putTime(timestamp);
            pageData.putDouble(aDouble);
          }
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aBinary))) {
            pageData.putTime(timestamp);
            pageData.putBinary(aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return pageData;
  }


  public void close() {
    timeBuffer = null;
    valueBuffer = null;
  }

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }
}
