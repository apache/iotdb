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

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PageReader {

  private PageHeader pageHeader;

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
    this(null, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public PageReader(PageHeader pageHeader, ByteBuffer pageData, TSDataType dataType,
                    Decoder valueDecoder, Decoder timeDecoder, Filter filter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.filter = filter;
    this.pageHeader = pageHeader;
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
            pageData.putBoolean(timestamp, aBoolean);
          }
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, anInt))) {
            pageData.putInt(timestamp, anInt);
          }
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aLong))) {
            pageData.putLong(timestamp, aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aFloat))) {
            pageData.putFloat(timestamp, aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aDouble))) {
            pageData.putDouble(timestamp, aDouble);
          }
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (timestamp > deletedAt && (filter == null || filter.satisfy(timestamp, aBinary))) {
            pageData.putBinary(timestamp, aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return pageData;
  }

  public Statistics getStatistics() {
    return pageHeader.getStatistics();
  }

  public void close() {
    timeBuffer = null;
    valueBuffer = null;
  }

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }
}
