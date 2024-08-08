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
package org.apache.iotdb.tsfile.v2.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.PlainDecoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PageReaderV2 extends PageReader {

  public PageReaderV2(
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public PageReaderV2(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    super(pageHeader, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {

    if (dataType != TSDataType.INT32 && dataType != TSDataType.TEXT) {
      return super.getAllSatisfiedPageData(ascending);
    }

    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);

    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      switch (dataType) {
        case INT32:
          int anInt =
              (valueDecoder instanceof PlainDecoder)
                  ? valueBuffer.getInt()
                  : valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
            pageData.putInt(timestamp, anInt);
          }
          break;
        case TEXT:
          int length = valueBuffer.getInt();
          byte[] buf = new byte[length];
          valueBuffer.get(buf, 0, buf.length);
          Binary aBinary = new Binary(buf);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
            pageData.putBinary(timestamp, aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return pageData.flip();
  }
}
