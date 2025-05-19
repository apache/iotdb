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
package org.apache.iotdb.session.rpccompress.decoder;

import org.apache.iotdb.session.rpccompress.ColumnEntry;
import org.apache.iotdb.session.rpccompress.EncodingTypeNotSupportedException;
import org.apache.iotdb.session.rpccompress.MetaHead;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class RpcDecoder {
  private MetaHead timeMeta = new MetaHead();
  private MetaHead valueMeta = new MetaHead();

  private Integer timeMetaLen = 0;
  private Integer valueMetaLen = 0;

  /**
   * read timestamps
   *
   * @param buffer ByteBuffer
   * @param size Number of rows
   */
  public long[] readTimesFromBuffer(ByteBuffer buffer, int size) {
    int savePosition = buffer.position();
    readMetaHeadForTimeStamp(buffer);
    buffer.position(savePosition);
    return decodeColumnForTimeStamp(buffer, size);
  }

  public void readMetaHeadForTimeStamp(ByteBuffer buffer) {
    this.timeMeta = readMetaHead(buffer, length -> this.timeMetaLen = length);
  }

  public void readMetaHeadForValues(ByteBuffer buffer) {
    this.valueMeta = readMetaHead(buffer, length -> this.valueMetaLen = length);
  }

  public MetaHead readMetaHead(ByteBuffer buffer, Consumer<Integer> length) {
    // 1. read metaHeader length
    int MetaHeaderLength = buffer.getInt(buffer.limit() - Integer.BYTES);
    length.accept(MetaHeaderLength);
    // 2. read metaHeader
    byte[] metaBytes = new byte[MetaHeaderLength];
    buffer.position(buffer.limit() - Integer.BYTES - MetaHeaderLength);
    buffer.get(metaBytes);
    // 3. Deserialize metaHeader
    return MetaHead.fromBytes(metaBytes);
  }

  public long[] decodeColumnForTimeStamp(ByteBuffer buffer, int rowCount) {
    ColumnEntry columnEntry = timeMeta.getColumnEntries().get(0);
    TSDataType dataType = columnEntry.getDataType();
    TSEncoding encodingType = columnEntry.getEncodingType();
    ColumnDecoder columnDecoder = createDecoder(dataType, encodingType);

    return columnDecoder.decodeLongColumn(buffer, columnEntry, rowCount);
  }

  public Object[] decodeValues(ByteBuffer buffer, int rowCount) {
    int savePosition = buffer.position();
    readMetaHeadForValues(buffer);
    buffer.position(savePosition);
    int columnNum = valueMeta.getColumnEntries().size();
    Object[] values = new Object[columnNum];
    for (int i = 0; i < columnNum; i++) {
      values[i] = decodeColumn(i, buffer, rowCount);
    }
    return values;
  }

  public Object decodeColumn(int columnIndex, ByteBuffer buffer, int rowCount) {
    ColumnEntry columnEntry = valueMeta.getColumnEntries().get(columnIndex);
    TSDataType dataType = columnEntry.getDataType();
    TSEncoding encodingType = columnEntry.getEncodingType();
    ColumnDecoder columnDecoder = createDecoder(dataType, encodingType);

    return columnDecoder.decode(buffer, columnEntry, rowCount);
  }

  private ColumnDecoder createDecoder(TSDataType dataType, TSEncoding encodingType) {
    switch (encodingType) {
      case PLAIN:
        return new PlainColumnDecoder(dataType);
      case RLE:
        return new RleColumnDecoder(dataType);
      case TS_2DIFF:
        return new Ts2DiffColumnDecoder(dataType);
      default:
        throw new EncodingTypeNotSupportedException(encodingType.name());
    }
  }
}
