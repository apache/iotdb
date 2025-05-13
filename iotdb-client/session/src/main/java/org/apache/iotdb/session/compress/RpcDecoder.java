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
package org.apache.iotdb.session.compress;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;

public class RpcDecoder {
  private MetaHead metaHead = new MetaHead();
  private MetaHead metaHeadForTimeStamp = new MetaHead();
  private Integer MetaHeaderLength = 0;
  private Integer MetaHeaderForTimeStampLength = 0;

  public void readMetaHead(ByteBuffer buffer) {
    this.MetaHeaderLength = buffer.getInt(buffer.limit() - Integer.BYTES);
    byte[] metaBytes = new byte[MetaHeaderLength];
    buffer.position(buffer.limit() - Integer.BYTES - MetaHeaderLength);
    buffer.get(metaBytes);
    this.metaHead = MetaHead.fromBytes(metaBytes);
  }

  public void readMetaHeadForTimeStamp(ByteBuffer buffer) {
    this.MetaHeaderForTimeStampLength = buffer.getInt(buffer.limit() - Integer.BYTES);
    byte[] metaBytes = new byte[MetaHeaderForTimeStampLength];
    buffer.position(buffer.limit() - Integer.BYTES - MetaHeaderForTimeStampLength);
    buffer.get(metaBytes);
    this.metaHeadForTimeStamp = MetaHead.fromBytes(metaBytes);
  }

  public long[] readTimesFromBuffer(ByteBuffer buffer, int size) {
    return decodeTimestamps(buffer, size);
  }

  public long[] decodeTimestamps(ByteBuffer buffer, int size) {
    int savePosition = buffer.position();
    readMetaHeadForTimeStamp(buffer);
    buffer.position(savePosition);
    long[] timestamps = (long[]) decodeColumnForTimeStamp(buffer, size);
    return timestamps;
  }

  public Object[] decodeValues(ByteBuffer buffer, int rowCount) {
    int savePosition = buffer.position();
    readMetaHead(buffer);
    buffer.position(savePosition);
    int columnNum = metaHead.getColumnEntries().size();
    Object[] values = new Object[columnNum];
    for (int i = 0; i < columnNum; i++) {
      Object value = decodeColumn(i, buffer, rowCount);
      values[i] = value;
    }
    return values;
  }

  public Object decodeColumn(int columnIndex, ByteBuffer buffer, int rowCount) {
    ColumnEntry columnEntry = metaHead.getColumnEntries().get(columnIndex);
    TSDataType dataType = columnEntry.getDataType();
    TSEncoding encodingType = columnEntry.getEncodingType();
    ColumnDecoder columnDecoder = createDecoder(dataType, encodingType);

    return columnDecoder.decode(buffer, columnEntry, rowCount);
  }

  public Object decodeColumnForTimeStamp(ByteBuffer buffer, int rowCount) {
    ColumnEntry columnEntry = metaHeadForTimeStamp.getColumnEntries().get(0);
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
