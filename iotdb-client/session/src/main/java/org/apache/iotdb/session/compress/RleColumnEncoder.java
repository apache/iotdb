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

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.util.List;

public class RleColumnEncoder implements ColumnEncoder {
  private final Encoder encoder;
  private final TSDataType dataType;
  private ColumnEntry columnEntry;

  public RleColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = getEncoder(dataType, TSEncoding.RLE);
    columnEntry = new ColumnEntry();
  }

  @Override
  public byte[] encode(List<?> data) {
    if (data == null || data.isEmpty()) {
      return new byte[0];
    }
    PublicBAOS outputStream = new PublicBAOS();
    try {
      switch (dataType) {
        case INT32:
        case DATE:
        case BOOLEAN:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((Integer) value, outputStream);
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((long) value, outputStream);
            }
          }
          break;
        case FLOAT:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((float) value, outputStream);
            }
          }
          break;
        case DOUBLE:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((double) value, outputStream);
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("RLE doesn't support data type: " + dataType);
      }
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      columnEntry.setUnCompressedSize(getUnCompressedSize(data));
      columnEntry.setCompressedSize(encodedData.length);
      columnEntry.setEncodingType(TSEncoding.RLE);
      columnEntry.setDataType(dataType);
      columnEntry.updateSize();
      return encodedData;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        outputStream.close();
      } catch (IOException e) {
      }
    }
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public TSEncoding getEncodingType() {
    return TSEncoding.RLE;
  }

  @Override
  public Encoder getEncoder(TSDataType type, TSEncoding encodingType) {
    return TSEncodingBuilder.getEncodingBuilder(encodingType).getEncoder(type);
  }

  @Override
  public ColumnEntry getColumnEntry() {
    return columnEntry;
  }

  /**
   * The logic in this part is universal and is used to calculate the original data size. You can
   * check whether there is such a method elsewhere.
   *
   * @param data
   * @return
   */
  public Integer getUnCompressedSize(List<?> data) {
    int unCompressedSize = 0;
    for (Object value : data) {
      if (value != null) {
        switch (dataType) {
          case BOOLEAN:
            unCompressedSize += 1; // boolean 占用 1 字节
            break;
          case INT32:
          case DATE:
            unCompressedSize += 4; // int32 占用 4 字节
            break;
          case INT64:
          case TIMESTAMP:
            unCompressedSize += 8; // int64 占用 8 字节
            break;
          case FLOAT:
            unCompressedSize += 4; // float 占用 4 字节
            break;
          case DOUBLE:
            unCompressedSize += 8; // double 占用 8 字节
            break;
          case TEXT:
          case STRING:
          case BLOB:
            if (value instanceof String) {
              unCompressedSize += ((String) value).getBytes().length;
            } else if (value instanceof Binary) {
              unCompressedSize += ((Binary) value).getLength();
            }
            break;
        }
      }
    }
    return unCompressedSize;
  }
}
