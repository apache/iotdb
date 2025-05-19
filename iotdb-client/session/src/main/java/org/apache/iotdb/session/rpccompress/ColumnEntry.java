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
package org.apache.iotdb.session.rpccompress;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ColumnEntry implements Serializable {

  private Integer compressedSize;
  private Integer unCompressedSize;
  private TSDataType dataType;
  private TSEncoding encodingType;

  /** The number of bytes occupied by ColumnEntry */
  private Integer size;

  public ColumnEntry() {
    updateSize();
  }

  public ColumnEntry(
      Integer compressedSize,
      Integer unCompressedSize,
      TSDataType dataType,
      TSEncoding encodingType) {
    this.compressedSize = compressedSize;
    this.unCompressedSize = unCompressedSize;
    this.dataType = dataType;
    this.encodingType = encodingType;
    updateSize();
  }

  /**
   * Update the total size of the column entry Total size = compressedSize (4 bytes) +
   * unCompressedSize (4 bytes) + dataType (1 byte) + encodingType (1 byte) + offset (4 bytes)
   */
  public void updateSize() {
    int totalSize = 0;
    totalSize += 4;
    totalSize += 4;
    totalSize += 1;
    totalSize += 1;
    this.size = totalSize;
  }

  public Integer getCompressedSize() {
    return compressedSize;
  }

  public Integer getUnCompressedSize() {
    return unCompressedSize;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public Integer getSize() {
    return size;
  }

  public void setCompressedSize(Integer compressedSize) {
    this.compressedSize = compressedSize;
  }

  public void setUnCompressedSize(Integer unCompressedSize) {
    this.unCompressedSize = unCompressedSize;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public void setEncodingType(TSEncoding encodingType) {
    this.encodingType = encodingType;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  @Override
  public String toString() {
    return "ColumnEntry{"
        + "compressedSize="
        + compressedSize
        + ", unCompressedSize="
        + unCompressedSize
        + ", dataType="
        + dataType
        + ", encodingType="
        + encodingType
        + ", size="
        + size
        + '}';
  }

  public byte[] toBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(getSize());
    buffer.putInt(compressedSize != null ? compressedSize : 0);
    buffer.putInt(unCompressedSize != null ? unCompressedSize : 0);
    buffer.put((byte) (dataType != null ? dataType.ordinal() : 0));
    buffer.put((byte) (encodingType != null ? encodingType.ordinal() : 0));
    return buffer.array();
  }

  public static ColumnEntry fromBytes(ByteBuffer buffer) {
    int compressedSize = buffer.getInt();
    int unCompressedSize = buffer.getInt();
    TSDataType dataType = TSDataType.values()[buffer.get()];
    TSEncoding encodingType = TSEncoding.values()[buffer.get()];
    ColumnEntry entry = new ColumnEntry();
    entry.setCompressedSize(compressedSize);
    entry.setUnCompressedSize(unCompressedSize);
    entry.setDataType(dataType);
    entry.setEncodingType(encodingType);
    entry.updateSize();
    return entry;
  }
}
