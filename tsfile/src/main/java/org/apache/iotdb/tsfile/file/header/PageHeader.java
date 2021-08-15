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

package org.apache.iotdb.tsfile.file.header;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class PageHeader {

  private int uncompressedSize;
  private int compressedSize;
  private Statistics statistics;
  private boolean modified;

  public PageHeader(int uncompressedSize, int compressedSize, Statistics statistics) {
    this.uncompressedSize = uncompressedSize;
    this.compressedSize = compressedSize;
    this.statistics = statistics;
  }

  /** max page header size without statistics */
  public static int estimateMaxPageHeaderSizeWithoutStatistics() {
    // uncompressedSize, compressedSize
    // because we use unsigned varInt to encode these two integer,
    // each unsigned arInt will cost at most 5 bytes
    return 2 * (Integer.BYTES + 1);
  }

  public static PageHeader deserializeFrom(
      InputStream inputStream, TSDataType dataType, boolean hasStatistic) throws IOException {
    int uncompressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream);
    int compressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream);
    Statistics statistics = null;
    if (hasStatistic) {
      statistics = Statistics.deserialize(inputStream, dataType);
    }
    return new PageHeader(uncompressedSize, compressedSize, statistics);
  }

  public static PageHeader deserializeFrom(ByteBuffer buffer, TSDataType dataType) {
    int uncompressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    int compressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    Statistics statistics = Statistics.deserialize(buffer, dataType);
    return new PageHeader(uncompressedSize, compressedSize, statistics);
  }

  public static PageHeader deserializeFrom(ByteBuffer buffer, Statistics chunkStatistic) {
    int uncompressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    int compressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    return new PageHeader(uncompressedSize, compressedSize, chunkStatistic);
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }

  public void setUncompressedSize(int uncompressedSize) {
    this.uncompressedSize = uncompressedSize;
  }

  public int getCompressedSize() {
    return compressedSize;
  }

  public void setCompressedSize(int compressedSize) {
    this.compressedSize = compressedSize;
  }

  public long getNumOfValues() {
    return statistics.getCount();
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public long getEndTime() {
    return statistics.getEndTime();
  }

  public long getStartTime() {
    return statistics.getStartTime();
  }

  public void serializeTo(OutputStream outputStream) throws IOException {
    ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, outputStream);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, outputStream);
    statistics.serialize(outputStream);
  }

  @Override
  public String toString() {
    return "PageHeader{"
        + "uncompressedSize="
        + uncompressedSize
        + ", compressedSize="
        + compressedSize
        + ", statistics="
        + statistics
        + "}";
  }

  public boolean isModified() {
    return modified;
  }

  public void setModified(boolean modified) {
    this.modified = modified;
  }

  /** max page header size without statistics */
  public int getSerializedPageSize() {
    return ReadWriteForEncodingUtils.uVarIntSize(uncompressedSize)
        + ReadWriteForEncodingUtils.uVarIntSize(compressedSize)
        + (statistics == null ? 0 : statistics.getSerializedSize()) // page header
        + compressedSize; // page data
  }
}
