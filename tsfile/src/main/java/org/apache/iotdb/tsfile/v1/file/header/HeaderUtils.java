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

package org.apache.iotdb.tsfile.v1.file.header;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.v1.file.metadata.statistics.OldStatistics;

public class HeaderUtils {
  
  public static PageHeader deserializeOldPageHeader(InputStream inputStream, 
      TSDataType dataType) throws IOException {
    int uncompressedSize = ReadWriteIOUtils.readInt(inputStream);
    int compressedSize = ReadWriteIOUtils.readInt(inputStream);
    int numOfValues = ReadWriteIOUtils.readInt(inputStream);
    long maxTimestamp = ReadWriteIOUtils.readLong(inputStream);
    long minTimestamp = ReadWriteIOUtils.readLong(inputStream);
    OldStatistics<?> oldstatistics = OldStatistics.deserialize(inputStream, dataType);
    Statistics<?> statistics = OldStatistics.upgradeOldStatistics(oldstatistics, dataType, 
        numOfValues, maxTimestamp, minTimestamp);
    return new PageHeader(uncompressedSize, compressedSize, statistics);
  }

  public static PageHeader deserializeOldPageHeader(ByteBuffer buffer, TSDataType dataType, 
      boolean isOldVersion) throws IOException {
    int uncompressedSize = ReadWriteIOUtils.readInt(buffer);
    int compressedSize = ReadWriteIOUtils.readInt(buffer);
    int numOfValues = ReadWriteIOUtils.readInt(buffer);
    long maxTimestamp = ReadWriteIOUtils.readLong(buffer);
    long minTimestamp = ReadWriteIOUtils.readLong(buffer);
    OldStatistics<?> oldstatistics = OldStatistics.deserialize(buffer, dataType);
    Statistics<?> statistics = OldStatistics.upgradeOldStatistics(oldstatistics, dataType, 
        numOfValues, maxTimestamp, minTimestamp);
    return new PageHeader(uncompressedSize, compressedSize, statistics);
  }

  /**
   * deserialize from inputStream.
   *
   * @param markerRead Whether the marker of the CHUNK_HEADER has been read
   */
  public static ChunkHeader deserializeOldChunkHeader(InputStream inputStream, boolean markerRead) 
      throws IOException {
    if (!markerRead) {
      byte marker = (byte) inputStream.read();
      if (marker != MetaMarker.CHUNK_HEADER) {
        MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    String measurementID = ReadWriteIOUtils.readString(inputStream);
    int dataSize = ReadWriteIOUtils.readInt(inputStream);
    TSDataType dataType = TSDataType.deserialize(ReadWriteIOUtils.readShort(inputStream));
    int numOfPages = ReadWriteIOUtils.readInt(inputStream);
    CompressionType type = ReadWriteIOUtils.readCompressionType(inputStream);
    TSEncoding encoding = ReadWriteIOUtils.readEncoding(inputStream);
    // read maxTombstoneTime from old TsFile, has been removed in newer versions of TsFile
    ReadWriteIOUtils.readLong(inputStream);
    ChunkHeader chunkHeader = new ChunkHeader(measurementID, dataSize, dataType, type, encoding,
        numOfPages);
    return chunkHeader;
  }

  /**
   * deserialize from TsFileInput.
   *
   * @param input           TsFileInput
   * @param offset          offset
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead      read marker (boolean type)
   * @return CHUNK_HEADER object
   * @throws IOException IOException
   */
  public static ChunkHeader deserializeOldChunkHeader(TsFileInput input, long offset,
      int chunkHeaderSize, boolean markerRead) throws IOException {
    long offsetVar = offset;
    if (!markerRead) {
      offsetVar++;
    }

    // read chunk header from input to buffer
    ByteBuffer buffer = ByteBuffer.allocate(chunkHeaderSize);
    input.read(buffer, offsetVar);
    buffer.flip();

    // read measurementID
    int size = buffer.getInt();
    String measurementID = ReadWriteIOUtils.readStringWithLength(buffer, size);
    int dataSize = ReadWriteIOUtils.readInt(buffer);
    TSDataType dataType = TSDataType.deserialize(ReadWriteIOUtils.readShort(buffer));
    int numOfPages = ReadWriteIOUtils.readInt(buffer);
    CompressionType type = ReadWriteIOUtils.readCompressionType(buffer);
    TSEncoding encoding = ReadWriteIOUtils.readEncoding(buffer);
    // read maxTombstoneTime from old TsFile, has been removed in newer versions of TsFile
    ReadWriteIOUtils.readLong(buffer);
    ChunkHeader chunkHeader = new ChunkHeader(measurementID, dataSize, dataType, type, encoding,
        numOfPages);
    return chunkHeader;
  }
}
