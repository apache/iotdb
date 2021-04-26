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
package org.apache.iotdb.tsfile.v2.file.header;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ChunkHeaderV2 {

  private ChunkHeaderV2() {}

  /**
   * deserialize from inputStream.
   *
   * @param markerRead Whether the marker of the CHUNK_HEADER has been read
   */
  public static ChunkHeader deserializeFrom(InputStream inputStream, boolean markerRead)
      throws IOException {
    if (!markerRead) {
      byte marker = (byte) inputStream.read();
      if (marker != MetaMarker.CHUNK_HEADER) {
        MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    String measurementID = ReadWriteIOUtils.readString(inputStream);
    int dataSize = ReadWriteIOUtils.readInt(inputStream);
    TSDataType dataType = TSDataType.deserialize((byte) ReadWriteIOUtils.readShort(inputStream));
    int numOfPages = ReadWriteIOUtils.readInt(inputStream);
    CompressionType type =
        CompressionType.deserialize((byte) ReadWriteIOUtils.readShort(inputStream));
    TSEncoding encoding = TSEncoding.deserialize((byte) ReadWriteIOUtils.readShort(inputStream));
    return new ChunkHeader(measurementID, dataSize, dataType, type, encoding, numOfPages);
  }

  /**
   * deserialize from TsFileInput.
   *
   * @param input TsFileInput
   * @param offset offset
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead read marker (boolean type)
   * @return CHUNK_HEADER object
   * @throws IOException IOException
   */
  public static ChunkHeader deserializeFrom(
      TsFileInput input, long offset, int chunkHeaderSize, boolean markerRead) throws IOException {
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
    TSDataType dataType = TSDataType.deserialize((byte) ReadWriteIOUtils.readShort(buffer));
    // numOfPages
    ReadWriteIOUtils.readInt(buffer);
    CompressionType type = CompressionType.deserialize((byte) ReadWriteIOUtils.readShort(buffer));
    TSEncoding encoding = TSEncoding.deserialize((byte) ReadWriteIOUtils.readShort(buffer));
    return new ChunkHeader(
        MetaMarker.CHUNK_HEADER,
        measurementID,
        dataSize,
        chunkHeaderSize,
        dataType,
        type,
        encoding);
  }

  public static int getSerializedSize(String measurementID) {
    return Byte.BYTES // marker
        + Integer.BYTES // measurementID length
        + measurementID.getBytes(TSFileConfig.STRING_CHARSET).length // measurementID
        + Integer.BYTES // dataSize
        + Short.BYTES // dataType
        + Short.BYTES // compressionType
        + Short.BYTES // encodingType
        + Integer.BYTES; // numOfPages
  }
}
