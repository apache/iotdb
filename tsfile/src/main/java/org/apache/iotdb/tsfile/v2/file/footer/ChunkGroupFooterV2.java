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

package org.apache.iotdb.tsfile.v2.file.footer;

import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ChunkGroupFooterV2 {

  private ChunkGroupFooterV2() {}

  /**
   * deserialize from inputStream.
   *
   * @param markerRead Whether the marker of the CHUNK_GROUP_FOOTER is read ahead.
   */
  public static ChunkGroupHeader deserializeFrom(InputStream inputStream, boolean markerRead)
      throws IOException {
    if (!markerRead) {
      byte marker = (byte) inputStream.read();
      if (marker != MetaMarker.CHUNK_GROUP_HEADER) {
        MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    String deviceID = ReadWriteIOUtils.readString(inputStream);
    // dataSize
    ReadWriteIOUtils.readLong(inputStream);
    // numOfChunks
    ReadWriteIOUtils.readInt(inputStream);
    return new ChunkGroupHeader(deviceID);
  }

  /**
   * deserialize from TsFileInput.
   *
   * @param markerRead Whether the marker of the CHUNK_GROUP_FOOTER is read ahead.
   */
  public static ChunkGroupHeader deserializeFrom(TsFileInput input, long offset, boolean markerRead)
      throws IOException {
    long offsetVar = offset;
    if (!markerRead) {
      offsetVar++;
    }
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    input.read(buffer, offsetVar);
    buffer.flip();
    int size = buffer.getInt();
    offsetVar += Integer.BYTES;
    buffer = ByteBuffer.allocate(getSerializedSize(size));
    ReadWriteIOUtils.readAsPossible(input, offsetVar, buffer);
    buffer.flip();
    String deviceID = ReadWriteIOUtils.readStringWithLength(buffer, size);
    // dataSize
    ReadWriteIOUtils.readLong(buffer);
    // numOfChunks
    ReadWriteIOUtils.readInt(buffer);
    return new ChunkGroupHeader(deviceID);
  }

  private static int getSerializedSize(int deviceIdLength) {
    return deviceIdLength + Long.BYTES + Integer.BYTES;
  }
}
