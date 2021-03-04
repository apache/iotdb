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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ChunkGroupHeader {

  private static final byte MARKER = MetaMarker.CHUNK_GROUP_HEADER;

  private final String deviceID;

  // this field does not need to be serialized.
  private int serializedSize;

  /**
   * constructor of CHUNK_GROUP_HEADER.
   *
   * @param deviceID device ID
   */
  public ChunkGroupHeader(String deviceID) {
    this.deviceID = deviceID;
    this.serializedSize = getSerializedSize(deviceID);
  }

  private int getSerializedSize(String deviceID) {
    int length = deviceID.getBytes(TSFileConfig.STRING_CHARSET).length;
    return Byte.BYTES + ReadWriteForEncodingUtils.varIntSize(length) + length;
  }

  /**
   * deserialize from inputStream.
   *
   * @param markerRead Whether the marker of the CHUNK_GROUP_HEADER is read ahead.
   */
  public static ChunkGroupHeader deserializeFrom(InputStream inputStream, boolean markerRead)
      throws IOException {
    if (!markerRead) {
      byte marker = (byte) inputStream.read();
      if (marker != MARKER) {
        MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    String deviceID = ReadWriteIOUtils.readVarIntString(inputStream);
    return new ChunkGroupHeader(deviceID);
  }

  /**
   * deserialize from TsFileInput.
   *
   * @param markerRead Whether the marker of the CHUNK_GROUP_HEADER is read ahead.
   */
  public static ChunkGroupHeader deserializeFrom(TsFileInput input, long offset, boolean markerRead)
      throws IOException {
    long offsetVar = offset;
    if (!markerRead) {
      offsetVar++;
    }
    String deviceID = input.readVarIntString(offsetVar);
    return new ChunkGroupHeader(deviceID);
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  public String getDeviceID() {
    return deviceID;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream output stream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int length = 0;
    length += ReadWriteIOUtils.write(MARKER, outputStream);
    length += ReadWriteIOUtils.writeVar(deviceID, outputStream);
    return length;
  }

  @Override
  public String toString() {
    return "ChunkGroupHeader{"
        + "deviceID='"
        + deviceID
        + '\''
        + ", serializedSize="
        + serializedSize
        + '}';
  }
}
