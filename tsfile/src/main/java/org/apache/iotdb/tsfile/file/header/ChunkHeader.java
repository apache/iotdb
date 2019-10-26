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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ChunkHeader {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkHeader.class);

  public static final byte MARKER = MetaMarker.CHUNK_HEADER;

  private String measurementID;
  private int dataSize;
  private TSDataType dataType;
  private CompressionType compressionType;
  private TSEncoding encodingType;
  private int numOfPages;
  /**
   * The maximum time of the tombstones that take effect on this chunk. Only data with larger
   * timestamps than this should be exposed to user.
   */
  private long maxTombstoneTime;

  // this field does not need to be serialized.
  private int serializedSize;

  public ChunkHeader(String measurementID, int dataSize, TSDataType dataType,
      CompressionType compressionType,
      TSEncoding encoding, int numOfPages) {
    this(measurementID, dataSize, getSerializedSize(measurementID), dataType, compressionType,
        encoding, numOfPages, 0);
  }

  public ChunkHeader(String measurementID, int dataSize, int headerSize, TSDataType dataType,
      CompressionType compressionType, TSEncoding encoding, int numOfPages) {
    this(measurementID, dataSize, headerSize, dataType, compressionType, encoding, numOfPages, 0);
  }

  private ChunkHeader(String measurementID, int dataSize, TSDataType dataType,
      CompressionType compressionType, TSEncoding encoding, int numOfPages, long maxTombstoneTime) {
    this(measurementID, dataSize, getSerializedSize(measurementID), dataType, compressionType,
        encoding, numOfPages, maxTombstoneTime);
  }

  private ChunkHeader(String measurementID, int dataSize, int headerSize, TSDataType dataType,
      CompressionType compressionType, TSEncoding encoding, int numOfPages, long maxTombstoneTime) {
    this.measurementID = measurementID;
    this.dataSize = dataSize;
    this.dataType = dataType;
    this.compressionType = compressionType;
    this.numOfPages = numOfPages;
    this.encodingType = encoding;
    this.serializedSize = headerSize;
    this.maxTombstoneTime = maxTombstoneTime;
  }

  public static int getSerializedSize(String measurementID) {
    return Byte.BYTES + Integer.BYTES + getSerializedSize(
        measurementID.getBytes(TSFileConfig.STRING_CHARSET).length);
  }

  private static int getSerializedSize(int measurementIdLength) {
    return measurementIdLength + Integer.BYTES + TSDataType.getSerializedSize() + Integer.BYTES
        + CompressionType.getSerializedSize() + TSEncoding.getSerializedSize() + Long.BYTES;
  }

  /**
   * deserialize from inputStream.
   *
   * @param markerRead Whether the marker of the CHUNK_HEADER has been read
   */
  public static ChunkHeader deserializeFrom(InputStream inputStream, boolean markerRead)
      throws IOException {
    if (!markerRead) {
      byte marker = (byte) inputStream.read();
      if (marker != MARKER) {
        MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    String measurementID = ReadWriteIOUtils.readString(inputStream);
    int dataSize = ReadWriteIOUtils.readInt(inputStream);
    TSDataType dataType = TSDataType.deserialize(ReadWriteIOUtils.readShort(inputStream));
    int numOfPages = ReadWriteIOUtils.readInt(inputStream);
    CompressionType type = ReadWriteIOUtils.readCompressionType(inputStream);
    TSEncoding encoding = ReadWriteIOUtils.readEncoding(inputStream);
    long maxTombstoneTime = ReadWriteIOUtils.readLong(inputStream);
    return new ChunkHeader(measurementID, dataSize, dataType, type, encoding, numOfPages,
        maxTombstoneTime);
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
  public static ChunkHeader deserializeFrom(TsFileInput input, long offset, int chunkHeaderSize,
      boolean markerRead)
      throws IOException {
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
    long maxTombstoneTime = ReadWriteIOUtils.readLong(buffer);
    return new ChunkHeader(measurementID, dataSize, chunkHeaderSize, dataType, type, encoding,
        numOfPages,
        maxTombstoneTime);
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  public String getMeasurementID() {
    return measurementID;
  }

  public int getDataSize() {
    return dataSize;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int length = 0;
    length += ReadWriteIOUtils.write(MetaMarker.CHUNK_HEADER, outputStream);
    length += ReadWriteIOUtils.write(measurementID, outputStream);
    length += ReadWriteIOUtils.write(dataSize, outputStream);
    length += ReadWriteIOUtils.write(dataType, outputStream);
    length += ReadWriteIOUtils.write(numOfPages, outputStream);
    length += ReadWriteIOUtils.write(compressionType, outputStream);
    length += ReadWriteIOUtils.write(encodingType, outputStream);
    length += ReadWriteIOUtils.write(maxTombstoneTime, outputStream);
    return length;
  }

  /**
   * serialize to ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return length
   */
  public int serializeTo(ByteBuffer buffer) {
    int length = 0;
    length += ReadWriteIOUtils.write(MetaMarker.CHUNK_HEADER, buffer);
    length += ReadWriteIOUtils.write(measurementID, buffer);
    length += ReadWriteIOUtils.write(dataSize, buffer);
    length += ReadWriteIOUtils.write(dataType, buffer);
    length += ReadWriteIOUtils.write(numOfPages, buffer);
    length += ReadWriteIOUtils.write(compressionType, buffer);
    length += ReadWriteIOUtils.write(encodingType, buffer);
    length += ReadWriteIOUtils.write(maxTombstoneTime, buffer);
    return length;
  }

  public int getNumOfPages() {
    return numOfPages;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public long getMaxTombstoneTime() {
    return maxTombstoneTime;
  }

  public void setMaxTombstoneTime(long maxTombstoneTime) {
    this.maxTombstoneTime = maxTombstoneTime;
  }

  @Override
  public String toString() {
    return "CHUNK_HEADER{" + "measurementID='" + measurementID + '\'' + ", dataSize=" + dataSize
        + ", dataType="
        + dataType + ", compressionType=" + compressionType + ", encodingType=" + encodingType
        + ", numOfPages="
        + numOfPages + ", serializedSize=" + serializedSize + '}';
  }
}
