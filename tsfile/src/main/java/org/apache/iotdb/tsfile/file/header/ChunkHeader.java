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
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ChunkHeader {

  /**
   * 1 means this chunk has more than one page, so each page has its own page statistic 5 means this
   * chunk has only one page, and this page has no page statistic
   *
   * <p>if the 8th bit of this byte is 1 means this chunk is a time chunk of one vector if the 7th
   * bit of this byte is 1 means this chunk is a value chunk of one vector
   */
  private byte chunkType;

  private String measurementID;
  private int dataSize;
  private TSDataType dataType;
  private CompressionType compressionType;
  private TSEncoding encodingType;

  // the following fields do not need to be serialized.
  private int numOfPages;
  private int serializedSize;

  public ChunkHeader(
      String measurementID,
      int dataSize,
      TSDataType dataType,
      CompressionType compressionType,
      TSEncoding encoding,
      int numOfPages) {
    this(measurementID, dataSize, dataType, compressionType, encoding, numOfPages, 0);
  }

  public ChunkHeader(
      String measurementID,
      int dataSize,
      TSDataType dataType,
      CompressionType compressionType,
      TSEncoding encoding,
      int numOfPages,
      int mask) {
    this(
        (byte)
            ((numOfPages <= 1 ? MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER : MetaMarker.CHUNK_HEADER)
                | (byte) mask),
        measurementID,
        dataSize,
        getSerializedSize(measurementID, dataSize),
        dataType,
        compressionType,
        encoding);
    this.numOfPages = numOfPages;
  }

  public ChunkHeader(
      byte chunkType,
      String measurementID,
      int dataSize,
      TSDataType dataType,
      CompressionType compressionType,
      TSEncoding encoding) {
    this(
        chunkType,
        measurementID,
        dataSize,
        getSerializedSize(measurementID, dataSize),
        dataType,
        compressionType,
        encoding);
  }

  public ChunkHeader(
      byte chunkType,
      String measurementID,
      int dataSize,
      int headerSize,
      TSDataType dataType,
      CompressionType compressionType,
      TSEncoding encoding) {
    this.chunkType = chunkType;
    this.measurementID = measurementID;
    this.dataSize = dataSize;
    this.dataType = dataType;
    this.compressionType = compressionType;
    this.encodingType = encoding;
    this.serializedSize = headerSize;
  }

  /** the exact serialized size of chunk header */
  public static int getSerializedSize(String measurementID, int dataSize) {
    int measurementIdLength = measurementID.getBytes(TSFileConfig.STRING_CHARSET).length;
    return Byte.BYTES // chunkType
        + ReadWriteForEncodingUtils.varIntSize(measurementIdLength) // measurementID length
        + measurementIdLength // measurementID
        + ReadWriteForEncodingUtils.uVarIntSize(dataSize) // dataSize
        + TSDataType.getSerializedSize() // dataType
        + CompressionType.getSerializedSize() // compressionType
        + TSEncoding.getSerializedSize(); // encodingType
  }

  /**
   * The estimated serialized size of chunk header. Only used when we don't know the actual dataSize
   * attribute
   */
  public static int getSerializedSize(String measurementID) {

    int measurementIdLength = measurementID.getBytes(TSFileConfig.STRING_CHARSET).length;
    return Byte.BYTES // chunkType
        + ReadWriteForEncodingUtils.varIntSize(measurementIdLength) // measurementID length
        + measurementIdLength // measurementID
        + Integer.BYTES
        + 1 // uVarInt dataSize
        + TSDataType.getSerializedSize() // dataType
        + CompressionType.getSerializedSize() // compressionType
        + TSEncoding.getSerializedSize(); // encodingType
  }

  /** deserialize from inputStream, the marker has already been read. */
  public static ChunkHeader deserializeFrom(InputStream inputStream, byte chunkType)
      throws IOException {
    // read measurementID
    String measurementID = ReadWriteIOUtils.readVarIntString(inputStream);
    int dataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream);
    TSDataType dataType = ReadWriteIOUtils.readDataType(inputStream);
    CompressionType type = ReadWriteIOUtils.readCompressionType(inputStream);
    TSEncoding encoding = ReadWriteIOUtils.readEncoding(inputStream);
    return new ChunkHeader(chunkType, measurementID, dataSize, dataType, type, encoding);
  }

  /**
   * deserialize from TsFileInput, the marker has not been read.
   *
   * @param input TsFileInput
   * @param offset offset
   * @param chunkHeaderSize the estimated size of chunk's header
   * @return CHUNK_HEADER object
   * @throws IOException IOException
   */
  public static ChunkHeader deserializeFrom(TsFileInput input, long offset, int chunkHeaderSize)
      throws IOException {

    // read chunk header from input to buffer
    ByteBuffer buffer = ByteBuffer.allocate(chunkHeaderSize);
    input.read(buffer, offset);
    buffer.flip();

    byte chunkType = buffer.get();
    // read measurementID
    String measurementID = ReadWriteIOUtils.readVarIntString(buffer);
    int dataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    TSDataType dataType = ReadWriteIOUtils.readDataType(buffer);
    CompressionType type = ReadWriteIOUtils.readCompressionType(buffer);
    TSEncoding encoding = ReadWriteIOUtils.readEncoding(buffer);
    chunkHeaderSize =
        chunkHeaderSize - Integer.BYTES - 1 + ReadWriteForEncodingUtils.uVarIntSize(dataSize);
    return new ChunkHeader(
        chunkType, measurementID, dataSize, chunkHeaderSize, dataType, type, encoding);
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
    length += ReadWriteIOUtils.write(chunkType, outputStream);
    length += ReadWriteIOUtils.writeVar(measurementID, outputStream);
    length += ReadWriteForEncodingUtils.writeUnsignedVarInt(dataSize, outputStream);
    length += ReadWriteIOUtils.write(dataType, outputStream);
    length += ReadWriteIOUtils.write(compressionType, outputStream);
    length += ReadWriteIOUtils.write(encodingType, outputStream);
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
    length += ReadWriteIOUtils.write(chunkType, buffer);
    length += ReadWriteIOUtils.writeVar(measurementID, buffer);
    length += ReadWriteForEncodingUtils.writeUnsignedVarInt(dataSize, buffer);
    length += ReadWriteIOUtils.write(dataType, buffer);
    length += ReadWriteIOUtils.write(compressionType, buffer);
    length += ReadWriteIOUtils.write(encodingType, buffer);
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

  @Override
  public String toString() {
    return "CHUNK_HEADER{"
        + "measurementID='"
        + measurementID
        + '\''
        + ", dataSize="
        + dataSize
        + ", dataType="
        + dataType
        + ", compressionType="
        + compressionType
        + ", encodingType="
        + encodingType
        + ", numOfPages="
        + numOfPages
        + ", serializedSize="
        + serializedSize
        + '}';
  }

  public void mergeChunkHeader(ChunkHeader chunkHeader) {
    this.dataSize += chunkHeader.getDataSize();
    this.numOfPages += chunkHeader.getNumOfPages();
  }

  public void setDataSize(int dataSize) {
    this.dataSize = dataSize;
  }

  public byte getChunkType() {
    return chunkType;
  }

  public void setChunkType(byte chunkType) {
    this.chunkType = chunkType;
  }

  public void increasePageNums(int i) {
    numOfPages += i;
  }
}
