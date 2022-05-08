/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
// package org.apache.iotdb.tsfile.file.header;
//
// import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
// import org.apache.iotdb.tsfile.read.reader.TsFileInput;
// import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
// import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
//
// import java.io.IOException;
// import java.nio.ByteBuffer;
//
// public class ChunkHeader {
//
//  /**
//   * deserialize from TsFileInput, the marker has not been read.
//   *
//   * @param input TsFileInput
//   * @param offset offset
//   * @param chunkHeaderSize the estimated size of chunk's header
//   * @return CHUNK_HEADER object
//   * @throws IOException IOException
//   */
//  public static ChunkHeader deserializeFrom(TsFileInput input, long offset, int chunkHeaderSize)
//      throws IOException {
//
//    // read chunk header from input to buffer
//    ByteBuffer buffer = ByteBuffer.allocate(chunkHeaderSize);
//    input.read(buffer, offset);
//    buffer.flip();
//
//    byte chunkType = buffer.get();
//    // read measurementID
//    String measurementID = ReadWriteIOUtils.readVarIntString(buffer);
//    int dataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
//    TSDataType dataType = ReadWriteIOUtils.readDataType(buffer);
//    CompressionType type = ReadWriteIOUtils.readCompressionType(buffer);
//    TSEncoding encoding = ReadWriteIOUtils.readEncoding(buffer);
//    chunkHeaderSize =
//        chunkHeaderSize - Integer.BYTES - 1 + ReadWriteForEncodingUtils.uVarIntSize(dataSize);
//    return new ChunkHeader(
//        chunkType, measurementID, dataSize, chunkHeaderSize, dataType, type, encoding);
//  }
//
//  /**
//   * serialize to ByteBuffer.
//   *
//   * @param buffer ByteBuffer
//   * @return length
//   */
//  public int serializeTo(ByteBuffer buffer) {
//    int length = 0;
//    length += ReadWriteIOUtils.write(chunkType, buffer);
//    length += ReadWriteIOUtils.writeVar(measurementID, buffer);
//    length += ReadWriteForEncodingUtils.writeUnsignedVarInt(dataSize, buffer);
//    length += ReadWriteIOUtils.write(dataType, buffer);
//    length += ReadWriteIOUtils.write(compressionType, buffer);
//    length += ReadWriteIOUtils.write(encodingType, buffer);
//    return length;
//  }
//
//  @Override
//  public String toString() {
//    return "CHUNK_HEADER{"
//        + "measurementID='"
//        + measurementID
//        + '\''
//        + ", dataSize="
//        + dataSize
//        + ", dataType="
//        + dataType
//        + ", compressionType="
//        + compressionType
//        + ", encodingType="
//        + encodingType
//        + ", numOfPages="
//        + numOfPages
//        + ", serializedSize="
//        + serializedSize
//        + '}';
//  }
// }
