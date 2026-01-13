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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.protocol.thrift.handler.RPCServiceThriftHandlerMetrics;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TabletDecoder {

  private final CompressionType compressionType;
  private final TSDataType[] dataTypes;
  private final List<TSEncoding> columnEncodings;
  private final IUnCompressor unCompressor;
  private final int rowSize;

  /**
   * @param compressionType the overall compression
   * @param dataTypes data types of each column
   * @param columnEncodings encoding method of each column, the first column is the time column
   * @param rowSize number of rows
   */
  public TabletDecoder(
      CompressionType compressionType,
      TSDataType[] dataTypes,
      List<TSEncoding> columnEncodings,
      int rowSize) {
    this.compressionType = compressionType;
    this.dataTypes = dataTypes;
    this.columnEncodings = columnEncodings;
    this.unCompressor = IUnCompressor.getUnCompressor(compressionType);
    this.rowSize = rowSize;
  }

  public long[] decodeTime(ByteBuffer buffer) {
    int compressedSize = buffer.remaining();

    ByteBuffer uncompressedBuffer = uncompress(buffer);
    RPCServiceThriftHandlerMetrics.getInstance()
        .recordUnCompressionSizeTimer(uncompressedBuffer.remaining());
    RPCServiceThriftHandlerMetrics.getInstance().recordCompressionSizeTimer(compressedSize);

    long memoryUsage =
        compressionType == CompressionType.UNCOMPRESSED
            ? uncompressedBuffer.remaining()
            : uncompressedBuffer.remaining() + compressedSize;
    RPCServiceThriftHandlerMetrics.getInstance().recordMemoryUsage(memoryUsage);

    long startDecodeTime = System.nanoTime();
    long[] timeStamps = decodeColumnForTimeStamp(uncompressedBuffer, rowSize);
    RPCServiceThriftHandlerMetrics.getInstance()
        .recordDecodeLatencyTimer((System.nanoTime() - startDecodeTime));
    return timeStamps;
  }

  private ByteBuffer uncompress(ByteBuffer compressedBuffer) {
    long startUncompressTime = System.nanoTime();
    if (compressionType == CompressionType.UNCOMPRESSED) {
      return compressedBuffer;
    }
    try {
      int uncompressedLength = compressedBuffer.getInt(compressedBuffer.limit() - 4);
      byte[] uncompressedBytes = new byte[uncompressedLength];
      unCompressor.uncompress(
          compressedBuffer.array(),
          compressedBuffer.arrayOffset() + compressedBuffer.position(),
          compressedBuffer.remaining() - 4,
          uncompressedBytes,
          0);
      ByteBuffer output = ByteBuffer.wrap(uncompressedBytes);
      RPCServiceThriftHandlerMetrics.getInstance()
          .recordDecompressLatencyTimer(System.nanoTime() - startUncompressTime);
      return output;
    } catch (IOException e) {
      throw new IoTDBRuntimeException(
          "Failed to decompress compressedBuffer",
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private long[] decodeColumnForTimeStamp(ByteBuffer buffer, int rowCount) {
    TSDataType dataType = TSDataType.INT64;
    TSEncoding encodingType = columnEncodings.get(0);

    Decoder decoder = Decoder.getDecoderByType(encodingType, dataType);
    long[] result = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readLong(buffer);
    }
    return result;
  }

  public Pair<Object[], ByteBuffer> decodeValues(ByteBuffer buffer) {
    int compressedSize = buffer.remaining();
    ByteBuffer uncompressed = uncompress(buffer);
    RPCServiceThriftHandlerMetrics.getInstance()
        .recordUnCompressionSizeTimer(uncompressed.remaining());
    RPCServiceThriftHandlerMetrics.getInstance().recordCompressionSizeTimer(compressedSize);

    long startDecodeTime = System.nanoTime();
    Object[] columns = new Object[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      columns[i] = decodeColumn(uncompressed, i);
    }

    RPCServiceThriftHandlerMetrics.getInstance()
        .recordDecodeLatencyTimer(System.nanoTime() - startDecodeTime);
    return new Pair<>(columns, uncompressed);
  }

  private Object decodeColumn(ByteBuffer uncompressed, int columnIndex) {
    TSDataType dataType = dataTypes[columnIndex];
    TSEncoding encoding = columnEncodings.get(columnIndex + 1);

    Decoder decoder = Decoder.getDecoderByType(encoding, dataType);
    Object column = null;
    switch (dataType) {
      case DATE:
      case INT32:
        int[] intCol = new int[rowSize];
        if (encoding == TSEncoding.PLAIN) {
          // PlainEncoder uses var int, which may cause compatibility problem
          for (int j = 0; j < rowSize; j++) {
            intCol[j] = ReadWriteIOUtils.readInt(uncompressed);
          }
        } else {
          for (int j = 0; j < rowSize; j++) {
            intCol[j] = decoder.readInt(uncompressed);
          }
        }
        column = intCol;
        break;
      case INT64:
      case TIMESTAMP:
        long[] longCol = new long[rowSize];
        for (int j = 0; j < rowSize; j++) {
          longCol[j] = decoder.readLong(uncompressed);
        }
        column = longCol;
        break;
      case FLOAT:
        float[] floatCol = new float[rowSize];
        for (int j = 0; j < rowSize; j++) {
          floatCol[j] = decoder.readFloat(uncompressed);
        }
        column = floatCol;
        break;
      case DOUBLE:
        double[] doubleCol = new double[rowSize];
        for (int j = 0; j < rowSize; j++) {
          doubleCol[j] = decoder.readDouble(uncompressed);
        }
        column = doubleCol;
        break;
      case BOOLEAN:
        boolean[] boolCol = new boolean[rowSize];
        for (int j = 0; j < rowSize; j++) {
          boolCol[j] = decoder.readBoolean(uncompressed);
        }
        column = boolCol;
        break;
      case STRING:
      case BLOB:
      case TEXT:
        Binary[] binaryCol = new Binary[rowSize];
        if (encoding == TSEncoding.PLAIN) {
          // PlainEncoder uses var int, which may cause compatibility problem
          for (int j = 0; j < rowSize; j++) {
            binaryCol[j] = ReadWriteIOUtils.readBinary(uncompressed);
          }
        } else {
          for (int j = 0; j < rowSize; j++) {
            binaryCol[j] = decoder.readBinary(uncompressed);
          }
        }
        column = binaryCol;
        break;
      case UNKNOWN:
      case VECTOR:
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
    return column;
  }
}
