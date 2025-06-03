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
package org.apache.iotdb.session.rpccompress.encoder;

import org.apache.iotdb.session.rpccompress.ColumnEntry;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PlainColumnEncoder implements ColumnEncoder {
  /** The encoder used to encode the data */
  private final Encoder encoder;

  private final TSDataType dataType;

  /** ColumnEntry stores metadata for a column's encoded data */
  private ColumnEntry columnEntry;

  private static final int DEFAULT_MAX_STRING_LENGTH = 0xffff;

  public PlainColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = new PlainEncoder(dataType, DEFAULT_MAX_STRING_LENGTH);
  }

  /**
   * Encodes a column of data using the PLAIN encoding algorithm.
   *
   * @param values values the input boolean array to be encoded
   * @param out out the output stream to write the encoded binary data
   */
  @Override
  public void encode(boolean[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length, null, dataType);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (boolean value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(int[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length, null, dataType);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (int value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(long[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length, null, dataType);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (long value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(float[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length, null, dataType);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (float value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(double[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length, null, dataType);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (double value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(Binary[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length, values, dataType);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (Binary value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Set column entry metadata
   *
   * @param compressedSize the size of the encoded data in bytes after compression
   * @param unCompressedSize the original size of the data in bytes before compression
   */
  private void setColumnEntry(Integer compressedSize, Integer unCompressedSize) {
    columnEntry = new ColumnEntry(compressedSize, unCompressedSize, dataType, TSEncoding.PLAIN);
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public TSEncoding getEncodingType() {
    return TSEncoding.PLAIN;
  }

  @Override
  public ColumnEntry getColumnEntry() {
    return columnEntry;
  }
}
