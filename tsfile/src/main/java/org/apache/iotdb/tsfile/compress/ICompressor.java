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

package org.apache.iotdb.tsfile.compress;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.iotdb.tsfile.exception.compress.CompressionTypeNotSupportedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.xerial.snappy.Snappy;

/**
 * compress data according to type in schema.
 */
public interface ICompressor extends Serializable {

  static ICompressor getCompressor(String name) {
    return getCompressor(CompressionType.valueOf(name));
  }

  /**
   * get Compressor according to CompressionType.
   *
   * @param name CompressionType
   * @return the Compressor of specified CompressionType
   */
  static ICompressor getCompressor(CompressionType name) {
    if (name == null) {
      throw new CompressionTypeNotSupportedException("NULL");
    }
    switch (name) {
      case UNCOMPRESSED:
        return new NoCompressor();
      case SNAPPY:
        return new SnappyCompressor();
      case LZ4:
        return new IOTDBLZ4Compressor();
      default:
        throw new CompressionTypeNotSupportedException(name.toString());
    }
  }

  byte[] compress(byte[] data) throws IOException;

  /**
   * abstract method of compress.
   *
   * @return byte length of compressed data.
   */
  int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException;

  /**
   * If the data is large, this function is better than byte[].
   *
   * @param data       MUST be DirectByteBuffer for Snappy.
   * @param compressed MUST be DirectByteBuffer for Snappy.
   * @return byte length of compressed data.
   */
  int compress(ByteBuffer data, ByteBuffer compressed) throws IOException;

  int getMaxBytesForCompression(int uncompressedDataSize);

  CompressionType getType();

  /**
   * NoCompressor will do nothing for data and return the input data directly.
   */
  class NoCompressor implements ICompressor {

    @Override
    public byte[] compress(byte[] data) {
      return data;
    }

    @Override
    public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
      throw new IOException("No Compressor does not support compression function");
    }

    @Override
    public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
      throw new IOException("No Compressor does not support compression function");
    }

    @Override
    public int getMaxBytesForCompression(int uncompressedDataSize) {
      return uncompressedDataSize;
    }

    @Override
    public CompressionType getType() {
      return CompressionType.UNCOMPRESSED;
    }
  }

  class SnappyCompressor implements ICompressor {

    @Override
    public byte[] compress(byte[] data) throws IOException {
      if (data == null) {
        return new byte[0];
      }
      return Snappy.compress(data);
    }

    @Override
    public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
      return Snappy.compress(data, offset, length, compressed, 0);
    }

    @Override
    public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
      return Snappy.compress(data, compressed);
    }

    @Override
    public int getMaxBytesForCompression(int uncompressedDataSize) {
      return Snappy.maxCompressedLength(uncompressedDataSize);
    }

    @Override
    public CompressionType getType() {
      return CompressionType.SNAPPY;
    }
  }

  class IOTDBLZ4Compressor implements ICompressor {
    private LZ4Compressor compressor;

    public IOTDBLZ4Compressor(){
      super();
      LZ4Factory factory = LZ4Factory.fastestInstance();
      compressor = factory.fastCompressor();
    }

    @Override
    public byte[] compress(byte[] data) throws IOException {
      if (data == null) {
        return new byte[0];
      }
      return compressor.compress(data);
    }

    @Override
    public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
      return compressor.compress(data, offset, length, compressed, 0);
    }

    @Override
    public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
      compressor.compress(data, compressed);
      return data.limit();
    }

    @Override
    public int getMaxBytesForCompression(int uncompressedDataSize) {
      return compressor.maxCompressedLength(uncompressedDataSize);
    }

    @Override
    public CompressionType getType() {
      return CompressionType.LZ4;
    }
  }
}
