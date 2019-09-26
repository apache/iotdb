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
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.exception.compress.CompressionTypeNotSupportedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * uncompress data according to type in metadata.
 */
public interface IUnCompressor {

  /**
   * get the UnCompressor based on the CompressionType.
   *
   * @param name CompressionType
   * @return the UnCompressor of specified CompressionType
   */
  static IUnCompressor getUnCompressor(CompressionType name) {
    if (name == null) {
      throw new CompressionTypeNotSupportedException("NULL");
    }
    switch (name) {
      case UNCOMPRESSED:
        return new NoUnCompressor();
      case SNAPPY:
        return new SnappyUnCompressor();
      default:
        throw new CompressionTypeNotSupportedException(name.toString());
    }
  }

  int getUncompressedLength(byte[] array, int offset, int length)
      throws IOException;

  /**
   * get the uncompressed length.
   *
   * @param buffer MUST be DirectByteBuffer
   */
  int getUncompressedLength(ByteBuffer buffer) throws IOException;

  /**
   * uncompress the byte array.
   *
   * @param byteArray to be uncompressed bytes
   * @return bytes after uncompressed
   */
  public abstract byte[] uncompress(byte[] byteArray);

  /**
   * uncompress the byte array.
   *
   * @param byteArray -to be uncompressed bytes
   * @param offset -offset
   * @param length -length
   * @param output -output byte
   * @param outOffset -
   * @return the valid length of the output array
   */
  int uncompress(byte[] byteArray, int offset, int length, byte[] output,
      int outOffset)
      throws IOException;

  /**
   * if the data is large, using this function is better.
   *
   * @param compressed MUST be DirectByteBuffer
   * @param uncompressed MUST be DirectByteBuffer
   */
  int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException;

  CompressionType getCodecName();

  class NoUnCompressor implements IUnCompressor {

    @Override
    public int getUncompressedLength(byte[] array, int offset, int length) {
      return length;
    }

    @Override
    public int getUncompressedLength(ByteBuffer buffer) {
      return buffer.remaining();
    }

    @Override
    public byte[] uncompress(byte[] byteArray) {
      return byteArray;
    }

    @Override
    public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
        throws IOException {
      throw new IOException("NoUnCompressor does not support this method.");
    }

    @Override
    public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
      throw new IOException("NoUnCompressor does not support this method.");
    }

    @Override
    public CompressionType getCodecName() {
      return CompressionType.UNCOMPRESSED;
    }
  }

  class SnappyUnCompressor implements IUnCompressor {

    private static final Logger logger = LoggerFactory.getLogger(SnappyUnCompressor.class);

    @Override
    public int getUncompressedLength(byte[] array, int offset, int length) throws IOException {
      return Snappy.uncompressedLength(array, offset, length);
    }

    @Override
    public int getUncompressedLength(ByteBuffer buffer) throws IOException {
      return Snappy.uncompressedLength(buffer);
    }

    @Override
    public byte[] uncompress(byte[] bytes) {
      if (bytes == null) {
        return new byte[0];
      }

      try {
        return Snappy.uncompress(bytes);
      } catch (IOException e) {
        logger.error(
            "tsfile-compression SnappyUnCompressor: errors occurs when uncompress input byte, "
                + "bytes is {}",
            bytes, e);
      }
      return new byte[0];
    }

    @Override
    public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
        throws IOException {
      Snappy.uncompressedLength(byteArray, offset, length);
      return Snappy.uncompress(byteArray, offset, length, output, outOffset);
    }

    @Override
    public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) {
      if (compressed == null || !compressed.hasRemaining()) {
        return 0;
      }

      try {
        return Snappy.uncompress(compressed, uncompressed);
      } catch (IOException e) {
        logger.error(
            "tsfile-compression SnappyUnCompressor: errors occurs when uncompress input byte, "
                + "bytes is {}",
            compressed.array(), e);
      }
      return 0;
    }

    @Override
    public CompressionType getCodecName() {
      return CompressionType.SNAPPY;
    }
  }
}
