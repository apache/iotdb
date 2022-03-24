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

import org.apache.iotdb.tsfile.exception.compress.CompressionTypeNotSupportedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

/** uncompress data according to type in metadata. */
public interface IUnCompressor {

  Logger logger = LoggerFactory.getLogger(IUnCompressor.class);
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
      case LZ4:
        return new LZ4UnCompressor();
      case GZIP:
        return new GZIPUnCompressor();
      default:
        throw new CompressionTypeNotSupportedException(name.toString());
    }
  }

  int getUncompressedLength(byte[] array, int offset, int length) throws IOException;

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
  byte[] uncompress(byte[] byteArray) throws IOException;

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
  int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
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
      logger.error("Uncompress NONE start");
      logger.error("Uncompress NONE stop");
      return byteArray;
    }

    @Override
    public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset) {
      logger.error("Uncompress NONE start");
      System.arraycopy(byteArray, offset, output, outOffset, length);
      logger.error("Uncompress NONE stop");
      return length;
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
      logger.error("Uncompress SNAPPY start");
      if (bytes == null) {
        logger.error("Uncompress SNAPPY stop");
        return new byte[0];
      }

      try {
        byte[] r = Snappy.uncompress(bytes);
        logger.error("Uncompress SNAPPY stop");
        return r;
      } catch (IOException e) {
        logger.error(
            "tsfile-compression SnappyUnCompressor: errors occurs when uncompress input byte", e);
      }
      logger.error("Uncompress SNAPPY stop");
      return new byte[0];
    }

    @Override
    public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
        throws IOException {
      logger.error("Uncompress SNAPPY start");
      int r = Snappy.uncompress(byteArray, offset, length, output, outOffset);
      logger.error("Uncompress SNAPPY stop");
      return r;
    }

    @Override
    public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) {
      if (compressed == null || !compressed.hasRemaining()) {
        return 0;
      }

      try {
        logger.error("Uncompress SNAPPY start");
        int r = Snappy.uncompress(compressed, uncompressed);
        logger.error("Uncompress SNAPPY stop");
        return r;
      } catch (IOException e) {
        logger.error(
            "tsfile-compression SnappyUnCompressor: errors occurs when uncompress input byte", e);
      }
      return 0;
    }

    @Override
    public CompressionType getCodecName() {
      return CompressionType.SNAPPY;
    }
  }

  class LZ4UnCompressor implements IUnCompressor {

    private static final Logger logger = LoggerFactory.getLogger(LZ4Compressor.class);
    private static final String UNCOMPRESS_INPUT_ERROR =
        "tsfile-compression LZ4UnCompressor: errors occurs when uncompress input byte";

    private static final int MAX_COMPRESS_RATIO = 255;
    private LZ4SafeDecompressor decompressor;

    public LZ4UnCompressor() {
      LZ4Factory factory = LZ4Factory.fastestInstance();
      decompressor = factory.safeDecompressor();
    }

    @Override
    public int getUncompressedLength(byte[] array, int offset, int length) {
      throw new UnsupportedOperationException("unsupported get uncompress length");
    }

    @Override
    public int getUncompressedLength(ByteBuffer buffer) {
      throw new UnsupportedOperationException("unsupported get uncompress length");
    }

    /**
     * We don't recommend using this method because we have to allocate MAX_COMPRESS_RATIO *
     * compressedSize to ensure uncompress safety, you can use other method if you know the
     * uncompressed size
     */
    @Override
    public byte[] uncompress(byte[] bytes) throws IOException {
      logger.error("Uncompress LZ4 start");
      if (bytes == null) {
        logger.error("Uncompress LZ4 stop");
        return new byte[0];
      }

      try {
        byte[] r = decompressor.decompress(bytes, MAX_COMPRESS_RATIO * bytes.length);
        logger.error("Uncompress LZ4 stop");
        return r;
      } catch (RuntimeException e) {
        logger.error(UNCOMPRESS_INPUT_ERROR, e);
        throw new IOException(e);
      }
    }

    @Override
    public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
        throws IOException {
      try {
        logger.error("Uncompress LZ4 start");
        int r = decompressor.decompress(byteArray, offset, length, output, offset);
        logger.error("Uncompress LZ4 stop");
        return r;
      } catch (RuntimeException e) {
        logger.error(UNCOMPRESS_INPUT_ERROR, e);
        throw new IOException(e);
      }
    }

    @Override
    public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
      if (compressed == null || !compressed.hasRemaining()) {
        return 0;
      }

      try {
        logger.error("Uncompress LZ4 start");
        decompressor.decompress(compressed, uncompressed);
        logger.error("Uncompress LZ4 stop");
        return compressed.limit();
      } catch (RuntimeException e) {
        logger.error(UNCOMPRESS_INPUT_ERROR, e);
        throw new IOException(e);
      }
    }

    @Override
    public CompressionType getCodecName() {
      return CompressionType.LZ4;
    }
  }

  class GZIPUnCompressor implements IUnCompressor {

    @Override
    public int getUncompressedLength(byte[] array, int offset, int length) {
      throw new UnsupportedOperationException("unsupported get uncompress length");
    }

    @Override
    public int getUncompressedLength(ByteBuffer buffer) {
      throw new UnsupportedOperationException("unsupported get uncompress length");
    }

    @Override
    public byte[] uncompress(byte[] byteArray) throws IOException {
      if (null == byteArray) {
        return new byte[0];
      }
      logger.error("Uncompress GZIP start");
      byte[] r = ICompressor.GZIPCompress.uncompress(byteArray);
      logger.error("Uncompress GZIP stop");
      return r;
    }

    @Override
    public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
        throws IOException {
      logger.error("Uncompress GZIP start");
      byte[] dataBefore = new byte[length];
      System.arraycopy(byteArray, offset, dataBefore, 0, length);
      byte[] res = ICompressor.GZIPCompress.uncompress(dataBefore);
      System.arraycopy(res, 0, output, outOffset, res.length);
      logger.error("Uncompress GZIP stop");
      return res.length;
    }

    @Override
    public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
      logger.error("Uncompress GZIP start");
      int length = compressed.remaining();
      byte[] dataBefore = new byte[length];
      compressed.get(dataBefore, 0, length);

      byte[] res = ICompressor.GZIPCompress.uncompress(dataBefore);
      uncompressed.put(res);
      logger.error("Uncompress GZIP stop");
      return res.length;
    }

    @Override
    public CompressionType getCodecName() {
      return CompressionType.GZIP;
    }
  }
}
