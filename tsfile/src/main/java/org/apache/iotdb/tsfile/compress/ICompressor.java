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
import org.apache.iotdb.tsfile.exception.compress.GZIPCompressOverflowException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.iotdb.tsfile.file.metadata.enums.CompressionType.GZIP;
import static org.apache.iotdb.tsfile.file.metadata.enums.CompressionType.LZ4;
import static org.apache.iotdb.tsfile.file.metadata.enums.CompressionType.SNAPPY;

/** compress data according to type in schema. */
public interface ICompressor extends Serializable {

  Logger logger = LoggerFactory.getLogger(ICompressor.class);

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
      case GZIP:
        return new GZIPCompressor();
      default:
        throw new CompressionTypeNotSupportedException(name.toString());
    }
  }

  byte[] compress(byte[] data) throws IOException;

  /**
   * abstract method of compress. this method has an important overhead due to the fact that it
   * needs to allocate a byte array to compress into, and then needs to resize this buffer to the
   * actual compressed length.
   *
   * @return byte array of compressed data.
   */
  byte[] compress(byte[] data, int offset, int length) throws IOException;

  /**
   * abstract method of compress.
   *
   * @return byte length of compressed data.
   */
  int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException;

  /**
   * If the data is large, this function is better than byte[].
   *
   * @param data MUST be DirectByteBuffer for Snappy.
   * @param compressed MUST be DirectByteBuffer for Snappy.
   * @return byte length of compressed data.
   */
  int compress(ByteBuffer data, ByteBuffer compressed) throws IOException;

  /**
   * Get the maximum byte size needed for compressing data of the given byte size. For GZIP, this
   * method is insecure and may cause {@code GZIPCompressOverflowException}
   *
   * @param uncompressedDataSize byte size of the data to compress
   * @return maximum byte size of the compressed data
   */
  int getMaxBytesForCompression(int uncompressedDataSize);

  CompressionType getType();

  /** NoCompressor will do nothing for data and return the input data directly. */
  class NoCompressor implements ICompressor {

    @Override
    public byte[] compress(byte[] data) {
      logger.error("Compress NONE start");
      logger.error("Compress NONE stop");
      return data;
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
      throw new IOException("No Compressor does not support compression function");
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
      logger.error("Compress SNAPPY start");
      if (data == null) {
        return new byte[0];
      }
      byte[] r = Snappy.compress(data);
      logger.error("Compress SNAPPY stop");
      return r;
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
      logger.error("Compress SNAPPY start");
      byte[] maxCompressed = new byte[getMaxBytesForCompression(length)];
      int compressedSize = Snappy.compress(data, offset, length, maxCompressed, 0);
      byte[] compressed = null;
      if (compressedSize < maxCompressed.length) {
        compressed = new byte[compressedSize];
        System.arraycopy(maxCompressed, 0, compressed, 0, compressedSize);
      } else {
        compressed = maxCompressed;
      }
      logger.error("Compress SNAPPY stop");
      return compressed;
    }

    @Override
    public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
      logger.error("Compress SNAPPY start");
      int r = Snappy.compress(data, offset, length, compressed, 0);
      logger.error("Compress SNAPPY stop");
      return r;
    }

    @Override
    public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
      logger.error("Compress SNAPPY start");
      int r = Snappy.compress(data, compressed);
      logger.error("Compress SNAPPY stop");
      return r;
    }

    @Override
    public int getMaxBytesForCompression(int uncompressedDataSize) {
      return Snappy.maxCompressedLength(uncompressedDataSize);
    }

    @Override
    public CompressionType getType() {
      return SNAPPY;
    }
  }

  class IOTDBLZ4Compressor implements ICompressor {
    private LZ4Compressor compressor;

    public IOTDBLZ4Compressor() {
      super();
      LZ4Factory factory = LZ4Factory.fastestInstance();
      compressor = factory.fastCompressor();
    }

    @Override
    public byte[] compress(byte[] data) {
      logger.error("Compress LZ4 start");
      if (data == null) {
        logger.error("Compress LZ4 stop");
        return new byte[0];
      }
      byte[] r = compressor.compress(data);
      logger.error("Compress LZ4 stop");
      return r;
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
      logger.error("Compress LZ4 start");
      byte[] r = compressor.compress(data, offset, length);
      logger.error("Compress LZ4 stop");
      return r;
    }

    @Override
    public int compress(byte[] data, int offset, int length, byte[] compressed) {
      logger.error("Compress LZ4 start");
      int r = compressor.compress(data, offset, length, compressed, 0);
      logger.error("Compress LZ4 stop");
      return r;
    }

    @Override
    public int compress(ByteBuffer data, ByteBuffer compressed) {
      logger.error("Compress LZ4 start");
      compressor.compress(data, compressed);
      int r = data.limit();
      logger.error("Compress LZ4 stop");
      return r;
    }

    @Override
    public int getMaxBytesForCompression(int uncompressedDataSize) {
      return compressor.maxCompressedLength(uncompressedDataSize);
    }

    @Override
    public CompressionType getType() {
      return LZ4;
    }
  }

  class GZIPCompress {
    public static byte[] compress(byte[] data) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      GZIPOutputStream gzip = new GZIPOutputStream(out);
      gzip.write(data);
      gzip.close();
      byte[] r = out.toByteArray();
      return r;
    }

    public static byte[] uncompress(byte[] data) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ByteArrayInputStream in = new ByteArrayInputStream(data);

      GZIPInputStream ungzip = new GZIPInputStream(in);
      byte[] buffer = new byte[256];
      int n;
      while ((n = ungzip.read(buffer)) > 0) {
        out.write(buffer, 0, n);
      }
      in.close();
      byte[] r = out.toByteArray();
      return r;
    }
  }

  class GZIPCompressor implements ICompressor {
    @Override
    public byte[] compress(byte[] data) throws IOException {
      logger.error("Compress GZIP start");
      if (null == data) {
        return new byte[0];
      }
      byte[] r = GZIPCompress.compress(data);
      logger.error("Compress GZIP stop");
      return r;
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
      logger.error("Compress GZIP start");
      byte[] dataBefore = new byte[length];
      System.arraycopy(data, offset, dataBefore, 0, length);
      byte[] r = GZIPCompress.compress(dataBefore);
      logger.error("Compress GZIP stop");
      return r;
    }

    /** @exception GZIPCompressOverflowException if compressed byte array is too small. */
    @Override
    public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
      logger.error("Compress GZIP start");
      byte[] dataBefore = new byte[length];
      System.arraycopy(data, offset, dataBefore, 0, length);
      byte[] res = GZIPCompress.compress(dataBefore);
      if (res.length > compressed.length) {
        throw new GZIPCompressOverflowException();
      }
      System.arraycopy(res, 0, compressed, 0, res.length);
      logger.error("Compress GZIP stop");
      return res.length;
    }

    /** @exception GZIPCompressOverflowException if compressed ByteBuffer is too small. */
    @Override
    public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
      logger.error("Compress GZIP start");
      int length = data.remaining();
      byte[] dataBefore = new byte[length];
      data.get(dataBefore, 0, length);
      byte[] res = GZIPCompress.compress(dataBefore);
      if (res.length > compressed.capacity()) {
        throw new GZIPCompressOverflowException();
      }
      compressed.put(res);
      logger.error("Compress GZIP stop");
      return res.length;
    }

    @Override
    public int getMaxBytesForCompression(int uncompressedDataSize) {
      // hard to estimate
      return Math.max(40 + uncompressedDataSize / 2, uncompressedDataSize);
    }

    @Override
    public CompressionType getType() {
      return GZIP;
    }
  }
}
