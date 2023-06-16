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

package org.apache.iotdb.consensus.natraft.protocol.log;

import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EntrySerialization {

  private static final Logger logger = LoggerFactory.getLogger(EntrySerialization.class);
  private volatile byte[] recycledBuffer;
  private volatile ByteBuffer preSerializationCache;
  private volatile ByteBuffer serializationCache;
  private volatile ByteBuffer compressionCache;
  private CompressionType compressionType = CompressionType.UNCOMPRESSED;
  private int uncompressedSize;

  public void preSerialize(Entry entry) {
    if (preSerializationCache != null || serializationCache != null) {
      return;
    }
    long startTime = Statistic.SERIALIZE_ENTRY.getOperationStartTime();
    ByteBuffer byteBuffer = entry.serializeInternal(recycledBuffer);
    Statistic.SERIALIZE_ENTRY.calOperationCostTimeFromStart(startTime);
    preSerializationCache = byteBuffer;
  }

  public ByteBuffer serialize(Entry entry) {
    ByteBuffer cache = serializationCache;
    if (cache != null) {
      return cache.slice();
    }
    if (preSerializationCache != null) {
      ByteBuffer slice = preSerializationCache.slice();
      slice.position(1);
      slice.putLong(entry.getCurrLogIndex());
      slice.putLong(entry.getCurrLogTerm());
      slice.putLong(entry.getPrevTerm());
      slice.position(0);
      serializationCache = slice;
      preSerializationCache = null;
    } else {
      long startTime = Statistic.SERIALIZE_ENTRY.getOperationStartTime();
      ByteBuffer byteBuffer = entry.serializeInternal(recycledBuffer);
      Statistic.SERIALIZE_ENTRY.calOperationCostTimeFromStart(startTime);
      serializationCache = byteBuffer;
    }
    entry.setByteSize(serializationCache.remaining());
    return serializationCache.slice();
  }

  public ByteBuffer serialize(Entry entry, ICompressor compressor) {
    ByteBuffer cache = compressionCache;
    if (cache != null && cache.limit() > 0) {
      return cache.slice();
    }

    if (preSerializationCache != null) {
      ByteBuffer slice = preSerializationCache.slice();
      slice.position(1);
      slice.putLong(entry.getCurrLogIndex());
      slice.putLong(entry.getCurrLogTerm());
      slice.putLong(entry.getPrevTerm());
      slice.position(0);
      serializationCache = slice;
      preSerializationCache = null;
    } else {
      long startTime = Statistic.SERIALIZE_ENTRY.getOperationStartTime();
      ByteBuffer byteBuffer = entry.serializeInternal(recycledBuffer);
      Statistic.SERIALIZE_ENTRY.calOperationCostTimeFromStart(startTime);
      serializationCache = byteBuffer;
    }
    compressSerializedCache(compressor);
    entry.setByteSize(compressionCache.remaining());
    return compressionCache.slice();
  }

  public static void main(String[] args) throws IOException {
    byte[] test = "tetetagsahfdkjhxcvjboi".getBytes();
    ByteBuffer testBuffer = ByteBuffer.wrap(test);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.LZ4);
    int maxBytesForCompression = compressor.getMaxBytesForCompression(test.length);
    ByteBuffer compressed = ByteBuffer.allocate(maxBytesForCompression);
    int compressLength = compressor.compress(testBuffer, compressed);
    compressed.position(0);
    compressed.limit(compressLength);

    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.LZ4);
    ByteBuffer uncompressed = ByteBuffer.allocate(test.length);
    unCompressor.uncompress(compressed, uncompressed);
  }

  private void compressSerializedCache(ICompressor compressor) {
    long startTime = Statistic.RAFT_SENDER_COMPRESS_LOG.getOperationStartTime();
    int uncompressedSize = serializationCache.remaining();
    Statistic.LOG_DISPATCHER_RAW_SIZE.add(uncompressedSize);

    this.uncompressedSize = uncompressedSize;
    int maxBytesForCompression = compressor.getMaxBytesForCompression(uncompressedSize);
    if (compressionCache == null || compressionCache.remaining() < maxBytesForCompression) {
      compressionCache = ByteBuffer.allocate(maxBytesForCompression);
    }
    try {
      int compressedLength =
          compressor.compress(
              serializationCache.array(),
              serializationCache.arrayOffset() + serializationCache.position(),
              uncompressedSize,
              compressionCache.array());
      Statistic.LOG_DISPATCHER_COMPRESSED_SIZE.add(compressedLength);
      compressionCache.position(0);
      compressionCache.limit(compressedLength);

      this.compressionType = compressor.getType();
    } catch (IOException e) {
      logger.warn("Cannot compress entry", e);
      this.compressionType = CompressionType.UNCOMPRESSED;
    }

    Statistic.RAFT_SENDER_COMPRESS_LOG.calOperationCostTimeFromStart(startTime);
  }

  public long serializedSize() {
    ByteBuffer cache;
    if ((cache = serializationCache) != null) {
      return cache.remaining();
    } else if ((cache = preSerializationCache) != null) {
      return cache.remaining();
    }
    return 0;
  }

  public void clear() {
    if (preSerializationCache != null) {
      recycledBuffer = preSerializationCache.array();
      preSerializationCache = null;
    }
    if (serializationCache != null) {
      recycledBuffer = serializationCache.array();
      serializationCache = null;
    }
    if (compressionCache != null) {
      compressionCache.limit(0);
    }
  }

  public byte[] getRecycledBuffer() {
    return recycledBuffer;
  }

  public void setRecycledBuffer(byte[] recycledBuffer) {
    this.recycledBuffer = recycledBuffer;
  }

  public ByteBuffer getPreSerializationCache() {
    return preSerializationCache;
  }

  public void setPreSerializationCache(ByteBuffer preSerializationCache) {
    this.preSerializationCache = preSerializationCache;
  }

  public ByteBuffer getSerializationCache() {
    return serializationCache;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public void setSerializationCache(ByteBuffer serializationCache) {
    this.serializationCache = serializationCache;
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }
}
