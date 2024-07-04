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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.MmapUtil;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class WALInputStream extends InputStream implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(WALInputStream.class);
  private final FileChannel channel;

  /** 1 byte for whether enable compression, 4 byte for compressedSize */
  private final ByteBuffer segmentHeaderWithoutCompressedSizeBuffer =
      ByteBuffer.allocate(Integer.BYTES + Byte.BYTES);

  private final ByteBuffer compressedSizeBuffer = ByteBuffer.allocate(Integer.BYTES);
  private ByteBuffer dataBuffer = null;
  private ByteBuffer compressedBuffer = null;
  private final long fileSize;
  File logFile;
  /*
   The WAL file consist of following parts:
   [MagicString] [Segment 1] [Segment 2] ... [Segment N] [Metadata] [MagicString]
   The endOffset indicates the maximum offset that a segment can reach.
   Aka, the last byte of the last segment.
  */
  private long endOffset = -1;

  WALFileVersion version;

  public WALInputStream(File logFile) throws IOException {
    channel = FileChannel.open(logFile.toPath());
    fileSize = channel.size();
    this.logFile = logFile;
    analyzeFileVersion();
    getEndOffset();
  }

  private void getEndOffset() throws IOException {
    if (channel.size() < WALWriter.MAGIC_STRING_V2_BYTES + Integer.BYTES) {
      // An broken file
      endOffset = channel.size();
      return;
    }
    ByteBuffer metadataSizeBuf = ByteBuffer.allocate(Integer.BYTES);
    long position;
    try {
      if (version == WALFileVersion.V2) {
        // New Version
        ByteBuffer magicStringBuffer = ByteBuffer.allocate(WALWriter.MAGIC_STRING_V2_BYTES);
        channel.read(magicStringBuffer, channel.size() - WALWriter.MAGIC_STRING_V2_BYTES);
        magicStringBuffer.flip();
        if (logFile.getName().endsWith(IoTDBConstant.WAL_CHECKPOINT_FILE_SUFFIX)
            || !new String(magicStringBuffer.array(), StandardCharsets.UTF_8)
                .equals(WALWriter.MAGIC_STRING_V2)) {
          // This is a broken wal or checkpoint file
          endOffset = channel.size();
          return;
        } else {
          // This is a normal wal file or check point file
          position = channel.size() - WALWriter.MAGIC_STRING_V2_BYTES - Integer.BYTES;
        }
      } else {
        if (logFile.getName().endsWith(IoTDBConstant.WAL_CHECKPOINT_FILE_SUFFIX)) {
          // this is an old check point file
          endOffset = channel.size();
          return;
        }
        // Old version
        ByteBuffer magicStringBuffer = ByteBuffer.allocate(WALWriter.MAGIC_STRING_V1_BYTES);
        channel.read(magicStringBuffer, channel.size() - WALWriter.MAGIC_STRING_V1_BYTES);
        magicStringBuffer.flip();
        if (!new String(magicStringBuffer.array(), StandardCharsets.UTF_8)
            .equals(WALWriter.MAGIC_STRING_V1)) {
          // this is a broken wal file
          endOffset = channel.size();
          return;
        } else {
          position = channel.size() - WALWriter.MAGIC_STRING_V1_BYTES - Integer.BYTES;
        }
      }
      // Read the metadata size
      channel.read(metadataSizeBuf, position);
      metadataSizeBuf.flip();
      int metadataSize = metadataSizeBuf.getInt();
      // -1 is for the endmarker
      endOffset =
          channel.size() - WALWriter.MAGIC_STRING_V2_BYTES - Integer.BYTES - metadataSize - 1;
    } finally {
      if (version == WALFileVersion.V2) {
        // Set the position back to the end of head magic string
        channel.position(WALWriter.MAGIC_STRING_V2_BYTES);
      } else {
        // There is no head magic string in V1 version
        channel.position(0);
      }
    }
  }

  private void analyzeFileVersion() throws IOException {
    version = WALFileVersion.getVersion(channel);
  }

  @Override
  public int read() throws IOException {
    if (Objects.isNull(dataBuffer) || dataBuffer.position() >= dataBuffer.limit()) {
      loadNextSegment();
    }
    return dataBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (Objects.isNull(dataBuffer) || dataBuffer.position() >= dataBuffer.limit()) {
      loadNextSegment();
    }
    if (dataBuffer.remaining() >= len) {
      dataBuffer.get(b, off, len);
      return len;
    }
    int toBeRead = len;
    while (toBeRead > 0) {
      int remaining = dataBuffer.remaining();
      int bytesRead = Math.min(remaining, toBeRead);
      dataBuffer.get(b, off, bytesRead);
      off += bytesRead;
      toBeRead -= bytesRead;
      if (toBeRead > 0) {
        loadNextSegment();
      }
    }
    return len;
  }

  @Override
  public void close() throws IOException {
    channel.close();
    MmapUtil.clean(dataBuffer);
    MmapUtil.clean(compressedBuffer);
    dataBuffer = null;
  }

  @Override
  public int available() throws IOException {
    long size = (endOffset - channel.position());
    if (!Objects.isNull(dataBuffer)) {
      size += dataBuffer.limit() - dataBuffer.position();
    }
    return (int) size;
  }

  private void loadNextSegment() throws IOException {
    if (channel.position() >= endOffset) {
      throw new IOException("Reach the end offset of wal file");
    }
    if (version == WALFileVersion.V2) {
      loadNextSegmentV2();
    } else if (version == WALFileVersion.V1) {
      loadNextSegmentV1();
    } else {
      tryLoadSegment();
    }
  }

  private void loadNextSegmentV1() throws IOException {
    // just read raw data as input
    if (channel.position() >= fileSize) {
      throw new IOException("Unexpected end of file");
    }
    if (Objects.isNull(dataBuffer)) {
      // read 128 KB
      dataBuffer = ByteBuffer.allocate(128 * 1024);
    }
    dataBuffer.clear();
    channel.read(dataBuffer);
    dataBuffer.flip();
  }

  private void loadNextSegmentV2() throws IOException {
    SegmentInfo segmentInfo = getNextSegmentInfo();
    if (segmentInfo.compressionType != CompressionType.UNCOMPRESSED) {
      // A compressed segment
      if (Objects.isNull(dataBuffer)
          || dataBuffer.capacity() < segmentInfo.uncompressedSize
          || dataBuffer.capacity() > segmentInfo.uncompressedSize * 2) {
        MmapUtil.clean(dataBuffer);
        dataBuffer = ByteBuffer.allocateDirect(segmentInfo.uncompressedSize);
      }
      dataBuffer.clear();

      if (Objects.isNull(compressedBuffer)
          || compressedBuffer.capacity() < segmentInfo.dataInDiskSize
          || compressedBuffer.capacity() > segmentInfo.dataInDiskSize * 2) {
        MmapUtil.clean(compressedBuffer);
        compressedBuffer = ByteBuffer.allocateDirect(segmentInfo.dataInDiskSize);
      }
      compressedBuffer.clear();
      // limit the buffer to prevent it from reading too much byte than expected
      compressedBuffer.limit(segmentInfo.dataInDiskSize);
      if (channel.read(compressedBuffer) != segmentInfo.dataInDiskSize) {
        throw new IOException("Unexpected end of file");
      }
      compressedBuffer.flip();
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(segmentInfo.compressionType);
      unCompressor.uncompress(compressedBuffer, dataBuffer);
    } else {
      // An uncompressed segment
      if (Objects.isNull(dataBuffer)
          || dataBuffer.capacity() < segmentInfo.dataInDiskSize
          || dataBuffer.capacity() > segmentInfo.dataInDiskSize * 2) {
        MmapUtil.clean(dataBuffer);
        dataBuffer = ByteBuffer.allocateDirect(segmentInfo.dataInDiskSize);
      }
      dataBuffer.clear();
      // limit the buffer to prevent it from reading too much byte than expected
      dataBuffer.limit(segmentInfo.dataInDiskSize);

      if (channel.read(dataBuffer) != segmentInfo.dataInDiskSize) {
        throw new IOException("Unexpected end of file");
      }
    }
    dataBuffer.flip();
  }

  private void tryLoadSegment() throws IOException {
    long originPosition = channel.position();
    try {
      loadNextSegmentV1();
      version = WALFileVersion.V1;
    } catch (Throwable e) {
      // failed to load in V2 way, try in V1 way
      channel.position(originPosition);
      loadNextSegmentV2();
      version = WALFileVersion.V2;
      logger.info("Failed to load WAL segment in V1 way, try in V2 way successfully.");
    }
  }

  /**
   * Since current WAL file is compressed, but some part of the system need to skip the offset of an
   * uncompressed wal file, this method is used to skip to the given logical position.
   *
   * @param pos The logical offset to skip to
   * @throws IOException If the file is broken or the given position is invalid
   */
  public void skipToGivenLogicalPosition(long pos) throws IOException {
    if (version == WALFileVersion.V2) {
      channel.position(WALWriter.MAGIC_STRING_V2_BYTES);
      long posRemain = pos;
      SegmentInfo segmentInfo;
      do {
        long currentPos = channel.position();
        segmentInfo = getNextSegmentInfo();
        if (posRemain >= segmentInfo.uncompressedSize) {
          posRemain -= segmentInfo.uncompressedSize;
          channel.position(currentPos + segmentInfo.dataInDiskSize + segmentInfo.headerSize());
        } else {
          break;
        }
      } while (posRemain >= 0);

      if (segmentInfo.compressionType != CompressionType.UNCOMPRESSED) {
        compressedBuffer = ByteBuffer.allocateDirect(segmentInfo.dataInDiskSize);
        channel.read(compressedBuffer);
        compressedBuffer.flip();
        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(segmentInfo.compressionType);
        dataBuffer = ByteBuffer.allocateDirect(segmentInfo.uncompressedSize);
        unCompressor.uncompress(compressedBuffer, dataBuffer);
        MmapUtil.clean(compressedBuffer);
      } else {
        dataBuffer = ByteBuffer.allocateDirect(segmentInfo.dataInDiskSize);
        channel.read(dataBuffer);
        dataBuffer.flip();
      }

      dataBuffer.position((int) posRemain);
    } else {
      dataBuffer = null;
      channel.position(pos);
    }
  }

  public void read(ByteBuffer buffer) throws IOException {
    int totalBytesToBeRead = buffer.remaining();
    int currReadBytes = Math.min(dataBuffer.remaining(), buffer.remaining());
    dataBuffer.get(buffer.array(), buffer.position(), currReadBytes);
    if (totalBytesToBeRead - currReadBytes > 0) {
      loadNextSegment();
      read(buffer);
    }
  }

  public long getFileCurrentPos() throws IOException {
    return channel.position();
  }

  private SegmentInfo getNextSegmentInfo() throws IOException {
    segmentHeaderWithoutCompressedSizeBuffer.clear();
    channel.read(segmentHeaderWithoutCompressedSizeBuffer);
    segmentHeaderWithoutCompressedSizeBuffer.flip();
    SegmentInfo info = new SegmentInfo();
    info.compressionType =
        CompressionType.deserialize(segmentHeaderWithoutCompressedSizeBuffer.get());
    info.dataInDiskSize = segmentHeaderWithoutCompressedSizeBuffer.getInt();
    if (info.compressionType != CompressionType.UNCOMPRESSED) {
      compressedSizeBuffer.clear();
      channel.read(compressedSizeBuffer);
      compressedSizeBuffer.flip();
      info.uncompressedSize = compressedSizeBuffer.getInt();
    } else {
      info.uncompressedSize = info.dataInDiskSize;
    }
    return info;
  }

  private static class SegmentInfo {
    public CompressionType compressionType;
    public int dataInDiskSize;
    public int uncompressedSize;

    int headerSize() {
      return compressionType == CompressionType.UNCOMPRESSED
          ? Byte.BYTES + Integer.BYTES
          : Byte.BYTES + Integer.BYTES * 2;
    }
  }
}
