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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TsFileTableSizeCacheReader {

  private long readSize = 0;
  private final File keyFile;
  private final long keyFileLength;
  private final File valueFile;
  private final long valueFileLength;
  private DirectBufferedSeekableFileInputStream inputStream;
  private final int regionId;

  public TsFileTableSizeCacheReader(
      long keyFileLength, File keyFile, long valueFileLength, File valueFile, int regionId) {
    this.keyFile = keyFile;
    this.keyFileLength = keyFileLength;
    this.valueFile = valueFile;
    this.valueFileLength = valueFileLength;
    this.regionId = regionId;
  }

  public void openKeyFile() throws IOException {
    if (keyFileLength > 0 && inputStream == null) {
      this.inputStream = new DirectBufferedSeekableFileInputStream(keyFile.toPath(), 4 * 1024);
    }
  }

  public void openValueFile() throws IOException {
    if (valueFileLength > 0 && inputStream == null) {
      this.inputStream = new DirectBufferedSeekableFileInputStream(valueFile.toPath(), 4 * 1024);
    }
  }

  public Pair<Long, Long> selfCheck() {
    List<Pair<Long, Long>> offsetsByReadValueFile = new ArrayList<>();
    try {
      openValueFile();
      while (readSize < valueFileLength) {
        long offset = inputStream.position();
        int tableNum = ReadWriteForEncodingUtils.readVarInt(inputStream);
        if (tableNum <= 0) {
          break;
        }
        for (int i = 0; i < tableNum; i++) {
          ReadWriteIOUtils.readVarIntString(inputStream);
          ReadWriteIOUtils.readLong(inputStream);
        }
        offsetsByReadValueFile.add(new Pair<>(offset, inputStream.position()));
      }
    } catch (Exception ignored) {
    } finally {
      closeCurrentFile();
    }

    if (offsetsByReadValueFile.isEmpty()) {
      return new Pair<>(0L, 0L);
    }
    Iterator<Pair<Long, Long>> valueOffsetIterator = offsetsByReadValueFile.iterator();
    long keyFileTruncateSize = 0;
    long valueFileTruncateSize = 0;
    try {
      openKeyFile();
      while (readSize < keyFileLength) {
        KeyFileEntry keyFileEntry = readOneEntryFromKeyFile();
        if (keyFileEntry.originTsFileID != null) {
          continue;
        }
        if (!valueOffsetIterator.hasNext()) {
          break;
        }
        Pair<Long, Long> startEndOffsetInValueFile = valueOffsetIterator.next();
        if (startEndOffsetInValueFile.left != keyFileEntry.offset) {
          break;
        }
        keyFileTruncateSize = readSize;
        valueFileTruncateSize = startEndOffsetInValueFile.right;
      }
    } catch (Exception ignored) {
    } finally {
      closeCurrentFile();
    }
    return new Pair<>(keyFileTruncateSize, valueFileTruncateSize);
  }

  public boolean readFromKeyFile(
      Map<Long, TimePartitionTableSizeQueryContext> timePartitionContexts,
      long startTime,
      long maxRunTime)
      throws IOException {
    long previousTimePartition = 0;
    TimePartitionTableSizeQueryContext timePartitionContext = null;
    do {
      if (readSize >= keyFileLength) {
        closeCurrentFile();
        return true;
      }
      try {
        KeyFileEntry keyFileEntry = readOneEntryFromKeyFile();
        if (timePartitionContext == null
            || keyFileEntry.tsFileID.timePartitionId != previousTimePartition) {
          previousTimePartition = keyFileEntry.tsFileID.timePartitionId;
          timePartitionContext = timePartitionContexts.get(previousTimePartition);
        }
        if (keyFileEntry.originTsFileID == null) {
          timePartitionContext.addCachedTsFileIDAndOffsetInValueFile(
              keyFileEntry.tsFileID, keyFileEntry.offset);
        } else {
          timePartitionContext.replaceCachedTsFileID(
              keyFileEntry.tsFileID, keyFileEntry.originTsFileID);
        }
      } catch (IOException e) {
        readSize = keyFileLength;
        closeCurrentFile();
        throw e;
      }
    } while (System.nanoTime() - startTime < maxRunTime);
    return false;
  }

  private KeyFileEntry readOneEntryFromKeyFile() throws IOException {
    byte type = ReadWriteIOUtils.readByte(inputStream);
    long timePartition = ReadWriteIOUtils.readLong(inputStream);
    long timestamp = ReadWriteIOUtils.readLong(inputStream);
    long fileVersion = ReadWriteIOUtils.readLong(inputStream);
    long compactionVersion = ReadWriteIOUtils.readLong(inputStream);
    TsFileID tsFileID =
        new TsFileID(regionId, timePartition, timestamp, fileVersion, compactionVersion);
    KeyFileEntry keyFileEntry;
    if (type == TableDiskUsageCacheWriter.KEY_FILE_RECORD_TYPE_OFFSET) {
      long offset = ReadWriteIOUtils.readLong(inputStream);
      keyFileEntry = new KeyFileEntry(tsFileID, offset);
      readSize += TableDiskUsageCacheWriter.KEY_FILE_OFFSET_RECORD_LENGTH + 1;
    } else if (type == TableDiskUsageCacheWriter.KEY_FILE_RECORD_TYPE_REDIRECT) {
      long originTimestamp = ReadWriteIOUtils.readLong(inputStream);
      long originFileVersion = ReadWriteIOUtils.readLong(inputStream);
      long originCompactionVersion = ReadWriteIOUtils.readLong(inputStream);
      TsFileID originTsFileID =
          new TsFileID(
              regionId, timePartition, originTimestamp, originFileVersion, originCompactionVersion);
      keyFileEntry = new KeyFileEntry(tsFileID, originTsFileID);
      readSize += TableDiskUsageCacheWriter.KEY_FILE_REDIRECT_RECORD_LENGTH + 1;
    } else {
      throw new IoTDBRuntimeException(
          "Unsupported record type in file: " + keyFile.getPath() + ", type: " + type,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return keyFileEntry;
  }

  public boolean readFromValueFile(
      Iterator<Pair<TsFileID, Long>> tsFilesToQueryInCache,
      Map<Long, TimePartitionTableSizeQueryContext> resultMap,
      long startTime,
      long maxRunTime)
      throws IOException {
    long previousTimePartition = 0;
    TimePartitionTableSizeQueryContext currentTimePartition = null;
    do {
      if (!tsFilesToQueryInCache.hasNext()) {
        closeCurrentFile();
        return true;
      }
      Pair<TsFileID, Long> pair = tsFilesToQueryInCache.next();
      long timePartition = pair.left.timePartitionId;
      if (currentTimePartition == null || timePartition != previousTimePartition) {
        currentTimePartition = resultMap.get(timePartition);
        previousTimePartition = timePartition;
      }
      long offset = pair.right;
      inputStream.seek(offset);

      readSize = offset;
      int tableNum = ReadWriteForEncodingUtils.readVarInt(inputStream);
      for (int i = 0; i < tableNum; i++) {
        String tableName = ReadWriteIOUtils.readVarIntString(inputStream);
        long size = ReadWriteIOUtils.readLong(inputStream);
        currentTimePartition.updateResult(tableName, size);
      }
    } while (System.nanoTime() - startTime < maxRunTime);
    return false;
  }

  public void closeCurrentFile() {
    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (IOException ignored) {
      }
      inputStream = null;
      readSize = 0;
    }
  }

  private static class KeyFileEntry {
    public TsFileID tsFileID;
    public TsFileID originTsFileID;
    public long offset;

    public KeyFileEntry(TsFileID tsFileID, long offset) {
      this.tsFileID = tsFileID;
      this.offset = offset;
    }

    public KeyFileEntry(TsFileID tsFileID, TsFileID originTsFileID) {
      this.tsFileID = tsFileID;
      this.originTsFileID = originTsFileID;
    }
  }

  private static class DirectBufferedSeekableFileInputStream extends InputStream {

    private final FileChannel channel;
    private ByteBuffer buffer;

    private long bufferStartPos = 0;
    private long position = 0;

    private final int seekThreshold;

    public DirectBufferedSeekableFileInputStream(Path path, int bufferSize) throws IOException {
      this.channel = FileChannel.open(path, StandardOpenOption.READ);
      this.buffer = ByteBuffer.allocateDirect(bufferSize);
      this.buffer.limit(0);
      this.seekThreshold = bufferSize * 2;
    }

    public void seek(long newPos) throws IOException {
      if (newPos == position) {
        return;
      }

      if (newPos > position) {

        long bufferEnd = bufferStartPos + buffer.limit();

        if (newPos < bufferEnd) {
          buffer.position((int) (newPos - bufferStartPos));
          position = newPos;
          return;
        }

        long gap = newPos - position;

        if (gap <= seekThreshold) {
          discardBuffer();
          bufferStartPos = position;
          refill();
          if (newPos < bufferStartPos + buffer.limit()) {
            buffer.position((int) (newPos - bufferStartPos));
            position = newPos;
            return;
          }
        }
      }

      discardBuffer();
      channel.position(newPos);
      bufferStartPos = newPos;
      position = newPos;
    }

    @Override
    public int read() throws IOException {
      if (!buffer.hasRemaining()) {
        if (!refill()) {
          return -1;
        }
      }
      position++;
      return buffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] dst, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }

      int totalRead = 0;

      while (len > 0) {
        if (!buffer.hasRemaining()) {
          if (!refill()) {
            return totalRead == 0 ? -1 : totalRead;
          }
        }

        int n = Math.min(len, buffer.remaining());
        buffer.get(dst, off, n);

        off += n;
        len -= n;
        totalRead += n;
        position += n;
      }

      return totalRead;
    }

    private boolean refill() throws IOException {
      buffer.clear();
      bufferStartPos = channel.position();

      int read = channel.read(buffer);
      if (read <= 0) {
        buffer.limit(0);
        return false;
      }

      buffer.flip();
      return true;
    }

    private void discardBuffer() {
      buffer.clear();
      buffer.limit(0);
    }

    public long position() {
      return position;
    }

    @Override
    public void close() throws IOException {
      if (buffer != null) {
        MmapUtil.clean(buffer);
        buffer = null;
      }
      channel.close();
    }
  }
}
