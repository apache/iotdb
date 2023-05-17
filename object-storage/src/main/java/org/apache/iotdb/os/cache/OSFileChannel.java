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
package org.apache.iotdb.os.cache;

import org.apache.iotdb.os.conf.ObjectStorageConfig;
import org.apache.iotdb.os.conf.ObjectStorageDescriptor;
import org.apache.iotdb.os.fileSystem.OSFile;
import org.apache.iotdb.os.fileSystem.OSTsFileInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class OSFileChannel implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(OSTsFileInput.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final OSFileCache cache = OSFileCache.getInstance();
  private final OSFile osFile;
  private long position = 0;

  private OSFileBlock currentOSFileBlock;

  public OSFileChannel(OSFile osFile) throws IOException {
    this.osFile = osFile;
  }

  public static InputStream newInputStream(OSFileChannel channel) {
    return new OSInputStream(channel);
  }

  private void openNextCacheFile(int position) throws IOException {
    // close prev cache file
    closeCurrentOSFileBlock();
    // open next cache file
    OSFileCacheKey key = locateCacheFileFromPosition(position);
    currentOSFileBlock = new OSFileBlock(key);
  }

  private OSFileCacheKey locateCacheFileFromPosition(int position) throws IOException {
    if (position >= size()) {
      throw new IOException("EOF");
    }
    long startPosition = position - position % config.getCachePageSize();
    return new OSFileCacheKey(osFile, startPosition);
  }

  public long size() {
    return osFile.length();
  }

  public long position() {
    return position;
  }

  public synchronized void position(long newPosition) {
    if (newPosition < 0) {
      throw new IllegalArgumentException();
    }
    position = newPosition;
  }

  public synchronized int read(ByteBuffer dst) throws IOException {
    int readSize = read(dst, position);
    position(position + readSize);
    return readSize;
  }

  public synchronized int read(ByteBuffer dst, long position) throws IOException {
    int currentPosition = (int) position;
    dst.mark();
    int dstLimit = dst.limit();
    // read each cache file
    int totalReadBytes = 0;

    // determiner the ead range
    long startPos = position;
    long endPos = position + dst.remaining();
    if (startPos >= size()) {
      return -1;
    }
    if (endPos > size()) {
      endPos = size();
    }
    try {
      while (startPos < endPos) {
        if (currentOSFileBlock == null || !currentOSFileBlock.canRead(startPos)) {
          openNextCacheFile(currentPosition);
        }
        int readSize = currentOSFileBlock.read(dst, startPos, endPos);
        totalReadBytes += readSize;
        startPos += readSize;
        currentPosition += readSize;
      }
    } catch (IOException e) {
      dst.reset();
      throw e;
    } finally {
      dst.limit(dstLimit);
    }
    return totalReadBytes;
  }

  private void closeCurrentOSFileBlock() throws IOException {
    if (currentOSFileBlock != null) {
      currentOSFileBlock.close();
    }
  }

  @Override
  public void close() throws IOException {
    closeCurrentOSFileBlock();
  }

  private static class OSFileBlock {
    private OSFileCacheValue cacheValue;
    private FileChannel fileChannel;

    public OSFileBlock(OSFileCacheKey cacheKey) throws IOException {
      do {
        cacheValue = cache.get(cacheKey);
      } while (!cacheValue.tryReadLock());
      fileChannel = FileChannel.open(cacheValue.getCacheFile().toPath(), StandardOpenOption.READ);
    }

    public boolean canRead(long positionInOSFile) {
      return cacheValue.getStartPositionInOSFile() <= positionInOSFile
          && positionInOSFile < cacheValue.getEndPositionInOSFile();
    }

    public int read(ByteBuffer dst, long startPos, long endPos) throws IOException {
      long readStartPosition = cacheValue.convertTsFilePos2CachePos(startPos);
      long expectedReadLength = endPos - startPos;

      int readSize =
          (int)
              Math.min(
                  expectedReadLength, cacheValue.getEndPositionInCacheFile() - readStartPosition);

      dst.limit(dst.position() + readSize);
      long actualReadSize = fileChannel.read(dst, readStartPosition);
      if (actualReadSize != readSize) {
        throw new IOException(
            String.format(
                "Cache file %s may crash because cannot read enough information in the cash file.",
                cacheValue.getCacheFile()));
      }
      return readSize;
    }

    public void close() throws IOException {
      try {
        fileChannel.close();
      } finally {
        cacheValue.readUnlock();
      }
    }
  }
}
