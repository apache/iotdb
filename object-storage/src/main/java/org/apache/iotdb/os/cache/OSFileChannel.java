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
  private OSFileCacheValue cacheFile;
  private FileChannel cacheFileChannel;

  public OSFileChannel(OSFile osFile) throws IOException {
    this.osFile = osFile;
    openNextCacheFile();
  }

  public static InputStream newInputStream(OSFileChannel channel) {
    return new OSInputStream(channel);
  }

  private boolean isPositionValid() {
    return cacheFile.getStartPositionInTsFile() <= position
        && position < cacheFile.getEndPositionInTsFile();
  }

  private void openNextCacheFile() throws IOException {
    // close prev cache file
    close();
    // open next cache file
    OSFileCacheKey key = locateCacheFileFromPosition();
    while (!cacheFile.tryReadLock()) {
      cacheFile = cache.get(key);
    }
    cacheFileChannel = FileChannel.open(cacheFile.getCacheFile().toPath(), StandardOpenOption.READ);
  }

  private OSFileCacheKey locateCacheFileFromPosition() throws IOException {
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

  public void position(long newPosition) {
    if (newPosition < 0) {
      throw new IllegalArgumentException();
    }
    position = newPosition;
  }

  public int read(ByteBuffer dst) throws IOException {
    return read(dst, position);
  }

  public int read(ByteBuffer dst, long position) throws IOException {
    dst.mark();
    // determiner the ead range
    long startPos = position;
    long endPos = position + dst.remaining();
    if (startPos >= size()) {
      return -1;
    }
    if (endPos > size()) {
      endPos = size();
    }
    // read each cache file
    int totalReadBytes = 0;
    while (startPos < endPos) {
      if (!isPositionValid()) {
        openNextCacheFile();
      }
      long readStartPosition = cacheFile.convertTsFilePos2CachePos(startPos);
      long readEndPosition = cacheFile.convertTsFilePos2CachePos(endPos);
      if (readEndPosition < 0) {
        readEndPosition = cacheFile.getEndPositionInCacheFile();
      }
      int readSize = (int) (readEndPosition - readStartPosition);
      cacheFileChannel.position(readStartPosition);
      long read = cacheFileChannel.read(new ByteBuffer[] {dst}, 0, readSize);
      if (read != readSize) {
        dst.reset();
        throw new IOException(
            String.format(
                "Cache file %s may crash because cannot read enough information in the cash file.",
                osFile));
      }
      totalReadBytes += read;
      startPos += read;
    }
    this.position = position + totalReadBytes;
    return totalReadBytes;
  }

  @Override
  public void close() throws IOException {
    if (cacheFile != null) {
      try {
        cacheFileChannel.close();
      } finally {
        cacheFile.readUnlock();
      }
    }
  }
}
