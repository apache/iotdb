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

public class OSFileChannel implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(OSTsFileInput.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final OSFileCache cache = OSFileCache.getInstance();
  private final OSFile osFile;
  private long position = 0;
  private OSFileCacheValue currentOSFileBlock;

  public OSFileChannel(OSFile osFile) {
    this.osFile = osFile;
  }

  public static InputStream newInputStream(OSFileChannel channel) {
    return new CacheInputStream(channel);
  }

  private boolean canReadFromCurrentCacheBlock(long startPos) {
    return currentOSFileBlock != null && currentOSFileBlock.containsPosition(startPos);
  }

  private void openNextCacheFile() throws IOException {
    // close prev cache file
    releaseCurrentOSFileBlock();
    // open next cache file
    OSFileCacheKey key = locateCacheFileFromPosition();
    // 用 while 是为了防止从 cache 中拿出来之后，对应的 value 又被挤出去，导致对应的文件被删除？
    while (currentOSFileBlock == null || !currentOSFileBlock.tryReadLock()) {
      currentOSFileBlock = cache.get(key);
    }
  }

  private OSFileCacheKey locateCacheFileFromPosition() {
    long startPosition = position - position % config.getCachePageSize();
    return new OSFileCacheKey(osFile, startPosition, config.getCachePageSize());
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
      if (!canReadFromCurrentCacheBlock(startPos)) {
        openNextCacheFile();
      }

      int maxReadSize = (int) Math.min(endPos - startPos, currentOSFileBlock.getDataSize());
      dst.limit(dst.position() + maxReadSize);

      int read = currentOSFileBlock.read(dst, startPos);
      if (read != maxReadSize) {
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

  public void releaseCurrentOSFileBlock() throws IOException {
    if (currentOSFileBlock != null) {
      currentOSFileBlock.readUnlock();
    }
  }

  @Override
  public void close() throws IOException {
    releaseCurrentOSFileBlock();
  }
}
