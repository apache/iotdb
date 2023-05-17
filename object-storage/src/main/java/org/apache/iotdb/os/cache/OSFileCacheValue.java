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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class OSFileCacheValue {
  private static final Logger logger = LoggerFactory.getLogger(OSFileCacheValue.class);
  /** local cache file */
  private File cacheFile;
  // 如果每个块用一个文件来存储，则该值一直为 0
  // 如果使用一个大文件存储所有块，则该值为大文件中的起点
  /** start position in the local cache file */
  private long startPositionInCacheFile;

  private long startPositionOfOSFile;
  /** cache data size */
  private int dataSize;
  /** cache key size */
  private int metaSize;

  private boolean shouldDelete;
  private int readCnt;

  private FileChannel cacheFileChannel;

  public OSFileCacheValue(
      File cacheFile, long startPosition, long startPositionOfOSFile, int metaSize, int dataSize) {
    this.cacheFile = cacheFile;
    this.startPositionInCacheFile = startPosition;
    this.startPositionOfOSFile = startPositionOfOSFile;
    this.metaSize = metaSize;
    this.dataSize = dataSize;
  }

  public File getCacheFile() {
    return cacheFile;
  }

  public long getStartPositionInCacheFile() {
    return startPositionInCacheFile;
  }

  public int getMetaSize() {
    return metaSize;
  }

  public int getDataSize() {
    return dataSize;
  }

  // 如果每个块用一个文件来存储，则该值一直为该文件的大小
  // 如果使用一个大文件存储所有块，则该值为该块的实际长度
  public int getLength() {
    return metaSize + dataSize;
  }

  public boolean containsPosition(long position) {
    return startPositionInCacheFile <= position && position < startPositionInCacheFile + dataSize;
  }

  public int read(ByteBuffer dst, long startPosition) throws IOException {
    long startPosInCacheFile = metaSize + (startPosition - this.startPositionOfOSFile);
    return cacheFileChannel.read(dst, startPosInCacheFile);
  }

  /** Mark this value should be deleted, delete this value when no one is reading it. */
  public synchronized void setShouldDelete() {
    this.shouldDelete = true;
    if (readCnt == 0) {
      cacheFile.delete();
    }
  }

  /**
   * Try to get the read lock, return false when this cache value should be deleted or has been
   * deleted.
   */
  public synchronized boolean tryReadLock() throws IOException {
    if (shouldDelete || !cacheFile.exists()) {
      return false;
    } else {
      this.readCnt++;
      if (!cacheFileChannel.isOpen()) {
        cacheFileChannel = FileChannel.open(cacheFile.toPath(), StandardOpenOption.READ);
      }
      return true;
    }
  }

  /**
   * Release the read lock, delete the cache value when no one else is reading it and this cache
   * value should be deleted.
   */
  public synchronized void readUnlock() throws IOException {
    this.readCnt--;
    // delete the cache file only when no reference is used
    if (shouldDelete && readCnt == 0) {
      boolean success = cacheFile.delete();
      if (!success) {
        logger.error("[OSFileCache] cannot delete cache file {}", cacheFile);
      }
    }
    // close the file channel if no reference is used
    if (readCnt == 0) {
      cacheFileChannel.close();
    }
  }
}
