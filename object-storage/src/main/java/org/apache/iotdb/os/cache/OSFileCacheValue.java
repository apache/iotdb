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

import java.io.File;

public class OSFileCacheValue {
  /** local cache file */
  private File cacheFile;
  // 如果每个块用一个文件来存储，则该值一直为 0
  // 如果使用一个大文件存储所有块，则该值为大文件中的起点
  /** start position in the local cache file */
  private long startPositionInCacheFile;
  /** cache data size */
  private int dataSize;
  /** cache key size */
  private int metaSize;
  /** start position in the remote TsFile */
  private long startPositionInTsFile;
  /** start position in the remote TsFile */
  private long endPositionInTsFile;

  private boolean shouldDelete;
  private int readCnt;

  public OSFileCacheValue(
      File cacheFile,
      long startPositionInCacheFile,
      int metaSize,
      int dataSize,
      long startPositionInTsFile) {
    this.cacheFile = cacheFile;
    this.startPositionInCacheFile = startPositionInCacheFile;
    this.metaSize = metaSize;
    this.dataSize = dataSize;
    this.startPositionInTsFile = startPositionInTsFile;
    this.endPositionInTsFile = startPositionInTsFile + dataSize;
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

  public long getStartPositionInTsFile() {
    return startPositionInTsFile;
  }

  public long getEndPositionInTsFile() {
    return endPositionInTsFile;
  }

  public long getEndPositionInCacheFile() {
    return startPositionInCacheFile + getLength();
  }

  /**
   * Convert position in the TsFile to the corresponding position in the cache file. Return -1 when
   * the position is outside the cache file range
   */
  public long convertTsFilePos2CachePos(long positionInTsFile) {
    if (positionInTsFile < startPositionInTsFile || positionInTsFile >= endPositionInTsFile) {
      return -1;
    }
    return startPositionInCacheFile + metaSize + (positionInTsFile - startPositionInTsFile);
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
  public synchronized boolean tryReadLock() {
    if (shouldDelete || !cacheFile.exists()) {
      return false;
    } else {
      this.readCnt++;
      return true;
    }
  }

  /**
   * Release the read lock, delete the cache value when no one else is reading it and this cache
   * value should be deleted.
   */
  public synchronized void readUnlock() {
    this.readCnt--;
    if (shouldDelete && readCnt == 0) {
      cacheFile.delete();
    }
  }
}
