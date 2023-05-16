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
  private File cacheFile;
  // 如果每个块用一个文件来存储，则该值一直为 0
  // 如果使用一个大文件存储所有块，则该值为大文件中的起点
  private long startPosition;
  // 如果每个块用一个文件来存储，则该值一直为该文件的大小
  // 如果使用一个大文件存储所有块，则该值为该块的实际长度
  private int length;
  private int metaSize;
  private boolean shouldDelete;
  private int readCnt;

  public OSFileCacheValue(File cacheFile, long startPosition, int length, int metaSize) {
    this.cacheFile = cacheFile;
    this.startPosition = startPosition;
    this.length = length;
    this.metaSize = metaSize;
  }

  public File getCacheFile() {
    return cacheFile;
  }

  public long getStartPosition() {
    return startPosition;
  }

  public int getLength() {
    return length;
  }

  public int getMetaSize() {
    return metaSize;
  }

  public int getOccupiedLength() {
    // 如果使用多个文件，则返回该文件的大小
    // 如果使用一个文件，则返回每个槽的大小
    return length;
  }

  public synchronized void setShouldDelete() {
    this.shouldDelete = true;
    if (readCnt == 0) {
      cacheFile.delete();
    }
  }

  public synchronized boolean readLock() {
    if (cacheFile.exists()) {
      this.readCnt++;
      return true;
    } else {
      return false;
    }
  }

  public synchronized void readUnlock() {
    this.readCnt--;
    if (shouldDelete && readCnt == 0) {
      cacheFile.delete();
    }
  }
}
