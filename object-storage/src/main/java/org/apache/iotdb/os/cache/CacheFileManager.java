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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/** This class manages all io operations to the cache files */
public class CacheFileManager {
  private static final Logger logger = LoggerFactory.getLogger(CacheFileManager.class);
  private static final String CACHE_FILE_SUFFIX = ".cache";
  private static final String TMP_CACHE_FILE_SUFFIX = ".cache.tmp";
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private String[] cacheDirs;
  private AtomicLong logFileId = new AtomicLong(0);

  public CacheFileManager(String[] cacheDirs) {
    this.cacheDirs = cacheDirs;
  }

  private long getNextCacheFileId() {
    return logFileId.getAndIncrement();
  }

  private File getTmpCacheFile(long id) {
    long dirId = id % cacheDirs.length;
    return new File(cacheDirs[(int) dirId], id + TMP_CACHE_FILE_SUFFIX);
  }

  private File getCacheFile(long id) {
    long dirId = id % cacheDirs.length;
    return new File(cacheDirs[(int) dirId], id + CACHE_FILE_SUFFIX);
  }

  /** Persist data, return null when failing to persist data */
  public OSFileCacheValue persist(OSFileCacheKey key, byte[] data) {
    OSFileCacheValue res = null;
    long cacheFileId = getNextCacheFileId();
    File tmpCacheFile = getTmpCacheFile(cacheFileId);
    try (FileChannel channel =
        FileChannel.open(tmpCacheFile.toPath(), StandardOpenOption.CREATE_NEW)) {
      ByteBuffer meta = key.serializeToByteBuffer();
      channel.write(meta);
      channel.write(ByteBuffer.wrap(data));
      res = new OSFileCacheValue(tmpCacheFile, meta.capacity(), data.length);
    } catch (IOException e) {
      logger.error("Fail to persist data to cache file {}", tmpCacheFile, e);
      tmpCacheFile.delete();
    }
    return tmpCacheFile.renameTo(getCacheFile(cacheFileId)) ? res : null;
  }
}
