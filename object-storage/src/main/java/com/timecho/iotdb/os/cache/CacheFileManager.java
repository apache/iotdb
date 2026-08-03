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
package com.timecho.iotdb.os.cache;

import com.timecho.iotdb.os.conf.ObjectStorageConfig;
import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

import static com.timecho.iotdb.os.utils.ObjectStorageConstant.CACHE_FILE_SUFFIX;
import static com.timecho.iotdb.os.utils.ObjectStorageConstant.TMP_CACHE_FILE_SUFFIX;

/** This class manages all write operations to the cache files */
public class CacheFileManager {
  private static final Logger logger = LoggerFactory.getLogger(CacheFileManager.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();

  /** incremental id of next cache file */
  private final AtomicLong cacheFileId = new AtomicLong(0);

  /** disk usage of all cache files */
  private final AtomicLong totalCacheFileSize = new AtomicLong(0);

  private CacheFileManager() {
    for (String cacheDir : config.getCacheDirs()) {
      File cacheDirFile = new File(cacheDir);
      if (!cacheDirFile.exists()) {
        cacheDirFile.mkdirs();
      }
    }
  }

  private long getNextCacheFileId() {
    return cacheFileId.incrementAndGet();
  }

  private File getTmpCacheFile(long id) {
    long dirId = id % config.getCacheDirs().length;
    return new File(config.getCacheDirs()[(int) dirId], id + TMP_CACHE_FILE_SUFFIX);
  }

  private File getCacheFile(long id) {
    long dirId = id % config.getCacheDirs().length;
    return new File(config.getCacheDirs()[(int) dirId], id + CACHE_FILE_SUFFIX);
  }

  /** Persist data, return null when failing to persist data */
  public OSFileCacheValue persist(OSFileCacheKey key, byte[] data) {
    long cacheFileId;
    File tmpCacheFile;
    // create new tmp cache file
    try {
      do {
        cacheFileId = getNextCacheFileId();
        tmpCacheFile = getTmpCacheFile(cacheFileId);
      } while (!tmpCacheFile.createNewFile());
    } catch (IOException e) {
      logger.error("Fail to create cache file.", e);
      return null;
    }
    // write value into tmp cache file
    try (FileChannel channel = FileChannel.open(tmpCacheFile.toPath(), StandardOpenOption.WRITE)) {
      ByteBuffer meta = key.serialize();
      meta.flip();
      channel.write(meta);
      channel.write(ByteBuffer.wrap(data));
    } catch (IOException e) {
      logger.error("Fail to persist data to cache file {}.", tmpCacheFile, e);
      org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(tmpCacheFile);
      return null;
    }
    // rename tmp file to cache file
    File cacheFile = getCacheFile(cacheFileId);
    if (tmpCacheFile.renameTo(cacheFile)) {
      OSFileCacheValue value =
          new OSFileCacheValue(
              cacheFile, 0, key.serializeSize(), data.length, key.getStartPosition());
      totalCacheFileSize.addAndGet(value.getLength());
      return value;
    } else {
      org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(tmpCacheFile);
      return null;
    }
  }

  public boolean delete(OSFileCacheValue value) {
    totalCacheFileSize.addAndGet(-value.getLength());
    try {
      return Files.deleteIfExists(value.getCacheFile().toPath());
    } catch (IOException e) {
      return false;
    }
  }

  void setCacheFileId(long startId) {
    cacheFileId.set(startId);
  }

  long getCacheFileId() {
    return cacheFileId.get();
  }

  void setTotalCacheFileSize(long initSize) {
    totalCacheFileSize.set(initSize);
  }

  public long getTotalCacheFileSize() {
    return totalCacheFileSize.get();
  }

  void clear() {
    cacheFileId.set(0);
    totalCacheFileSize.set(0);
  }

  public static CacheFileManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final CacheFileManager INSTANCE = new CacheFileManager();
  }
}
