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

import static org.apache.iotdb.os.utils.ObjectStorageConstant.CACHE_FILE_SUFFIX;
import static org.apache.iotdb.os.utils.ObjectStorageConstant.TMP_CACHE_FILE_SUFFIX;

/** This class manages all write operations to the cache files */
public class CacheFileManager {
  private static final Logger logger = LoggerFactory.getLogger(CacheFileManager.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private final String[] cacheDirs = config.getCacheDirs();
  private final AtomicLong cacheFileId = new AtomicLong(0);

  private CacheFileManager() {
    for (String cacheDir : cacheDirs) {
      File cacheDirFile = new File(cacheDir);
      if (!cacheDirFile.exists()) {
        cacheDirFile.mkdirs();
      }
    }
  }

  private long getNextCacheFileId() {
    return cacheFileId.getAndIncrement();
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
      ByteBuffer meta = key.serialize();
      channel.write(meta);
      channel.write(ByteBuffer.wrap(data));
      res =
          new OSFileCacheValue(
              tmpCacheFile, 0, meta.capacity(), data.length, key.getStartPosition());
    } catch (IOException e) {
      logger.error("Fail to persist data to cache file {}", tmpCacheFile, e);
      tmpCacheFile.delete();
    }
    return tmpCacheFile.renameTo(getCacheFile(cacheFileId)) ? res : null;
  }

  /** This method is used by the recover procedure */
  void setCacheFileId(long startId) {
    cacheFileId.set(startId);
  }

  public static CacheFileManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final CacheFileManager INSTANCE = new CacheFileManager();
  }
}
