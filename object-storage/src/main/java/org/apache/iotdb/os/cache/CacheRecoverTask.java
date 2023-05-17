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
import java.io.InputStream;
import java.nio.file.Files;

import static org.apache.iotdb.os.utils.ObjectStorageConstant.CACHE_FILE_SUFFIX;

public class CacheRecoverTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(CacheRecoverTask.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final OSFileCache cache = OSFileCache.getInstance();

  @Override
  public void run() {
    long maxCacheFileId = 0;
    for (String cacheDir : config.getCacheDirs()) {
      File cacheDirFile = new File(cacheDir);
      if (!cacheDirFile.exists()) {
        continue;
      }
      File[] cacheFiles = cacheDirFile.listFiles();
      if (cacheFiles == null) {
        continue;
      }
      for (File cacheFile : cacheFiles) {
        try {
          if (cacheFile.isDirectory()) {
            continue;
          }
          String cacheFileName = cacheFile.getName();
          if (!cacheFileName.endsWith(CACHE_FILE_SUFFIX)) {
            cacheFile.delete();
            continue;
          }
          // read meta and put it back to the cache
          try (InputStream in = Files.newInputStream(cacheFile.toPath())) {
            OSFileCacheKey key = OSFileCacheKey.deserialize(in);
            int metaSize = key.serializeSize();
            int dataSize = (int) cacheFile.length() - metaSize;
            if (dataSize != config.getCachePageSize()) {
              logger.debug(
                  "Cache file {}'s data size doesn't match the cache page size, so delete it.",
                  cacheFile);
              cacheFile.delete();
              continue;
            }
            OSFileCacheValue value = new OSFileCacheValue(cacheFile, 0, metaSize, dataSize);
            cache.put(key, value);
          }
          // update max cache file id
          String cacheFileIdStr = cacheFileName.substring(0, cacheFileName.indexOf('.'));
          long cacheFileId = Long.parseLong(cacheFileIdStr);
          if (cacheFileId > maxCacheFileId) {
            maxCacheFileId = cacheFileId;
          }
        } catch (Exception e) {
          logger.error("Failed to recover cache file {}", cacheFile.getName(), e);
        }
      }
    }
  }
}
