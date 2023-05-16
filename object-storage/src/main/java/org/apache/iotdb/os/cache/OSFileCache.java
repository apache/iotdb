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
import org.apache.iotdb.os.fileSystem.OSURI;
import org.apache.iotdb.os.io.ObjectStorageConnector;
import org.apache.iotdb.os.io.aws.S3ObjectStorageConnector;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

public class OSFileCache implements IOSFileCache {
  private static final Logger logger = LoggerFactory.getLogger(OSFileCache.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final ObjectStorageConnector connector;

  static {
    switch (config.getOsType()) {
      case AWS_S3:
        connector = new S3ObjectStorageConnector();
        break;
      default:
        connector = null;
    }
  }

  /**
   * persistent LRU cache for remote TsFile, value is loaded successfully when it has been stored on
   * the disk
   */
  private LoadingCache<OSFileCacheKey, OSFileCacheValue> remotePos2LocalCacheFile;
  /** manage all io operations to the cache files */
  private CacheFileManager cacheFileManager = new CacheFileManager(config.getCacheDirs());

  private OSFileCache() {
    remotePos2LocalCacheFile =
        Caffeine.newBuilder()
            .maximumWeight(config.getCacheMaxDiskUsage())
            .weigher(
                (Weigher<OSFileCacheKey, OSFileCacheValue>)
                    (key, value) -> value.getOccupiedLength())
            .removalListener(
                (key, value, cuase) -> {
                  if (value != null) {
                    value.clear();
                  }
                })
            .build(new OSFileCacheLoader());
  }

  @Override
  public InputStream getAsInputSteam(OSURI file, long startPosition) throws IOException {
    return null;
  }

  @Override
  public FileChannel getLocalCacheFileChannel(OSURI file, long startPosition) {
    // 根据 fileName 和 startPosition 计算出对应的本地文件路径，并返回对应的 FileChannel
    // 如果是使用一个 CacheFile, 则寻找到对应的位置，可能需要封装一个自己的 FileChannel 防止读多
    return null;
  }

  void put(OSFileCacheKey key, OSFileCacheValue value) {
    remotePos2LocalCacheFile.put(key, value);
  }

  class OSFileCacheLoader implements CacheLoader<OSFileCacheKey, OSFileCacheValue> {
    @Override
    public @Nullable OSFileCacheValue load(@NonNull OSFileCacheKey key) throws Exception {
      byte[] data =
          connector.getRemoteFile(key.getFile().toOSURI(), key.getStartPosition(), key.getLength());
      return cacheFileManager.persist(key, data);
    }
  }

  public static OSFileCache getInstance() {
    return OSFileCache.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final OSFileCache INSTANCE = new OSFileCache();
  }
}
