/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to cache <code>TsFileMetaData</code> of tsfile in IoTDB.
 *
 * @author liukun
 */
public class TsFileMetaDataCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetaDataCache.class);
  /**
   * key: The file seriesPath of tsfile.
   */
  private ConcurrentHashMap<String, TsFileMetaData> cache;
  private AtomicLong cacheHintNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  private TsFileMetaDataCache() {
    cache = new ConcurrentHashMap<>();
  }

  public static TsFileMetaDataCache getInstance() {
    return TsFileMetaDataCacheHolder.INSTANCE;
  }

  /**
   * get the TsFileMetaData for the given path.
   *
   * @param path -given path
   */
  public TsFileMetaData get(String path) throws IOException {

    path = path.intern();
    synchronized (path) {
      cacheRequestNum.incrementAndGet();
      if (!cache.containsKey(path)) {
        // read value from tsfile
        TsFileMetaData fileMetaData = TsFileMetadataUtils.getTsFileMetaData(path);
        cache.put(path, fileMetaData);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Cache didn't hint: the number of requests for cache is {}",
              cacheRequestNum.get());
        }
        return cache.get(path);
      } else {
        cacheHintNum.incrementAndGet();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Cache hint: the number of requests for cache is {}, the number of hints for cache "
                  + "is {}",
              cacheRequestNum.get(), cacheHintNum.get());
        }
        return cache.get(path);
      }
    }
  }

  public void remove(String path) {
    cache.remove(path);
  }

  public void clear() {
    cache.clear();
  }

  /*
   * Singleton pattern
   */
  private static class TsFileMetaDataCacheHolder {

    private static final TsFileMetaDataCache INSTANCE = new TsFileMetaDataCache();
  }
}
