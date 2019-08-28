/**
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
package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to cache <code>TsFileMetaData</code> of tsfile in IoTDB.
 */
public class TsFileMetaDataCache {

  private static final Logger logger = LoggerFactory.getLogger(TsFileMetaDataCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static boolean cacheEnable = config.isMetaDataCacheEnable();
  private static final long MEMORY_THRESHOLD_IN_B = config.getAllocateMemoryForFileMetaDataCache();
  /**
   * key: Tsfile path. value: TsFileMetaData
   */
  private LRULinkedHashMap<TsFileResource, TsFileMetaData> cache;
  private AtomicLong cacheHitNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  /**
   * estimated size of a deviceIndexMap entry in TsFileMetaData.
   */
  private long deviceIndexMapEntrySize = 0;
  /**
   * estimated size of measurementSchema entry in TsFileMetaData.
   */
  private long measurementSchemaEntrySize = 0;
  /**
   * estimated size of version and CreateBy in TsFileMetaData.
   */
  private long versionAndCreatebySize = 10;

  private TsFileMetaDataCache() {
    if (!cacheEnable) {
      return;
    }
    cache = new LRULinkedHashMap<TsFileResource, TsFileMetaData>(MEMORY_THRESHOLD_IN_B, true) {
      @Override
      protected long calEntrySize(TsFileResource key, TsFileMetaData value) {
        if (deviceIndexMapEntrySize == 0 && value.getDeviceMap().size() > 0) {
          deviceIndexMapEntrySize = RamUsageEstimator
              .sizeOf(value.getDeviceMap().entrySet().iterator().next());
        }
        if (measurementSchemaEntrySize == 0 && value.getMeasurementSchema().size() > 0) {
          measurementSchemaEntrySize = RamUsageEstimator
              .sizeOf(value.getMeasurementSchema().entrySet().iterator().next());
        }
        long valueSize = value.getDeviceMap().size() * deviceIndexMapEntrySize
            + measurementSchemaEntrySize * value.getMeasurementSchema().size()
            + versionAndCreatebySize;
        return key.getFile().getPath().length() * 2 + valueSize;
      }
    };
  }

  public static TsFileMetaDataCache getInstance() {
    return TsFileMetaDataCacheHolder.INSTANCE;
  }

  /**
   * get the TsFileMetaData for given TsFile.
   *
   * @param tsFileResource -given TsFile
   */
  public TsFileMetaData get(TsFileResource tsFileResource) throws IOException {
    if (!cacheEnable) {
      return TsFileMetadataUtils.getTsFileMetaData(tsFileResource);
    }

    String path = tsFileResource.getFile().getPath();
    Object internPath = path.intern();
    cacheRequestNum.incrementAndGet();
    synchronized (cache) {
      if (cache.containsKey(path)) {
        cacheHitNum.incrementAndGet();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Cache hit: the number of requests for cache is {}, "
                  + "the number of hints for cache is {}",
              cacheRequestNum.get(), cacheHitNum.get());
        }
        return cache.get(path);
      }
    }
    synchronized (internPath) {
      synchronized (cache) {
        if (cache.containsKey(path)) {
          cacheHitNum.incrementAndGet();
          return cache.get(path);
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Cache didn't hit: the number of requests for cache is {}",
            cacheRequestNum.get());
      }
      TsFileMetaData fileMetaData = TsFileMetadataUtils.getTsFileMetaData(tsFileResource);
      synchronized (cache) {
        cache.put(tsFileResource, fileMetaData);
        return fileMetaData;
      }
    }
  }

  public void remove(TsFileResource resource) {
    synchronized (cache) {
      if (cache != null) {
        cache.remove(resource);
      }
    }
  }

  public void clear() {
    synchronized (cache) {
      if (cache != null) {
        cache.clear();
      }
    }
  }

  /**
   * Singleton pattern
   */
  private static class TsFileMetaDataCacheHolder {

    private TsFileMetaDataCacheHolder() {
    }

    private static final TsFileMetaDataCache INSTANCE = new TsFileMetaDataCache();
  }
}
