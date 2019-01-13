/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;

/**
 * This class is used to cache <code>RowGroupBlockMetaDataCache</code> of tsfile in IoTDB.
 * 
 * @author liukun
 *
 */
public class RowGroupBlockMetaDataCache {

    private static Logger LOGGER = LoggerFactory.getLogger(RowGroupBlockMetaDataCache.class);
    private static final int cacheSize = 100;

    /** key: the file path + deviceId */
    private LinkedHashMap<String, TsDeviceMetadata> LRUCache;

    private AtomicLong cacheHintNum = new AtomicLong();
    private AtomicLong cacheRequestNum = new AtomicLong();

    /**
     * This class is a map used to cache the <code>RowGroupBlockMetaData</code>. The caching strategy is LRU.
     * 
     * @author liukun
     *
     */
    private class LRULinkedHashMap extends LinkedHashMap<String, TsDeviceMetadata> {

        private static final long serialVersionUID = 1290160928914532649L;
        private static final float loadFactor = 0.75f;
        private int maxCapacity;

        public LRULinkedHashMap(int maxCapacity, boolean isLRU) {
            super(maxCapacity, loadFactor, isLRU);
            this.maxCapacity = maxCapacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, TsDeviceMetadata> eldest) {
            return size() > maxCapacity;
        }
    }

    /**
     * the default LRU cache size is 100. The singleton pattern.
     */
    private static class RowGroupBlockMetaDataCacheSingleton {
        private static final RowGroupBlockMetaDataCache INSTANCE = new RowGroupBlockMetaDataCache(cacheSize);
    }

    public static RowGroupBlockMetaDataCache getInstance() {
        return RowGroupBlockMetaDataCacheSingleton.INSTANCE;
    }

    private RowGroupBlockMetaDataCache(int cacheSize) {
        LRUCache = new LRULinkedHashMap(cacheSize, true);
    }

    public TsDeviceMetadata get(String filePath, String deviceId, TsFileMetaData fileMetaData) throws IOException {
        // The key(the tsfile path and deviceId) for the LRUCache

        String jointPath = filePath + deviceId;
        jointPath = jointPath.intern();
        synchronized (LRUCache) {
            cacheRequestNum.incrementAndGet();
            if (LRUCache.containsKey(jointPath)) {
                cacheHintNum.incrementAndGet();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "Cache hint: the number of requests for cache is {}, the number of hints for cache is {}",
                            cacheRequestNum.get(), cacheHintNum.get());
                }
                return LRUCache.get(jointPath);
            }
        }
        synchronized (jointPath) {
            synchronized (LRUCache) {
                if (LRUCache.containsKey(jointPath)) {
                    return LRUCache.get(jointPath);
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cache didn't hint: the number of requests for cache is {}", cacheRequestNum.get());
            }
            TsDeviceMetadata blockMetaData = TsFileMetadataUtils.getTsRowGroupBlockMetaData(filePath, deviceId,
                    fileMetaData);
            synchronized (LRUCache) {
                LRUCache.put(jointPath, blockMetaData);
                return LRUCache.get(jointPath);
            }
        }
    }

    public void clear() {
        synchronized (LRUCache) {
            LRUCache.clear();
        }
    }
}
