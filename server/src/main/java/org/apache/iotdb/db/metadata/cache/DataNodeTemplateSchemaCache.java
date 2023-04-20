/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataNodeTemplateSchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeTemplateSchemaCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Cache<PartialPath, Integer> cache;

  // cache update due to activation or clear procedure
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  private DataNodeTemplateSchemaCache() {
    // TODO proprietary config parameter expected
    cache =
        Caffeine.newBuilder()
            .maximumWeight(config.getAllocateMemoryForSchemaCache())
            .weigher(
                (Weigher<PartialPath, Integer>) (key, val) -> (PartialPath.estimateSize(key) + 16))
            .build();
  }

  public static DataNodeTemplateSchemaCache getInstance() {
    return DataNodeTemplateSchemaCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeTemplateSchemaCacheHolder {
    private static final DataNodeTemplateSchemaCache INSTANCE = new DataNodeTemplateSchemaCache();
  }

  public void takeReadLock() {
    readWriteLock.readLock().lock();
  }

  public void releaseReadLock() {
    readWriteLock.readLock().unlock();
  }

  public void takeWriteLock() {
    readWriteLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    readWriteLock.writeLock().unlock();
  }

  public Integer get(PartialPath path) {
    takeReadLock();
    try {
      return cache.getIfPresent(path);
    } finally {
      releaseReadLock();
    }
  }

  public void put(PartialPath path, Integer id) {
    cache.put(path, id);
  }

  public void invalidateCache() {
    cache.invalidateAll();
  }
}
