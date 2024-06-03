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
package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema;

import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATER_NO_REGEX;

public class DataNodeTTLCache {
  private final TTLCache ttlCache;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private DataNodeTTLCache() {
    ttlCache = new TTLCache();
  }

  public static DataNodeTTLCache getInstance() {
    return DataNodeTTLCacheHolder.INSTANCE;
  }

  private static class DataNodeTTLCacheHolder {
    private static final DataNodeTTLCache INSTANCE = new DataNodeTTLCache();
  }

  public void setTTL(String path, long ttl) {
    lock.writeLock().lock();
    try {
      ttlCache.setTTL(path.split(PATH_SEPARATER_NO_REGEX), ttl);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setTTL(Map<String, Long> pathTTLs) {
    lock.writeLock().lock();
    try {
      pathTTLs.forEach((k, v) -> ttlCache.setTTL(k.split(PATH_SEPARATER_NO_REGEX), v));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void unsetTTL(String path) {
    lock.writeLock().lock();
    try {
      ttlCache.unsetTTL(path.split(PATH_SEPARATER_NO_REGEX));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public long getTTL(String[] path) {
    lock.readLock().lock();
    try {
      return ttlCache.getClosestTTL(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  public long getTTL(IDeviceID deviceID) {
    lock.readLock().lock();
    try {
      // TODO Tien change this way
      return ttlCache.getClosestTTL(deviceID.toString().split(PATH_SEPARATER_NO_REGEX));
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Get all ttl map under path node. */
  public Map<String, Long> getTTLUnderOneNode(String path) {
    lock.readLock().lock();
    try {
      return ttlCache.getAllTTLUnderOneNode(path.split(PATH_SEPARATER_NO_REGEX));
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Get ttl of one specific path node. If this node does not set ttl, then return -1. */
  public long getNodeTTL(String path) {
    lock.readLock().lock();
    try {
      return ttlCache.getLastNodeTTL(path.split(PATH_SEPARATER_NO_REGEX));
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Clear all ttl of cache. */
  @TestOnly
  public void clearAllTTL() {
    lock.writeLock().lock();
    try {
      ttlCache.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
