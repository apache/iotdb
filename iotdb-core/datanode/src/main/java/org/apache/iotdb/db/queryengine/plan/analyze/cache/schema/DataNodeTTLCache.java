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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataNodeTTLCache {
  private final TTLCache treeModelTTLCache;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private DataNodeTTLCache() {
    treeModelTTLCache = new TTLCache();
  }

  public static DataNodeTTLCache getInstance() {
    return DataNodeTTLCacheHolder.INSTANCE;
  }

  private static class DataNodeTTLCacheHolder {
    private static final DataNodeTTLCache INSTANCE = new DataNodeTTLCache();
  }

  @TestOnly
  public void setTTLForTree(String path, long ttl) throws IllegalPathException {
    lock.writeLock().lock();
    try {
      treeModelTTLCache.setTTL(PathUtils.splitPathToDetachedNodes(path), ttl);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setTTLForTree(String[] path, long ttl) {
    lock.writeLock().lock();
    try {
      treeModelTTLCache.setTTL(path, ttl);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void unsetTTLForTree(String[] path) {
    lock.writeLock().lock();
    try {
      treeModelTTLCache.unsetTTL(path);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public long getTTLForTree(IDeviceID deviceID) {
    try {
      return getTTLForTree(PathUtils.splitPathToDetachedNodes(deviceID.toString()));
    } catch (IllegalPathException e) {
      return Long.MAX_VALUE;
    }
  }

  public long getTTLForTable(final String database, final String table) {
    final TsTable tsTable = DataNodeTableCache.getInstance().getTable(database, table);
    return tsTable == null ? Long.MAX_VALUE : tsTable.getTableTTL();
  }

  /** Get ttl with time precision conversion. */
  public long getTTLForTree(String[] path) {
    long ttl = getTTLInMSForTree(path);
    return ttl == Long.MAX_VALUE
        ? ttl
        : CommonDateTimeUtils.convertMilliTimeWithPrecision(
            ttl, CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  /** Get ttl without time precision conversion. */
  public long getTTLInMSForTree(String[] path) {
    lock.readLock().lock();
    try {
      return treeModelTTLCache.getClosestTTL(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get ttl of one specific path node without time precision conversion. If this node does not set
   * ttl, then return -1.
   */
  public long getNodeTTLInMSForTree(String path) throws IllegalPathException {
    lock.readLock().lock();
    try {
      return treeModelTTLCache.getLastNodeTTL(PathUtils.splitPathToDetachedNodes(path));
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Clear all ttl of cache. */
  @TestOnly
  public void clearAllTTLForTree() {
    lock.writeLock().lock();
    try {
      treeModelTTLCache.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
