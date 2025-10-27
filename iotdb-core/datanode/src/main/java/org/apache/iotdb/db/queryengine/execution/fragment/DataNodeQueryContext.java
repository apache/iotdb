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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;

public class DataNodeQueryContext {
  // Used for TreeModel, left of Pair is DataNodeSeriesScanNum, right of Pair is the last value
  // waiting to be updated
  @GuardedBy("lock")
  private final Map<PartialPath, Pair<AtomicInteger, TimeValuePair>> uncachedPathToSeriesScanInfo;

  // Used for TableModel
  // 1. Outer Map: record the info for each Table to make sure DeviceEntry is unique in the value
  // Scope.
  // 2. Inner Map: record DeviceEntry to last cache for each measurement, left of Pair is the
  // count of device regions, right is the measurement values wait to be updated for last cache.
  // Notice: only the device counts more than one will be recorded
  @GuardedBy("lock")
  private final Map<
          QualifiedObjectName, Map<DeviceEntry, Pair<Integer, Map<String, TimeValuePair>>>>
      deviceCountAndMeasurementValues;

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private final AtomicInteger dataNodeFINum;

  // TODO consider more fine-grained locks, now the AtomicInteger in uncachedPathToSeriesScanInfo is
  // unnecessary
  private final ReentrantLock lock = new ReentrantLock();

  public DataNodeQueryContext(int dataNodeFINum) {
    this.uncachedPathToSeriesScanInfo = new ConcurrentHashMap<>();
    this.dataNodeFINum = new AtomicInteger(dataNodeFINum);
    this.deviceCountAndMeasurementValues = new HashMap<>();
  }

  public boolean unCached(PartialPath path) {
    return uncachedPathToSeriesScanInfo.containsKey(path);
  }

  public void addUnCachePath(PartialPath path, AtomicInteger dataNodeSeriesScanNum) {
    uncachedPathToSeriesScanInfo.put(path, new Pair<>(dataNodeSeriesScanNum, null));
  }

  public void decreaseDeviceAndMayUpdateLastCache(
      QualifiedObjectName tableName, DeviceEntry deviceEntry, Integer initialCount) {
    checkArgument(initialCount != null, "initialCount shouldn't be null here");

    Map<DeviceEntry, Pair<Integer, Map<String, TimeValuePair>>> deviceInfo =
        deviceCountAndMeasurementValues.computeIfAbsent(tableName, t -> new HashMap<>());

    Pair<Integer, Map<String, TimeValuePair>> info =
        deviceInfo.computeIfAbsent(deviceEntry, d -> new Pair<>(initialCount, new HashMap<>()));
    info.left--;
    if (info.left == 0) {
      updateLastCache(tableName, deviceEntry);
    }
  }

  public void addUnCachedDeviceIfAbsent(
      QualifiedObjectName tableName, DeviceEntry deviceEntry, Integer count) {
    checkArgument(count != null, "count shouldn't be null here");

    Map<DeviceEntry, Pair<Integer, Map<String, TimeValuePair>>> deviceInfo =
        deviceCountAndMeasurementValues.computeIfAbsent(tableName, t -> new HashMap<>());

    deviceInfo.putIfAbsent(deviceEntry, new Pair<>(count, new HashMap<>()));
  }

  public Pair<Integer, Map<String, TimeValuePair>> getDeviceInfo(
      QualifiedObjectName tableName, DeviceEntry deviceEntry) {
    return deviceCountAndMeasurementValues.get(tableName).get(deviceEntry);
  }

  /** Update the last cache when device count decrease to zero. */
  public void updateLastCache(QualifiedObjectName tableName, DeviceEntry deviceEntry) {
    Map<String, TimeValuePair> values =
        deviceCountAndMeasurementValues.get(tableName).get(deviceEntry).getRight();
    // if a device hits cache each time, the values recorded in context will be null
    if (values != null) {
      TABLE_DEVICE_SCHEMA_CACHE.updateLastCacheIfExists(
          tableName.getDatabaseName(),
          deviceEntry.getDeviceID(),
          values.keySet().toArray(new String[0]),
          values.values().toArray(new TimeValuePair[0]));
    }
  }

  public Pair<AtomicInteger, TimeValuePair> getSeriesScanInfo(PartialPath path) {
    return uncachedPathToSeriesScanInfo.get(path);
  }

  public Map<PartialPath, Pair<AtomicInteger, TimeValuePair>> getUncachedPathToSeriesScanInfo() {
    return uncachedPathToSeriesScanInfo;
  }

  public int decreaseDataNodeFINum() {
    return dataNodeFINum.decrementAndGet();
  }

  public void lock(boolean isDeviceInMultiRegion) {
    // When a device exists in only one region, there will be no intermediate state.
    if (isDeviceInMultiRegion) {
      lock.lock();
    }
  }

  public void unLock(boolean isDeviceInMultiRegion) {
    if (isDeviceInMultiRegion) {
      lock.unlock();
    }
  }
}
