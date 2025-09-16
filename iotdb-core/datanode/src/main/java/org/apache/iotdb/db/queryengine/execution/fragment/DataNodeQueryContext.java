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

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;

import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DataNodeQueryContext {
  // Used for TreeModel, left of Pair is DataNodeSeriesScanNum, right of Pair is the last value
  // waiting to be updated
  @GuardedBy("lock")
  private final Map<PartialPath, Pair<AtomicInteger, TimeValuePair>> uncachedPathToSeriesScanInfo;

  // Used for TableModel
  // 1. Outer Map: record the info for each AggTableScanNode because of Join will produce the same
  // deviceEntry in different AggTableScan.
  // 2. Inner Map: record DeviceEntry to last cache info, left of Pair is the region number of
  // DeviceEntry, right is the last value waiting to be updated
  @GuardedBy("lock")
  private final Map<QualifiedObjectName, Map<DeviceEntry, Pair<Integer, TimeValuePair[]>>>
      uncachedDeviceToValuesInfo;

  private final AtomicInteger dataNodeFINum;

  // TODO consider more fine-grained locks, now the AtomicInteger in uncachedPathToSeriesScanInfo is
  // unnecessary
  private final ReentrantLock lock = new ReentrantLock();

  public DataNodeQueryContext(int dataNodeFINum) {
    this.uncachedPathToSeriesScanInfo = new ConcurrentHashMap<>();
    this.dataNodeFINum = new AtomicInteger(dataNodeFINum);
    this.uncachedDeviceToValuesInfo = new ConcurrentHashMap<>();
  }

  public boolean unCached(PartialPath path) {
    return uncachedPathToSeriesScanInfo.containsKey(path);
  }

  public void addUnCachePath(PartialPath path, AtomicInteger dataNodeSeriesScanNum) {
    uncachedPathToSeriesScanInfo.put(path, new Pair<>(dataNodeSeriesScanNum, null));
  }

  public void addUnCachedDeviceIfAbsent(
      QualifiedObjectName tableName, DeviceEntry deviceEntry, Integer regionNum) {
    Map<DeviceEntry, Pair<Integer, TimeValuePair[]>> uncachedDeviceToValues =
        uncachedDeviceToValuesInfo.computeIfAbsent(tableName, t -> new ConcurrentHashMap<>());
    uncachedDeviceToValues.putIfAbsent(deviceEntry, new Pair<>(regionNum, null));
  }

  public Pair<Integer, TimeValuePair[]> getDeviceValues(
      QualifiedObjectName tableName, DeviceEntry deviceEntry) {
    return uncachedDeviceToValuesInfo.get(tableName).get(deviceEntry);
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
