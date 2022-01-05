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

package org.apache.iotdb.db.metadata.idtable.entry;

import java.util.HashMap;
import java.util.Map;

/** device entry in id table */
public class DeviceEntry {
  /** for device ID reuse in memtable */
  IDeviceID deviceID;

  /** measurement schema map */
  Map<String, SchemaEntry> measurementMap;

  boolean isAligned;

  // for manages flush time
  Map<Long, Long> lastTimeMap;

  Map<Long, Long> flushTimeMap;

  long globalFlushTime = Long.MIN_VALUE;

  public DeviceEntry(IDeviceID deviceID) {
    this.deviceID = deviceID;
    measurementMap = new HashMap<>();
    lastTimeMap = new HashMap<>();
    flushTimeMap = new HashMap<>();
  }

  /**
   * get schema entry of the measurement
   *
   * @param measurementName name of the measurement
   * @return if exist, schema entry of the measurement. if not exist, null
   */
  public SchemaEntry getSchemaEntry(String measurementName) {
    return measurementMap.get(measurementName);
  }

  /**
   * put new schema entry of the measurement
   *
   * @param measurementName name of the measurement
   * @param schemaEntry schema entry of the measurement
   */
  public void putSchemaEntry(String measurementName, SchemaEntry schemaEntry) {
    measurementMap.put(measurementName, schemaEntry);
  }

  /**
   * whether the device entry contains the measurement
   *
   * @param measurementName name of the measurement
   * @return whether the device entry contains the measurement
   */
  public boolean contains(String measurementName) {
    return measurementMap.containsKey(measurementName);
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  // region support flush time
  public void putLastTimeMap(long timePartition, long lastTime) {
    lastTimeMap.put(timePartition, lastTime);
  }

  public void putFlushTimeMap(long timePartition, long flushTime) {
    flushTimeMap.put(timePartition, flushTime);
  }

  public long updateLastTimeMap(long timePartition, long lastTime) {
    return lastTimeMap.compute(
        timePartition, (k, v) -> v == null ? lastTime : Math.max(v, lastTime));
  }

  public long updateFlushTimeMap(long timePartition, long flushTime) {
    return flushTimeMap.compute(
        timePartition, (k, v) -> v == null ? flushTime : Math.max(v, flushTime));
  }

  public void updateGlobalFlushTime(long flushTime) {
    globalFlushTime = Math.max(globalFlushTime, flushTime);
  }

  public void setGlobalFlushTime(long globalFlushTime) {
    this.globalFlushTime = globalFlushTime;
  }

  public Long getLastTime(long timePartition) {
    return lastTimeMap.get(timePartition);
  }

  public Long getFlushTime(long timePartition) {
    return flushTimeMap.get(timePartition);
  }

  public Long getLastTimeWithDefaultValue(long timePartition) {
    return lastTimeMap.getOrDefault(timePartition, Long.MIN_VALUE);
  }

  public Long getFLushTimeWithDefaultValue(long timePartition) {
    return flushTimeMap.getOrDefault(timePartition, Long.MIN_VALUE);
  }

  public long getGlobalFlushTime() {
    return globalFlushTime;
  }

  public void clearLastTime() {
    lastTimeMap.clear();
  }

  public void clearFlushTime() {
    flushTimeMap.clear();
  }
  // endregion
}
