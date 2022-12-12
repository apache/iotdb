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

package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** This class is used to cache Altering Timeseries. */
public class AlteringRecordsCache {

  private static final Logger logger = LoggerFactory.getLogger(AlteringRecordsCache.class);

  // measurement->(encoding, compression)
  private final Map<String, Pair<TSEncoding, CompressionType>> alteringRecords =
      new ConcurrentHashMap<>(32);
  // device->measurement->(encoding, compression)
  private final Map<String, Map<String, Pair<TSEncoding, CompressionType>>> alteringDeviceRecords =
      new ConcurrentHashMap<>(32);
  // sg->deviceIds
  private final Map<String, Set<String>> sgDeviceMap = new ConcurrentHashMap<>(2);

  private final AtomicBoolean isAltering = new AtomicBoolean(false);

  private AlteringRecordsCache() {}

  public synchronized void startAlter() {
    isAltering.set(true);
  }

  public synchronized void putRecord(
      String fullPath, TSEncoding encoding, CompressionType compressionType) throws Exception {
    if (fullPath != null) {
      PartialPath path = new PartialPath(fullPath);
      String storageGroupName = StorageEngine.getInstance().getStorageGroupName(path);
      putRecord(storageGroupName, fullPath, encoding, compressionType);
    }
  }

  /**
   * only for recover
   *
   * @param storageGroupName
   * @param fullPath
   * @param encoding
   * @param compressionType
   * @throws Exception
   */
  public synchronized void putRecord(
      String storageGroupName,
      String fullPath,
      TSEncoding encoding,
      CompressionType compressionType)
      throws Exception {
    if (fullPath != null) {
      PartialPath path = new PartialPath(fullPath);
      String device = path.getDevice();
      Set<String> devices =
          sgDeviceMap.computeIfAbsent(
              storageGroupName, id -> Collections.synchronizedSet(new HashSet<>()));
      devices.add(device);
      Pair<TSEncoding, CompressionType> record = new Pair<>(encoding, compressionType);
      alteringRecords.put(fullPath, record);
      Map<String, Pair<TSEncoding, CompressionType>> deviceRecordMap =
          alteringDeviceRecords.computeIfAbsent(device, id -> new ConcurrentHashMap<>());
      deviceRecordMap.put(path.getMeasurement(), record);
    }
  }

  public Set<String> getDevicesCache(String storageGroupName) {
    return sgDeviceMap.get(storageGroupName);
  }

  public Pair<TSEncoding, CompressionType> getRecord(String fullPath) {

    if (!isAltering.get()) {
      return null;
    }
    return alteringRecords.get(fullPath);
  }

  public Map<String, Pair<TSEncoding, CompressionType>> getDeviceRecords(String device) {
    return alteringDeviceRecords.get(device);
  }

  public boolean isStorageGroupExsist(String storageGroupName) {

    if (storageGroupName == null) {
      return false;
    }
    return sgDeviceMap.get(storageGroupName) != null;
  }

  public static AlteringRecordsCache getInstance() {
    return AlteringRecordsCacheHolder.INSTANCE;
  }

  public synchronized void clear(String storageGroupName) {

    if (!isStorageGroupExsist(storageGroupName)) {
      return;
    }
    Set<String> devices = sgDeviceMap.get(storageGroupName);
    if (devices != null) {
      sgDeviceMap.remove(storageGroupName);
      devices.forEach(
          device -> {
            Map<String, Pair<TSEncoding, CompressionType>> deviceMap =
                alteringDeviceRecords.get(device);
            if (deviceMap != null) {
              alteringDeviceRecords.remove(device);
              deviceMap.forEach(
                  (k, v) -> {
                    try {
                      PartialPath fullPath = new PartialPath(device, k);
                      alteringRecords.remove(fullPath);
                    } catch (IllegalPathException e) {
                      logger.error("fullPath error!!!!!", e);
                    }
                  });
            }
          });
    }
    if (sgDeviceMap.isEmpty()) {
      isAltering.set(false);
    }
  }

  @TestOnly
  public boolean isAltering() {
    return isAltering.get();
  }

  /** singleton pattern. */
  private static class AlteringRecordsCacheHolder {

    private static final AlteringRecordsCache INSTANCE = new AlteringRecordsCache();
  }
}
