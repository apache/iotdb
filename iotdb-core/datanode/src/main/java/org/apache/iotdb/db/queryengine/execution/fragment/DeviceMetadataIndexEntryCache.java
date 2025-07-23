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

import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DeviceMetadataIndexEntryCache {
  private TreeMap<IDeviceID, Integer> deviceIndexMap;
  private final Map<String, long[]> deviceMetadataIndexNodeOffsetsCache = new HashMap<>();
  private List<IDeviceID> sortedDevices;
  private int[] deviceIdxArr;

  public void addDevices(AbstractDataSourceOperator operator, List<DeviceEntry> deviceEntries) {
    deviceIndexMap = deviceIndexMap == null ? new TreeMap<>(IDeviceID::compareTo) : deviceIndexMap;
    int[] operatorDeviceIndexArr = new int[deviceEntries.size()];
    for (int i = 0; i < deviceEntries.size(); i++) {
      int idx =
          deviceIndexMap.computeIfAbsent(
              deviceEntries.get(i).getDeviceID(), k -> deviceIndexMap.size());
      operatorDeviceIndexArr[i] = idx;
    }
    operator.setDeviceIndexArr(operatorDeviceIndexArr);
  }

  public void addDevice(AbstractDataSourceOperator operator, IDeviceID deviceID) {
    deviceIndexMap = deviceIndexMap == null ? new TreeMap<>() : deviceIndexMap;
    int idx = deviceIndexMap.computeIfAbsent(deviceID, k -> deviceIndexMap.size());
    operator.setDeviceIndexArr(new int[] {idx});
  }

  public Pair<long[], Boolean> getCachedDeviceMetadataIndexNodeOffset(
      int deviceIndex, String filePath) throws IOException {
    // cache is disabled
    if (deviceIndex < 0) {
      return new Pair<>(null, true);
    }
    // not in cache
    long[] resourceCache = loadOffsetsToCache(filePath);
    if (resourceCache == null) {
      return new Pair<>(null, true);
    }
    int indexAfterSort = deviceIdxArr[deviceIndex];
    long startOffset = resourceCache[2 * indexAfterSort];
    // the device does not exist in the file
    if (startOffset < 0) {
      return new Pair<>(null, false);
    }
    long endOffset = resourceCache[2 * indexAfterSort + 1];
    return new Pair<>(new long[] {startOffset, endOffset}, true);
  }

  private long[] loadOffsetsToCache(String filePath) throws IOException {
    long[] offsets = deviceMetadataIndexNodeOffsetsCache.get(filePath);
    if (offsets != null) {
      return offsets;
    }
    TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
    IDeviceID firstDevice = getSortedDevices().get(0);
    offsets =
        reader.getDeviceMetadataIndexNodeOffsets(
            firstDevice.isTableModel() ? firstDevice.getTableName() : "", sortedDevices, null);
    deviceMetadataIndexNodeOffsetsCache.put(filePath, offsets);
    return offsets;
  }

  private synchronized List<IDeviceID> getSortedDevices() {
    if (deviceIdxArr == null) {
      sort();
    }
    return sortedDevices;
  }

  private void sort() {
    deviceIdxArr = new int[deviceIndexMap.size()];
    sortedDevices = new ArrayList<>(deviceIndexMap.size());
    int i = 0;
    for (Map.Entry<IDeviceID, Integer> entry : deviceIndexMap.entrySet()) {
      sortedDevices.add(entry.getKey());
      deviceIdxArr[entry.getValue()] = i++;
    }
    deviceIndexMap = null;
  }
}
