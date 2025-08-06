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

import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;

import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.query.DeviceMetadataIndexEntriesQueryResult;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

public class DeviceMetadataIndexEntryCache {
  private static final DataNodeMemoryConfig config =
      IoTDBDescriptor.getInstance().getMemoryConfig();
  private final FragmentInstanceContext context;
  private TreeMap<IDeviceID, Integer> deviceIndexMap;
  private final Map<String, DeviceMetadataIndexEntriesQueryResult>
      deviceMetadataIndexNodeOffsetsCache = new ConcurrentHashMap<>();
  private List<IDeviceID> sortedDevices;
  private int[] deviceIdxArr;
  private final AtomicLong reservedMemory;
  private final LongConsumer ioSizeRecorder;

  public DeviceMetadataIndexEntryCache(FragmentInstanceContext context) {
    this.context = context;
    this.reservedMemory = new AtomicLong(0);
    this.ioSizeRecorder =
        context.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet;
  }

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

  // Pair.right represents whether the device may exist in the file
  public Pair<long[], Boolean> getCachedDeviceMetadataIndexNodeOffset(
      IDeviceID device, int deviceIndex, String filePath, boolean ignoreNotExists)
      throws IOException {
    // cache is disabled
    if (deviceIndex < 0) {
      return new Pair<>(null, true);
    }
    if (deviceIndexMap != null && deviceIndexMap.size() == 1) {
      return new Pair<>(null, true);
    }
    // not cached
    DeviceMetadataIndexEntriesQueryResult resourceCache = loadOffsetsToCache(filePath);
    if (resourceCache == null) {
      return new Pair<>(null, true);
    }
    int indexAfterSort = deviceIdxArr[deviceIndex];
    long[] result = resourceCache.getDeviceMetadataIndexNodeOffset(indexAfterSort);
    // the device does not exist in the file
    if (result == null) {
      if (!ignoreNotExists) {
        throw new IOException("Device {" + device + "} is not in tsFileMetaData of " + filePath);
      }
      return new Pair<>(null, false);
    }
    return new Pair<>(result, true);
  }

  private DeviceMetadataIndexEntriesQueryResult loadOffsetsToCache(String filePath)
      throws IOException {
    TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
    IDeviceID firstDevice = getSortedDevices().get(0);
    return deviceMetadataIndexNodeOffsetsCache.computeIfAbsent(
        filePath,
        k -> {
          if (reservedMemory.get() >= config.getMaxCachedDeviceMetadataIndexEntryBytesPerFI()) {
            return null;
          }
          DeviceMetadataIndexEntriesQueryResult result;
          try {
            result =
                reader.getDeviceMetadataIndexNodeOffsets(
                    firstDevice.isTableModel() ? firstDevice.getTableName() : null,
                    sortedDevices,
                    ioSizeRecorder);
            long memCost = result.ramBytesUsed();
            context.getMemoryReservationContext().reserveMemoryCumulatively(memCost);
            reservedMemory.addAndGet(memCost);
          } catch (IOException e) {
            throw new TsFileRuntimeException(e);
          } catch (MemoryNotEnoughException ignored) {
            return null;
          }
          return result;
        });
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
