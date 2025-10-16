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

package org.apache.iotdb.metrics.metricsets.disk;

import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;
import oshi.software.os.OSProcess;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Disk Metrics Manager for Windows system, not implemented yet. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class WindowsDiskMetricsManager extends AbstractDiskMetricsManager {

  private final SystemInfo systemInfo = new SystemInfo();
  private final OSProcess thisProcess;
  private List<HWDiskStore> diskStores;

  public WindowsDiskMetricsManager() {
    thisProcess = systemInfo.getOperatingSystem().getCurrentProcess();
    diskStores = systemInfo.getHardware().getDiskStores();
    init();
  }

  @Override
  public Map<String, Double> getReadDataSizeForDisk() {
    checkUpdate();
    Map<String, Double> result = new HashMap<>();
    diskStores.forEach(
        disk -> {
          result.put(this.getDisplayName(disk), (double) disk.getReadBytes() / BYTES_PER_KB);
        });
    return result;
  }

  @Override
  public Map<String, Double> getWriteDataSizeForDisk() {
    checkUpdate();
    Map<String, Double> result = new HashMap<>();
    diskStores.forEach(
        disk -> {
          result.put(this.getDisplayName(disk), (double) disk.getWriteBytes() / BYTES_PER_KB);
        });
    return result;
  }

  @Override
  public Map<String, Long> getReadOperationCountForDisk() {
    checkUpdate();
    Map<String, Long> result = new HashMap<>();
    diskStores.forEach(
        disk -> {
          result.put(this.getDisplayName(disk), disk.getReads());
        });
    return result;
  }

  @Override
  public Map<String, Long> getWriteOperationCountForDisk() {
    checkUpdate();
    Map<String, Long> result = new HashMap<>();
    diskStores.forEach(
        disk -> {
          result.put(this.getDisplayName(disk), disk.getWrites());
        });
    return result;
  }

  private Map<String, Long> getTransferTimesForDisk() {
    checkUpdate();
    Map<String, Long> result = new HashMap<>();
    diskStores.forEach(
        disk -> {
          result.put(this.getDisplayName(disk), disk.getTransferTime());
        });
    return result;
  }

  @Override
  public Map<String, Double> getQueueSizeForDisk() {
    checkUpdate();
    Map<String, Double> result = new HashMap<>();
    diskStores.forEach(
        disk -> {
          result.put(this.getDisplayName(disk), (double) disk.getCurrentQueueLength());
        });
    return result;
  }

  @Override
  public double getActualReadDataSizeForProcess() {
    return thisProcess.getBytesRead() / BYTES_PER_KB;
  }

  @Override
  public double getActualWriteDataSizeForProcess() {
    return thisProcess.getBytesWritten() / BYTES_PER_KB;
  }

  @Override
  public Map<String, Double> getAvgSizeOfEachReadForDisk() {
    checkUpdate();
    Map<String, Double> result = new HashMap<>(incrementReadSizeForDisk.size());
    for (Map.Entry<String, Long> incrementReadSize : incrementReadSizeForDisk.entrySet()) {
      // use Long.max to avoid NaN
      long readOpsCount =
          Long.max(
              incrementReadOperationCountForDisk.getOrDefault(incrementReadSize.getKey(), 1L), 1L);
      result.put(
          incrementReadSize.getKey(), ((double) incrementReadSize.getValue()) / readOpsCount);
    }
    return result;
  }

  @Override
  public Map<String, Double> getAvgSizeOfEachWriteForDisk() {
    checkUpdate();
    Map<String, Double> result = new HashMap<>(incrementWriteSizeForDisk.size());
    for (Map.Entry<String, Long> incrementReadSize : incrementWriteSizeForDisk.entrySet()) {
      // use Long.max to avoid NaN
      long readOpsCount =
          Long.max(
              incrementWriteOperationCountForDisk.getOrDefault(incrementReadSize.getKey(), 1L), 1L);
      result.put(
          incrementReadSize.getKey(), ((double) incrementReadSize.getValue()) / readOpsCount);
    }
    return result;
  }

  protected void updateInfo() {
    super.updateInfo();
    updateDiskInfo();
  }

  private void updateDiskInfo() {
    diskStores = systemInfo.getHardware().getDiskStores();

    Map[] currentMapArray = {
      getTransferTimesForDisk(), getReadDataSizeForDisk(), getWriteDataSizeForDisk(),
    };
    Map[] lastMapArray = {
      lastIoBusyTimeForDisk, lastReadSizeForDisk, lastWriteSizeForDisk,
    };
    Map[] incrementMapArray = {
      incrementIoBusyTimeForDisk, incrementReadSizeForDisk, incrementWriteSizeForDisk,
    };

    for (int i = 0; i < currentMapArray.length; i++) {
      Map map = currentMapArray[i];
      int finalI = i;
      map.forEach(
          (key, value) -> {
            updateSingleDiskInfo(
                (String) key,
                ((Number) value).longValue(),
                lastMapArray[finalI],
                incrementMapArray[finalI]);
          });
    }
  }

  private String getDisplayName(HWDiskStore disk) {
    return disk.getName() + "-" + disk.getModel();
  }

  @Override
  protected void collectDiskId() {
    diskIdSet = diskStores.stream().map(this::getDisplayName).collect(Collectors.toSet());
  }
}
