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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.apache.commons.collections4.map.LRUMap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Estimate the memory cost of one compaction task with specific source files based on its
 * corresponding implementation.
 */
public abstract class AbstractCompactionEstimator {

  private static final Map<File, FileInfo> globalFileInfoCacheForFailedCompaction =
      Collections.synchronizedMap(
          new LRUMap<>(
              IoTDBDescriptor.getInstance().getConfig().getGlobalCompactionFileInfoCacheSize()));
  protected Map<TsFileResource, FileInfo> fileInfoCache = new HashMap<>();
  protected Map<TsFileResource, DeviceTimeIndex> deviceTimeIndexCache = new HashMap<>();

  protected IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  protected long compressionRatio = (long) CompressionRatio.getInstance().getRatio() + 1;

  protected abstract long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo);

  protected abstract long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) throws IOException;

  protected CompactionTaskInfo calculatingCompactionTaskInfo(List<TsFileResource> resources)
      throws IOException {
    List<FileInfo> fileInfoList = new ArrayList<>();
    for (TsFileResource resource : resources) {
      FileInfo fileInfo = getFileInfoFromCache(resource);
      fileInfoList.add(fileInfo);
    }
    return new CompactionTaskInfo(resources, fileInfoList);
  }

  private FileInfo getFileInfoFromCache(TsFileResource resource) throws IOException {
    if (fileInfoCache.containsKey(resource)) {
      return fileInfoCache.get(resource);
    }
    File file = new File(resource.getTsFilePath());
    if (globalFileInfoCacheForFailedCompaction.containsKey(file)) {
      FileInfo fileInfo = globalFileInfoCacheForFailedCompaction.get(file);
      fileInfoCache.put(resource, fileInfo);
      return fileInfo;
    }
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(resource.getTsFilePath(), true, false)) {
      FileInfo fileInfo = CompactionEstimateUtils.calculateFileInfo(reader);
      fileInfoCache.put(resource, fileInfo);
      globalFileInfoCacheForFailedCompaction.put(file, fileInfo);
      return fileInfo;
    }
  }

  protected int calculatingMaxOverlapFileNumInSubCompactionTask(List<TsFileResource> resources)
      throws IOException {
    Set<String> devices = new HashSet<>();
    List<DeviceTimeIndex> resourceDevices = new ArrayList<>(resources.size());
    for (TsFileResource resource : resources) {
      DeviceTimeIndex deviceTimeIndex = getDeviceTimeIndexFromCache(resource);
      devices.addAll(deviceTimeIndex.getDevices());
      resourceDevices.add(deviceTimeIndex);
    }
    int maxOverlapFileNumInSubCompactionTask = 1;
    for (String device : devices) {
      List<DeviceTimeIndex> resourcesContainsCurrentDevice =
          resourceDevices.stream()
              .filter(resource -> !resource.definitelyNotContains(device))
              .sorted(Comparator.comparingLong(resource -> resource.getStartTime(device)))
              .collect(Collectors.toList());
      if (resourcesContainsCurrentDevice.size() < maxOverlapFileNumInSubCompactionTask) {
        continue;
      }

      long maxEndTimeOfCurrentDevice = Long.MIN_VALUE;
      int overlapFileNumOfCurrentDevice = 0;
      for (DeviceTimeIndex resource : resourcesContainsCurrentDevice) {
        long deviceStartTimeInCurrentFile = resource.getStartTime(device);
        long deviceEndTimeInCurrentFile = resource.getEndTime(device);
        if (deviceStartTimeInCurrentFile <= maxEndTimeOfCurrentDevice) {
          // has overlap, update max end time
          maxEndTimeOfCurrentDevice =
              Math.max(maxEndTimeOfCurrentDevice, deviceEndTimeInCurrentFile);
          overlapFileNumOfCurrentDevice++;
          maxOverlapFileNumInSubCompactionTask =
              Math.max(maxOverlapFileNumInSubCompactionTask, overlapFileNumOfCurrentDevice);
        } else {
          // reset max end time and overlap file num of current device
          maxEndTimeOfCurrentDevice = deviceEndTimeInCurrentFile;
          overlapFileNumOfCurrentDevice = 1;
        }
      }
      // already reach the max value
      if (maxOverlapFileNumInSubCompactionTask == resources.size()) {
        return maxOverlapFileNumInSubCompactionTask;
      }
    }
    return maxOverlapFileNumInSubCompactionTask;
  }

  private DeviceTimeIndex getDeviceTimeIndexFromCache(TsFileResource resource) throws IOException {
    if (deviceTimeIndexCache.containsKey(resource)) {
      return deviceTimeIndexCache.get(resource);
    }
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof FileTimeIndex) {
      timeIndex = resource.buildDeviceTimeIndex();
    }
    deviceTimeIndexCache.put(resource, (DeviceTimeIndex) timeIndex);
    return (DeviceTimeIndex) timeIndex;
  }

  public void cleanup() {
    deviceTimeIndexCache.clear();
    fileInfoCache.clear();
  }

  public static void removeFileInfoFromGlobalFileInfoCache(TsFileResource resource) {
    if (resource == null || resource.getTsFile() == null) {
      return;
    }
    globalFileInfoCacheForFailedCompaction.remove(resource.getTsFile());
  }
}
