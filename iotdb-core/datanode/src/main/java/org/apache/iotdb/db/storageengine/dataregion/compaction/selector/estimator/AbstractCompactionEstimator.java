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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.BatchCompactionPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.external.commons.collections4.map.LRUMap;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;

import javax.annotation.Nullable;

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

  /** The size of global compaction estimation file info cahce. */
  private static int globalCompactionFileInfoCacheSize = 1000;

  /** The size of global compaction estimation rough file info cahce. */
  private static int globalCompactionRoughFileInfoCacheSize = 100000;

  private static final double maxRatioToAllocateFileInfoCache = 0.1;
  private static boolean globalFileInfoCacheEnabled;
  private static Map<TsFileID, FileInfo> globalFileInfoCacheForFailedCompaction;
  private static Map<TsFileID, FileInfo.RoughFileInfo> globalRoughInfoCacheForCompaction;

  protected IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static long allocateMemoryCostForFileInfoCache(long compactionMemorySize) {
    long fixedMemoryCost =
        globalCompactionFileInfoCacheSize * FileInfo.MEMORY_COST_OF_FILE_INFO_ENTRY_IN_CACHE
            + globalCompactionRoughFileInfoCacheSize
                * FileInfo.MEMORY_COST_OF_ROUGH_FILE_INFO_ENTRY_IN_CACHE;
    globalFileInfoCacheEnabled =
        compactionMemorySize * maxRatioToAllocateFileInfoCache > fixedMemoryCost;
    if (globalFileInfoCacheEnabled) {
      globalRoughInfoCacheForCompaction =
          Collections.synchronizedMap(new LRUMap<>(globalCompactionFileInfoCacheSize));
      globalFileInfoCacheForFailedCompaction =
          Collections.synchronizedMap(new LRUMap<>(globalCompactionRoughFileInfoCacheSize));
    } else {
      globalRoughInfoCacheForCompaction = Collections.emptyMap();
      globalFileInfoCacheForFailedCompaction = Collections.emptyMap();
    }
    return globalFileInfoCacheEnabled ? fixedMemoryCost : 0;
  }

  protected Map<TsFileResource, FileInfo> fileInfoCache = new HashMap<>();
  protected Map<TsFileResource, FileInfo.RoughFileInfo> roughInfoMap = new HashMap<>();
  protected Map<TsFileResource, ArrayDeviceTimeIndex> deviceTimeIndexCache = new HashMap<>();

  protected TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  protected long fixedMemoryBudget =
      (long)
              ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                  / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                  * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion())
          + BatchCompactionPlan.maxCachedTimeChunksSize;

  protected abstract long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo);

  protected abstract long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) throws IOException;

  protected abstract TsFileSequenceReader getReader(String filePath) throws IOException;

  protected boolean isAllSourceFileExist(List<TsFileResource> resources) {
    for (TsFileResource resource : resources) {
      if (resource.getStatus() == TsFileResourceStatus.DELETED) {
        return false;
      }
    }
    return true;
  }

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
    TsFileID tsFileID = resource.getTsFileID();
    synchronized (globalFileInfoCacheForFailedCompaction) {
      FileInfo fileInfo = globalFileInfoCacheForFailedCompaction.get(tsFileID);
      if (fileInfo != null) {
        fileInfoCache.put(resource, fileInfo);
        return fileInfo;
      }
    }
    try (TsFileSequenceReader reader = getReader(resource.getTsFilePath())) {
      FileInfo fileInfo = CompactionEstimateUtils.calculateFileInfo(reader);
      fileInfoCache.put(resource, fileInfo);
      if (globalFileInfoCacheEnabled) {
        synchronized (globalFileInfoCacheForFailedCompaction) {
          globalFileInfoCacheForFailedCompaction.put(tsFileID, fileInfo);
        }
        synchronized (globalRoughInfoCacheForCompaction) {
          globalRoughInfoCacheForCompaction.put(tsFileID, fileInfo.getSimpleFileInfo());
        }
      }
      return fileInfo;
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  protected int calculatingMaxOverlapFileNumInSubCompactionTask(
      @Nullable CompactionScheduleContext context, List<TsFileResource> resources)
      throws IOException {
    Set<IDeviceID> devices = new HashSet<>();
    List<ArrayDeviceTimeIndex> resourceDevices = new ArrayList<>(resources.size());
    for (TsFileResource resource : resources) {
      ArrayDeviceTimeIndex deviceTimeIndex = getDeviceTimeIndexFromCache(context, resource);
      devices.addAll(deviceTimeIndex.getDevices());
      resourceDevices.add(deviceTimeIndex);
    }
    int maxOverlapFileNumInSubCompactionTask = 1;
    for (IDeviceID device : devices) {
      @SuppressWarnings("OptionalGetWithoutIsPresent") // checked in filter
      List<ArrayDeviceTimeIndex> resourcesContainsCurrentDevice =
          resourceDevices.stream()
              .filter(resource -> !resource.definitelyNotContains(device))
              .sorted(Comparator.comparingLong(resource -> resource.getStartTime(device).get()))
              .collect(Collectors.toList());
      if (resourcesContainsCurrentDevice.size() < maxOverlapFileNumInSubCompactionTask) {
        continue;
      }

      long maxEndTimeOfCurrentDevice = Long.MIN_VALUE;
      int overlapFileNumOfCurrentDevice = 0;
      for (ArrayDeviceTimeIndex resource : resourcesContainsCurrentDevice) {
        // checked by Stream.filter()
        long deviceStartTimeInCurrentFile = resource.getStartTime(device).get();
        long deviceEndTimeInCurrentFile = resource.getEndTime(device).get();
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

  private ArrayDeviceTimeIndex getDeviceTimeIndexFromCache(
      @Nullable CompactionScheduleContext context, TsFileResource resource) throws IOException {
    if (deviceTimeIndexCache.containsKey(resource)) {
      return deviceTimeIndexCache.get(resource);
    }
    if (context != null) {
      ArrayDeviceTimeIndex timeIndex = context.getResourceDeviceInfo(resource);
      if (timeIndex != null) {
        deviceTimeIndexCache.put(resource, timeIndex);
        return timeIndex;
      }
    }
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof FileTimeIndex) {
      timeIndex = CompactionUtils.buildDeviceTimeIndex(resource);
    }
    deviceTimeIndexCache.put(resource, (ArrayDeviceTimeIndex) timeIndex);
    return (ArrayDeviceTimeIndex) timeIndex;
  }

  public void cleanup() {
    deviceTimeIndexCache.clear();
    fileInfoCache.clear();
  }

  public boolean hasCachedRoughFileInfo(TsFileResource resource) {
    return getRoughFileInfo(resource) != null;
  }

  public FileInfo.RoughFileInfo getRoughFileInfo(TsFileResource resource) {
    FileInfo.RoughFileInfo roughFileInfo = roughInfoMap.get(resource);
    if (roughFileInfo != null) {
      return roughFileInfo;
    }
    synchronized (globalRoughInfoCacheForCompaction) {
      roughFileInfo = globalRoughInfoCacheForCompaction.get(resource.getTsFileID());
    }
    if (roughFileInfo != null) {
      roughInfoMap.put(resource, roughFileInfo);
    }
    return roughFileInfo;
  }

  public boolean supportsRoughEstimation() {
    return true;
  }

  public static void removeFileInfoFromGlobalFileInfoCache(TsFileResource resource) {
    if (resource == null || resource.getTsFile() == null) {
      return;
    }
    if (globalFileInfoCacheEnabled) {
      synchronized (globalFileInfoCacheForFailedCompaction) {
        globalFileInfoCacheForFailedCompaction.remove(resource.getTsFileID());
      }
      synchronized (globalRoughInfoCacheForCompaction) {
        globalRoughInfoCacheForCompaction.remove(resource.getTsFileID());
      }
    }
  }

  @TestOnly
  public static void enableFileInfoCacheForTest(
      int globalCompactionFileInfoCacheSize, int globalCompactionRoughFileInfoCacheSize) {
    globalFileInfoCacheEnabled = true;
    globalRoughInfoCacheForCompaction =
        Collections.synchronizedMap(new LRUMap<>(globalCompactionFileInfoCacheSize));
    globalFileInfoCacheForFailedCompaction =
        Collections.synchronizedMap(new LRUMap<>(globalCompactionRoughFileInfoCacheSize));
  }

  @TestOnly
  public static void disableFileInfoCacheForTest() {
    globalFileInfoCacheEnabled = false;
    globalRoughInfoCacheForCompaction = Collections.emptyMap();
    globalFileInfoCacheForFailedCompaction = Collections.emptyMap();
  }

  @TestOnly
  public static boolean isGlobalFileInfoCacheEnabled() {
    return globalFileInfoCacheEnabled;
  }
}
