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

import org.apache.iotdb.metrics.config.MetricConfigDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * All data metrics are collected from <b>/proc/</b>.
 *
 * <p><b>/proc/diskstats</b> is a file in Linux, it contains the running information of the disks,
 * including device name, read operation count, merged read operation count, write operation count,
 * merged write operation count etc. This class collects the data periodically and analyzes the
 * changes in each pieces of data to gain an insight of the disks of status.
 *
 * <p><b>/proc/[PID]/io</b> is also a file in Linux, it indicates the io status of a specific
 * process. The content of it contains following items: actual read byte, actual write byte, read
 * system call count, write system call count, byte attempt to read, byte attempt to write,
 * cancelled write byte.
 */
public class LinuxDiskMetricsManager implements IDiskMetricsManager {
  private final Logger log = LoggerFactory.getLogger(LinuxDiskMetricsManager.class);

  @SuppressWarnings("squid:S1075")
  private static final String DISK_STATUS_FILE_PATH = "/proc/diskstats";

  @SuppressWarnings("squid:S1075")
  private static final String DISK_ID_PATH = "/sys/block";

  private final String processIoStatusPath;
  private static final int DISK_ID_OFFSET = 3;
  private static final int DISK_READ_COUNT_OFFSET = 4;
  private static final int DISK_MERGED_READ_COUNT_OFFSET = 5;
  private static final int DISK_SECTOR_READ_COUNT_OFFSET = 6;
  private static final int DISK_READ_TIME_OFFSET = 7;
  private static final int DISK_WRITE_COUNT_OFFSET = 8;
  private static final int DISK_MERGED_WRITE_COUNT_OFFSET = 9;
  private static final int DISK_SECTOR_WRITE_COUNT_OFFSET = 10;
  private static final int DISK_WRITE_TIME_COST_OFFSET = 11;
  private static final int DISK_QUEUE_SIZE_OFFSET = 12;
  private static final int DISK_IO_TOTAL_TIME_OFFSET = 13;
  private static final long UPDATE_SMALLEST_INTERVAL = 10000L;
  private Set<String> diskIdSet;
  private long lastUpdateTime = 0L;
  private long updateInterval = 1L;

  // Disk IO status structure
  private final Map<String, Long> lastReadOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> lastReadTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastMergedReadCountForDisk = new HashMap<>();
  private final Map<String, Long> lastMergedWriteCountForDisk = new HashMap<>();
  private final Map<String, Long> lastReadSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> queueSizeMap = new HashMap<>();
  private final Map<String, Long> lastIoBusyTimeForDisk = new HashMap<>();
  private final Map<String, Long> incrementReadOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementWriteOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementReadTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> incrementWriteTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> incrementReadSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementWriteSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementIoBusyTimeForDisk = new HashMap<>();

  // Process IO status structure
  private long lastReallyReadSizeForProcess = 0L;
  private long lastReallyWriteSizeForProcess = 0L;
  private long lastAttemptReadSizeForProcess = 0L;
  private long lastAttemptWriteSizeForProcess = 0L;
  private long lastReadOpsCountForProcess = 0L;
  private long lastWriteOpsCountForProcess = 0L;

  public LinuxDiskMetricsManager() {
    super();
    processIoStatusPath =
        String.format(
            "/proc/%s/io", MetricConfigDescriptor.getInstance().getMetricConfig().getPid());
  }

  @Override
  public Map<String, Long> getReadDataSizeForDisk() {
    checkUpdate();
    Map<String, Long> readDataMap = new HashMap<>();
    for (Map.Entry<String, Long> entry : lastReadSectorCountForDisk.entrySet()) {
      // the data size in each sector is 512 byte
      readDataMap.put(entry.getKey(), entry.getValue() * 512 / 1024);
    }
    return readDataMap;
  }

  @Override
  public Map<String, Long> getWriteDataSizeForDisk() {
    checkUpdate();
    Map<String, Long> writeDataMap = new HashMap<>();
    for (Map.Entry<String, Long> entry : lastWriteSectorCountForDisk.entrySet()) {
      // the data size in each sector is 512 byte
      writeDataMap.put(entry.getKey(), entry.getValue() * 512 / 1024);
    }
    return writeDataMap;
  }

  @Override
  public Map<String, Long> getReadOperationCountForDisk() {
    checkUpdate();
    return lastReadOperationCountForDisk;
  }

  @Override
  public Map<String, Long> getWriteOperationCountForDisk() {
    return lastWriteOperationCountForDisk;
  }

  @Override
  public Map<String, Long> getReadCostTimeForDisk() {
    return lastReadTimeCostForDisk;
  }

  @Override
  public Map<String, Long> getWriteCostTimeForDisk() {
    return lastWriteTimeCostForDisk;
  }

  @Override
  public Map<String, Long> getIoUtilsPercentage() {
    Map<String, Long> utilsMap = new HashMap<>();
    for (Map.Entry<String, Long> entry : incrementIoBusyTimeForDisk.entrySet()) {
      utilsMap.put(entry.getKey(), (long) (entry.getValue() * 10000.0 / updateInterval));
    }
    return utilsMap;
  }

  @Override
  public Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    Map<String, Double> avgReadTimeCostMap = new HashMap<>();
    for (Map.Entry<String, Long> readCostEntry : incrementReadTimeCostForDisk.entrySet()) {
      long writeOpsCount =
          incrementReadOperationCountForDisk.getOrDefault(readCostEntry.getKey(), 1L);
      // convert to nanosecond
      avgReadTimeCostMap.put(
          readCostEntry.getKey(), (double) readCostEntry.getValue() / writeOpsCount * 1000_000.0);
    }
    return avgReadTimeCostMap;
  }

  @Override
  public Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk() {
    Map<String, Double> avgWriteTimeCostMap = new HashMap<>();
    for (Map.Entry<String, Long> writeCostEntry : incrementWriteTimeCostForDisk.entrySet()) {
      long writeOpsCount =
          incrementWriteOperationCountForDisk.getOrDefault(writeCostEntry.getKey(), 1L);
      // convert to nanosecond
      avgWriteTimeCostMap.put(
          writeCostEntry.getKey(), (double) writeCostEntry.getValue() / writeOpsCount * 1000_000.0);
    }
    return avgWriteTimeCostMap;
  }

  @Override
  public Map<String, Double> getAvgSectorCountOfEachReadForDisk() {
    Map<String, Double> avgSectorSizeOfRead = new HashMap<>();
    for (Map.Entry<String, Long> readSectorSizeEntry : incrementReadSectorCountForDisk.entrySet()) {
      long readOpsCount =
          incrementReadOperationCountForDisk.getOrDefault(readSectorSizeEntry.getKey(), 1L);
      avgSectorSizeOfRead.put(
          readSectorSizeEntry.getKey(), ((double) readSectorSizeEntry.getValue()) / readOpsCount);
    }
    return avgSectorSizeOfRead;
  }

  @Override
  public Map<String, Double> getAvgSectorCountOfEachWriteForDisk() {
    Map<String, Double> avgSectorSizeOfWrite = new HashMap<>();
    for (Map.Entry<String, Long> writeSectorSizeEntry :
        incrementWriteSectorCountForDisk.entrySet()) {
      long writeOpsCount =
          incrementWriteOperationCountForDisk.getOrDefault(writeSectorSizeEntry.getKey(), 1L);
      avgSectorSizeOfWrite.put(
          writeSectorSizeEntry.getKey(),
          ((double) writeSectorSizeEntry.getValue()) / writeOpsCount);
    }
    return avgSectorSizeOfWrite;
  }

  @Override
  public Map<String, Long> getMergedWriteOperationForDisk() {
    return lastMergedWriteCountForDisk;
  }

  @Override
  public Map<String, Long> getMergedReadOperationForDisk() {
    return lastMergedReadCountForDisk;
  }

  @Override
  public Map<String, Long> getQueueSizeForDisk() {
    return queueSizeMap;
  }

  @Override
  public long getActualReadDataSizeForProcess() {
    return lastReallyReadSizeForProcess / 1024;
  }

  @Override
  public long getActualWriteDataSizeForProcess() {
    return lastReallyWriteSizeForProcess / 1024;
  }

  @Override
  public long getReadOpsCountForProcess() {
    return lastReadOpsCountForProcess;
  }

  @Override
  public long getWriteOpsCountForProcess() {
    return lastWriteOpsCountForProcess;
  }

  @Override
  public long getAttemptReadSizeForProcess() {
    return (long) (lastAttemptReadSizeForProcess / 1024.0);
  }

  @Override
  public long getAttemptWriteSizeForProcess() {
    return (long) (lastAttemptWriteSizeForProcess / 1024.0);
  }

  @Override
  public Set<String> getDiskIds() {
    File diskIdFolder = new File(DISK_ID_PATH);
    if (!diskIdFolder.exists()) {
      return Collections.emptySet();
    }
    diskIdSet =
        new ArrayList<>(Arrays.asList(Objects.requireNonNull(diskIdFolder.listFiles())))
            .stream()
                .filter(x -> !x.getName().startsWith("loop") && !x.getName().startsWith("ram"))
                .map(File::getName)
                .collect(Collectors.toSet());
    return diskIdSet;
  }

  private void updateInfo() {
    long currentTime = System.currentTimeMillis();
    updateInterval = currentTime - lastUpdateTime;
    lastUpdateTime = currentTime;
    updateDiskInfo();
    updateProcessInfo();
  }

  private void updateDiskInfo() {
    File diskStatsFile = new File(DISK_STATUS_FILE_PATH);
    if (!diskStatsFile.exists()) {
      log.warn("Cannot find disk io status file {}", DISK_STATUS_FILE_PATH);
      return;
    }

    try (Scanner diskStatsScanner = new Scanner(Files.newInputStream(diskStatsFile.toPath()))) {
      while (diskStatsScanner.hasNextLine()) {
        String[] diskInfo = diskStatsScanner.nextLine().split("\\s+");
        String diskId = diskInfo[DISK_ID_OFFSET];
        if (!diskIdSet.contains(diskId)) {
          continue;
        }
        int[] offsetArray = {
          DISK_READ_COUNT_OFFSET,
          DISK_WRITE_COUNT_OFFSET,
          DISK_MERGED_READ_COUNT_OFFSET,
          DISK_MERGED_WRITE_COUNT_OFFSET,
          DISK_SECTOR_READ_COUNT_OFFSET,
          DISK_SECTOR_WRITE_COUNT_OFFSET,
          DISK_READ_TIME_OFFSET,
          DISK_WRITE_TIME_COST_OFFSET,
          DISK_QUEUE_SIZE_OFFSET,
          DISK_IO_TOTAL_TIME_OFFSET
        };
        Map[] lastMapArray = {
          lastReadOperationCountForDisk,
          lastWriteOperationCountForDisk,
          lastMergedReadCountForDisk,
          lastMergedWriteCountForDisk,
          lastReadSectorCountForDisk,
          lastWriteSectorCountForDisk,
          lastReadTimeCostForDisk,
          lastWriteTimeCostForDisk,
          queueSizeMap,
          lastIoBusyTimeForDisk
        };
        Map[] incrementMapArray = {
          incrementReadOperationCountForDisk,
          incrementWriteOperationCountForDisk,
          null,
          null,
          incrementReadSectorCountForDisk,
          incrementWriteSectorCountForDisk,
          incrementReadTimeCostForDisk,
          incrementWriteTimeCostForDisk,
          null,
          incrementIoBusyTimeForDisk
        };
        for (int index = 0, length = offsetArray.length; index < length; ++index) {
          updateSingleDiskInfo(
              diskId, diskInfo, offsetArray[index], lastMapArray[index], incrementMapArray[index]);
        }
      }
    } catch (IOException e) {
      log.error("Meets error while updating disk io info", e);
    }
  }

  private void updateSingleDiskInfo(
      String diskId,
      String[] diskInfo,
      int offset,
      Map<String, Long> lastMap,
      Map<String, Long> incrementMap) {
    long currentValue = Long.parseLong(diskInfo[offset]);
    if (incrementMap != null) {
      long lastValue = lastMap.getOrDefault(diskId, 0L);
      if (lastValue != 0) {
        incrementMap.put(diskId, currentValue - lastValue);
      } else {
        incrementMap.put(diskId, 0L);
      }
    }
    lastMap.put(diskId, currentValue);
  }

  private void updateProcessInfo() {
    File processStatInfoFile = new File(processIoStatusPath);
    if (!processStatInfoFile.exists()) {
      log.warn("Cannot find process io status file {}", processIoStatusPath);
    }

    try (Scanner processStatsScanner =
        new Scanner(Files.newInputStream(processStatInfoFile.toPath()))) {
      while (processStatsScanner.hasNextLine()) {
        String infoLine = processStatsScanner.nextLine();
        if (infoLine.startsWith("syscr")) {
          lastReadOpsCountForProcess = Long.parseLong(infoLine.split(":\\s")[1]);
        } else if (infoLine.startsWith("syscw")) {
          lastWriteOpsCountForProcess = Long.parseLong(infoLine.split(":\\s")[1]);
        } else if (infoLine.startsWith("read_bytes")) {
          lastReallyReadSizeForProcess = Long.parseLong(infoLine.split(":\\s")[1]);
        } else if (infoLine.startsWith("write_bytes")) {
          lastReallyWriteSizeForProcess = Long.parseLong(infoLine.split(":\\s")[1]);
        } else if (infoLine.startsWith("rchar")) {
          lastAttemptReadSizeForProcess = Long.parseLong(infoLine.split(":\\s")[1]);
        } else if (infoLine.startsWith("wchar")) {
          lastAttemptWriteSizeForProcess = Long.parseLong(infoLine.split(":\\s")[1]);
        }
      }
    } catch (IOException e) {
      log.error("Meets error while updating process io info", e);
    }
  }

  private void checkUpdate() {
    if (System.currentTimeMillis() - lastUpdateTime > UPDATE_SMALLEST_INTERVAL) {
      updateInfo();
    }
  }
}
