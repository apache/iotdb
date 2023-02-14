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

package org.apache.iotdb.db.service.metrics.io;

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
public class LinuxDiskMetricsManager extends AbstractDiskMetricsManager {
  private final Logger log = LoggerFactory.getLogger(AbstractDiskMetricsManager.class);
  private final String DISK_STATS_FILE_PATH = "/proc/diskstats";
  private final String DISK_ID_PATH = "/sys/block";
  private final String PROCESS_IO_STAT_PATH;
  private final int DISK_ID_OFFSET = 3;
  private final int DISK_READ_COUNT_OFFSET = 4;
  private final int DISK_MERGED_READ_COUNT_OFFSET = 5;
  private final int DISK_SECTOR_READ_COUNT_OFFSET = 6;
  private final int DISK_READ_TIME_COST_OFFSET = 7;
  private final int DISK_WRITE_COUNT_OFFSET = 8;
  private final int DISK_MERGED_WRITE_COUNT_OFFSET = 9;
  private final int DISK_SECTOR_WRITE_COUNT_OFFSET = 10;
  private final int DISK_WRITE_TIME_COST_OFFSET = 11;
  private final int DISK_IO_TOTAL_TIME_OFFSET = 13;
  private final long UPDATE_SMALLEST_INTERVAL = 10000L;
  private Set<String> diskIDSet;
  private long lastUpdateTime = 0L;
  private long updateInterval = 1L;

  // Disk IO status structure
  private final Map<String, Integer> lastReadOperationCountForDisk = new HashMap<>();
  private final Map<String, Integer> lastWriteOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> lastReadTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastMergedReadCountForDisk = new HashMap<>();
  private final Map<String, Long> lastMergedWriteCountForDisk = new HashMap<>();
  private final Map<String, Long> lastReadSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> lastIOBusyTimeForDisk = new HashMap<>();
  private final Map<String, Integer> incrementReadOperationCountForDisk = new HashMap<>();
  private final Map<String, Integer> incrementWriteOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementReadTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> incrementWriteTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> incrementReadSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementWriteSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementIOBusyTimeForDisk = new HashMap<>();

  // Process IO status structure
  private long lastReallyReadSizeForProcess = 0L;
  private long lastReallyWriteSizeForProcess = 0L;
  private long lastAttemptReadSizeForProcess = 0L;
  private long lastAttemptWriteSizeForProcess = 0L;
  private long lastReadOpsCountForProcess = 0L;
  private long lastWriteOpsCountForProcess = 0L;

  public LinuxDiskMetricsManager() {
    super();
    PROCESS_IO_STAT_PATH =
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
  public Map<String, Integer> getReadOperationCountForDisk() {
    checkUpdate();
    return lastReadOperationCountForDisk;
  }

  @Override
  public Map<String, Integer> getWriteOperationCountForDisk() {
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
  public Map<String, Long> getIOUtilsPercentage() {
    Map<String, Long> utilsMap = new HashMap<>();
    for (Map.Entry<String, Long> entry : incrementIOBusyTimeForDisk.entrySet()) {
      utilsMap.put(entry.getKey(), (long) (entry.getValue() * 1000.0 / updateInterval * 10000.0));
    }
    return utilsMap;
  }

  @Override
  public Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    Map<String, Double> avgReadTimeCostMap = new HashMap<>();
    for (Map.Entry<String, Long> readCostEntry : incrementReadTimeCostForDisk.entrySet()) {
      int writeOpsCount =
          incrementReadOperationCountForDisk.getOrDefault(readCostEntry.getKey(), 1);
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
      int writeOpsCount =
          incrementWriteOperationCountForDisk.getOrDefault(writeCostEntry.getKey(), 1);
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
      int readOpsCount =
          incrementReadOperationCountForDisk.getOrDefault(readSectorSizeEntry.getKey(), 1);
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
      int writeOpsCount =
          incrementWriteOperationCountForDisk.getOrDefault(writeSectorSizeEntry.getKey(), 1);
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
  public Set<String> getDiskIDs() {
    File diskIDFolder = new File(DISK_ID_PATH);
    if (!diskIDFolder.exists()) {
      return Collections.emptySet();
    }
    diskIDSet =
        new ArrayList<>(Arrays.asList(Objects.requireNonNull(diskIDFolder.listFiles())))
            .stream()
                .filter(x -> !x.getName().startsWith("loop") && !x.getName().startsWith("ram"))
                .map(File::getName)
                .collect(Collectors.toSet());
    return diskIDSet;
  }

  private void updateInfo() {
    long currentTime = System.currentTimeMillis();
    updateInterval = currentTime - lastUpdateTime;
    lastUpdateTime = currentTime;
    updateDiskInfo();
    updateProcessInfo();
  }

  private void updateDiskInfo() {
    File diskStatsFile = new File(DISK_STATS_FILE_PATH);
    if (!diskStatsFile.exists()) {
      log.warn("Cannot find disk io status file {}", DISK_STATS_FILE_PATH);
      return;
    }

    try (Scanner diskStatsScanner = new Scanner(Files.newInputStream(diskStatsFile.toPath()))) {
      while (diskStatsScanner.hasNextLine()) {
        String[] diskInfo = diskStatsScanner.nextLine().split("\\s+");
        String diskId = diskInfo[DISK_ID_OFFSET];
        if (!diskIDSet.contains(diskId)) {
          continue;
        }
        int readOperationCount = Integer.parseInt(diskInfo[DISK_READ_COUNT_OFFSET]);
        int writeOperationCount = Integer.parseInt(diskInfo[DISK_WRITE_COUNT_OFFSET]);
        long mergedReadOperationCount = Long.parseLong(diskInfo[DISK_MERGED_READ_COUNT_OFFSET]);
        long mergedWriteOperationCount = Long.parseLong(diskInfo[DISK_MERGED_WRITE_COUNT_OFFSET]);
        long sectorReadCount = Long.parseLong(diskInfo[DISK_SECTOR_READ_COUNT_OFFSET]);
        long sectorWriteCount = Long.parseLong(diskInfo[DISK_SECTOR_WRITE_COUNT_OFFSET]);
        long readTimeCost = Long.parseLong(diskInfo[DISK_READ_TIME_COST_OFFSET]);
        long writeTimeCost = Long.parseLong(diskInfo[DISK_WRITE_TIME_COST_OFFSET]);
        long ioBusyTime = Long.parseLong(diskInfo[DISK_IO_TOTAL_TIME_OFFSET]);
        long lastMergedReadCount = lastMergedReadCountForDisk.getOrDefault(diskId, 0L);
        long lastMergedWriteCount = lastMergedWriteCountForDisk.getOrDefault(diskId, 0L);
        int lastReadOperationCount = lastReadOperationCountForDisk.getOrDefault(diskId, 0);
        int lastWriteOperationCount = lastWriteOperationCountForDisk.getOrDefault(diskId, 0);
        long lastSectorReadCount = lastReadSectorCountForDisk.getOrDefault(diskId, 0L);
        long lastSectorWriteCount = lastWriteSectorCountForDisk.getOrDefault(diskId, 0L);
        long lastReadTime = lastReadTimeCostForDisk.getOrDefault(diskId, 0L);
        long lastWriteTime = lastWriteTimeCostForDisk.getOrDefault(diskId, 0L);
        long lastIOBusyTime = lastIOBusyTimeForDisk.getOrDefault(diskId, 0L);

        if (lastReadOperationCount != 0) {
          incrementReadOperationCountForDisk.put(
              diskId, readOperationCount - lastReadOperationCount);
        } else {
          incrementReadOperationCountForDisk.put(diskId, 0);
        }

        if (lastWriteOperationCount != 0) {
          incrementWriteOperationCountForDisk.put(
              diskId, writeOperationCount - lastWriteOperationCount);
        } else {
          incrementWriteOperationCountForDisk.put(diskId, 0);
        }

        if (lastSectorReadCount != 0) {
          incrementReadSectorCountForDisk.put(diskId, sectorReadCount - lastSectorReadCount);
        } else {
          incrementReadSectorCountForDisk.put(diskId, 0L);
        }

        if (lastSectorWriteCount != 0) {
          incrementWriteSectorCountForDisk.put(diskId, sectorWriteCount - lastSectorWriteCount);
        } else {
          incrementWriteSectorCountForDisk.put(diskId, 0L);
        }

        if (lastReadTime != 0) {
          incrementReadTimeCostForDisk.put(diskId, readTimeCost - lastReadTime);
        } else {
          incrementReadTimeCostForDisk.put(diskId, 0L);
        }

        if (lastWriteTime != 0) {
          incrementWriteTimeCostForDisk.put(diskId, writeTimeCost - lastWriteTime);
        } else {
          incrementWriteTimeCostForDisk.put(diskId, 0L);
        }

        if (lastIOBusyTime != 0) {
          incrementIOBusyTimeForDisk.put(diskId, ioBusyTime - lastIOBusyTime);
        } else {
          incrementIOBusyTimeForDisk.put(diskId, 0L);
        }

        lastReadOperationCountForDisk.put(diskId, readOperationCount);
        lastWriteOperationCountForDisk.put(diskId, writeOperationCount);
        lastReadSectorCountForDisk.put(diskId, sectorReadCount);
        lastWriteSectorCountForDisk.put(diskId, sectorWriteCount);
        lastReadTimeCostForDisk.put(diskId, readTimeCost);
        lastWriteTimeCostForDisk.put(diskId, writeTimeCost);
        lastIOBusyTimeForDisk.put(diskId, ioBusyTime);
        lastMergedReadCountForDisk.put(diskId, mergedReadOperationCount);
        lastMergedWriteCountForDisk.put(diskId, mergedWriteOperationCount);
      }
    } catch (IOException e) {
      log.error("Meets error while updating disk io info", e);
    }
  }

  private void updateProcessInfo() {
    File processStatInfoFile = new File(PROCESS_IO_STAT_PATH);
    if (!processStatInfoFile.exists()) {
      log.warn("Cannot find process io status file {}", PROCESS_IO_STAT_PATH);
    }

    try (Scanner processStatsScanner =
        new Scanner(Files.newInputStream(processStatInfoFile.toPath()))) {
      while (processStatsScanner.hasNextLine()) {
        String infoLine = processStatsScanner.nextLine();
        if (infoLine.startsWith("syscr")) {
          long currentReadOpsCount = Long.parseLong(infoLine.split(":\\s")[1]);
          lastReadOpsCountForProcess = currentReadOpsCount;
        } else if (infoLine.startsWith("syscw")) {
          long currentWriteOpsCount = Long.parseLong(infoLine.split(":\\s")[1]);
          lastWriteOpsCountForProcess = currentWriteOpsCount;
        } else if (infoLine.startsWith("read_bytes")) {
          long currentReadSize = Long.parseLong(infoLine.split(":\\s")[1]);
          lastReallyReadSizeForProcess = currentReadSize;
        } else if (infoLine.startsWith("write_bytes")) {
          long currentWriteSize = Long.parseLong(infoLine.split(":\\s")[1]);
          lastReallyWriteSizeForProcess = currentWriteSize;
        } else if (infoLine.startsWith("rchar")) {
          long currentAttemptReadSize = Long.parseLong(infoLine.split(":\\s")[1]);
          lastAttemptReadSizeForProcess = currentAttemptReadSize;
        } else if (infoLine.startsWith("wchar")) {
          long currentAttemptWriteSize = Long.parseLong(infoLine.split(":\\s")[1]);
          lastAttemptWriteSizeForProcess = currentAttemptWriteSize;
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
