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

  @SuppressWarnings("squid:S1075")
  private static final String DISK_SECTOR_SIZE_PATH = "/sys/block/%s/queue/hw_sector_size";

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
  private static final int DISK_IO_TOTAL_TIME_OFFSET = 13;
  private static final int DISK_TIME_IN_QUEUE_OFFSET = 14;
  private static final int DEFAULT_SECTOR_SIZE = 512;
  private static final double BYTES_PER_KB = 1024.0;
  private static final long UPDATE_SMALLEST_INTERVAL = 10000L;
  private Set<String> diskIdSet;
  private final Map<String, Integer> diskSectorSizeMap;
  private long lastUpdateTime = 0L;
  private long updateInterval = 1L;

  // Disk IO status structure
  private final Map<String, Long> lastReadOperationCountForDisk;
  private final Map<String, Long> lastWriteOperationCountForDisk;
  private final Map<String, Long> lastReadTimeCostForDisk;
  private final Map<String, Long> lastWriteTimeCostForDisk;
  private final Map<String, Long> lastMergedReadCountForDisk;
  private final Map<String, Long> lastMergedWriteCountForDisk;
  private final Map<String, Long> lastReadSectorCountForDisk;
  private final Map<String, Long> lastWriteSectorCountForDisk;
  private final Map<String, Long> lastIoBusyTimeForDisk;
  private final Map<String, Long> lastTimeInQueueForDisk;
  private final Map<String, Long> incrementReadOperationCountForDisk;
  private final Map<String, Long> incrementWriteOperationCountForDisk;
  private final Map<String, Long> incrementMergedReadOperationCountForDisk;
  private final Map<String, Long> incrementMergedWriteOperationCountForDisk;
  private final Map<String, Long> incrementReadTimeCostForDisk;
  private final Map<String, Long> incrementWriteTimeCostForDisk;
  private final Map<String, Long> incrementReadSectorCountForDisk;
  private final Map<String, Long> incrementWriteSectorCountForDisk;
  private final Map<String, Long> incrementIoBusyTimeForDisk;
  private final Map<String, Long> incrementTimeInQueueForDisk;

  // Process IO status structure
  private long lastReallyReadSizeForProcess = 0L;
  private long lastReallyWriteSizeForProcess = 0L;
  private long lastAttemptReadSizeForProcess = 0L;
  private long lastAttemptWriteSizeForProcess = 0L;
  private long lastReadOpsCountForProcess = 0L;
  private long lastWriteOpsCountForProcess = 0L;

  public LinuxDiskMetricsManager() {
    processIoStatusPath =
        String.format(
            "/proc/%s/io", MetricConfigDescriptor.getInstance().getMetricConfig().getPid());
    collectDiskId();
    // leave one entry to avoid hashmap resizing
    diskSectorSizeMap = new HashMap<>(diskIdSet.size() + 1, 1);
    collectDiskInfo();
    lastReadOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastWriteOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastReadTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastWriteTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastMergedReadCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastMergedWriteCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastReadSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastWriteSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastIoBusyTimeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastTimeInQueueForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementReadOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementWriteOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementMergedReadOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementMergedWriteOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementReadTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementWriteTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementReadSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementWriteSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementIoBusyTimeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementTimeInQueueForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
  }

  @Override
  public Map<String, Double> getReadDataSizeForDisk() {
    checkUpdate();
    Map<String, Double> readDataMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> entry : lastReadSectorCountForDisk.entrySet()) {
      int sectorSize = diskSectorSizeMap.getOrDefault(entry.getKey(), DEFAULT_SECTOR_SIZE);
      readDataMap.put(entry.getKey(), entry.getValue() * sectorSize / BYTES_PER_KB);
    }
    return readDataMap;
  }

  @Override
  public Map<String, Double> getWriteDataSizeForDisk() {
    checkUpdate();
    Map<String, Double> writeDataMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> entry : lastWriteSectorCountForDisk.entrySet()) {
      int sectorSize = diskSectorSizeMap.getOrDefault(entry.getKey(), DEFAULT_SECTOR_SIZE);
      writeDataMap.put(entry.getKey(), entry.getValue() * sectorSize / BYTES_PER_KB);
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
  public Map<String, Double> getIoUtilsPercentage() {
    Map<String, Double> utilsMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> entry : incrementIoBusyTimeForDisk.entrySet()) {
      utilsMap.put(entry.getKey(), ((double) entry.getValue()) / updateInterval);
    }
    return utilsMap;
  }

  @Override
  public Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    Map<String, Double> avgReadTimeCostMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> readCostEntry : incrementReadTimeCostForDisk.entrySet()) {
      // use Long.max to avoid NaN
      long readOpsCount =
          Long.max(
              incrementReadOperationCountForDisk.getOrDefault(readCostEntry.getKey(), 1L)
                  + incrementMergedReadOperationCountForDisk.getOrDefault(
                      readCostEntry.getKey(), 1L),
              1L);
      avgReadTimeCostMap.put(
          readCostEntry.getKey(), (double) readCostEntry.getValue() / readOpsCount);
    }
    return avgReadTimeCostMap;
  }

  @Override
  public Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk() {
    Map<String, Double> avgWriteTimeCostMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> writeCostEntry : incrementWriteTimeCostForDisk.entrySet()) {
      // use Long.max to avoid NaN
      long writeOpsCount =
          Long.max(
              incrementWriteOperationCountForDisk.getOrDefault(writeCostEntry.getKey(), 1L)
                  + incrementMergedWriteOperationCountForDisk.getOrDefault(
                      writeCostEntry.getKey(), 1L),
              1L);
      avgWriteTimeCostMap.put(
          writeCostEntry.getKey(), (double) writeCostEntry.getValue() / writeOpsCount);
    }
    return avgWriteTimeCostMap;
  }

  @Override
  public Map<String, Double> getAvgSizeOfEachReadForDisk() {
    Map<String, Double> avgSizeOfReadMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> readSectorSizeEntry : incrementReadSectorCountForDisk.entrySet()) {
      // use Long.max to avoid NaN
      long readOpsCount =
          Long.max(
              incrementReadOperationCountForDisk.getOrDefault(readSectorSizeEntry.getKey(), 1L)
                  + incrementMergedReadOperationCountForDisk.getOrDefault(
                      readSectorSizeEntry.getKey(), 1L),
              1L);
      int sectorSize =
          diskSectorSizeMap.getOrDefault(readSectorSizeEntry.getKey(), DEFAULT_SECTOR_SIZE);
      avgSizeOfReadMap.put(
          readSectorSizeEntry.getKey(),
          ((double) readSectorSizeEntry.getValue()) / readOpsCount * sectorSize);
    }
    return avgSizeOfReadMap;
  }

  @Override
  public Map<String, Double> getAvgSizeOfEachWriteForDisk() {
    Map<String, Double> avgSizeOfWriteMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> writeSectorSizeEntry :
        incrementWriteSectorCountForDisk.entrySet()) {
      // use Long.max to avoid NaN
      long writeOpsCount =
          Long.max(
              incrementWriteOperationCountForDisk.getOrDefault(writeSectorSizeEntry.getKey(), 1L)
                  + incrementMergedWriteOperationCountForDisk.getOrDefault(
                      writeSectorSizeEntry.getKey(), 1L),
              1L);
      int sectorSize =
          diskSectorSizeMap.getOrDefault(writeSectorSizeEntry.getKey(), DEFAULT_SECTOR_SIZE);
      avgSizeOfWriteMap.put(
          writeSectorSizeEntry.getKey(),
          ((double) writeSectorSizeEntry.getValue()) / writeOpsCount * sectorSize);
    }
    return avgSizeOfWriteMap;
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
  public Map<String, Double> getQueueSizeForDisk() {
    Map<String, Double> avgQueueSizeMap = new HashMap<>(diskIdSet.size());
    for (Map.Entry<String, Long> entry : incrementTimeInQueueForDisk.entrySet()) {
      avgQueueSizeMap.put(entry.getKey(), (((double) entry.getValue()) / updateInterval));
    }
    return avgQueueSizeMap;
  }

  @Override
  public double getActualReadDataSizeForProcess() {
    return lastReallyReadSizeForProcess / BYTES_PER_KB;
  }

  @Override
  public double getActualWriteDataSizeForProcess() {
    return lastReallyWriteSizeForProcess / BYTES_PER_KB;
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
  public double getAttemptReadSizeForProcess() {
    return lastAttemptReadSizeForProcess / BYTES_PER_KB;
  }

  @Override
  public double getAttemptWriteSizeForProcess() {
    return lastAttemptWriteSizeForProcess / BYTES_PER_KB;
  }

  @Override
  public Set<String> getDiskIds() {
    return diskIdSet;
  }

  private void collectDiskId() {
    File diskIdFolder = new File(DISK_ID_PATH);
    if (!diskIdFolder.exists()) {
      return;
    }
    diskIdSet =
        new ArrayList<>(Arrays.asList(Objects.requireNonNull(diskIdFolder.listFiles())))
            .stream()
                .filter(x -> !x.getName().startsWith("loop") && !x.getName().startsWith("ram"))
                .map(File::getName)
                .collect(Collectors.toSet());
  }

  private void collectDiskInfo() {
    for (String diskId : diskIdSet) {
      String diskSectorSizePath = String.format(DISK_SECTOR_SIZE_PATH, diskId);
      File diskSectorSizeFile = new File(diskSectorSizePath);
      try (Scanner scanner = new Scanner(Files.newInputStream(diskSectorSizeFile.toPath()))) {
        if (scanner.hasNext()) {
          int sectorSize = Integer.parseInt(scanner.nextLine());
          diskSectorSizeMap.put(diskId, sectorSize);
        } else {
          // use DEFAULT_SECTOR_SIZE byte as default value
          diskSectorSizeMap.put(diskId, DEFAULT_SECTOR_SIZE);
        }
      } catch (IOException e) {
        log.warn("Failed to get the sector size of {}", diskId, e);
        // use DEFAULT_SECTOR_SIZE bytes as default value
        diskSectorSizeMap.put(diskId, DEFAULT_SECTOR_SIZE);
      }
    }
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
          DISK_IO_TOTAL_TIME_OFFSET,
          DISK_TIME_IN_QUEUE_OFFSET
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
          lastIoBusyTimeForDisk,
          lastTimeInQueueForDisk
        };
        Map[] incrementMapArray = {
          incrementReadOperationCountForDisk,
          incrementWriteOperationCountForDisk,
          incrementMergedReadOperationCountForDisk,
          incrementMergedWriteOperationCountForDisk,
          incrementReadSectorCountForDisk,
          incrementWriteSectorCountForDisk,
          incrementReadTimeCostForDisk,
          incrementWriteTimeCostForDisk,
          incrementIoBusyTimeForDisk,
          incrementTimeInQueueForDisk
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
