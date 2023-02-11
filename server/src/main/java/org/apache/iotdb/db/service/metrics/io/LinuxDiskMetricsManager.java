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

public class LinuxDiskMetricsManager extends AbstractDiskMetricsManager {
  private final String DISK_STATS_FILE_PATH = "/proc/diskstats";
  private final String DISK_ID_PATH = "/sys/block";
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
  private final long UPDATE_INTERVAL = 10000L;
  private Set<String> diskIDSet;
  private long lastUpdateTime = 0L;
  private String[] dataNodeProcessId;
  private String[] configNodeProcessId;
  private final Map<String, Integer> lastReadOperationCountForDisk = new HashMap<>();
  private final Map<String, Integer> lastWriteOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> lastReadTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastReadSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteSectorCountForDisk = new HashMap<>();
  private final Map<String, Integer> incrementReadOperationCountForDisk = new HashMap<>();
  private final Map<String, Integer> incrementWriteOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementReadTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> incrementWriteTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> incrementReadSectorCountForDisk = new HashMap<>();
  private final Map<String, Long> incrementWriteSectorCountForDisk = new HashMap<>();

  public LinuxDiskMetricsManager() {}

  @Override
  public Map<String, Long> getReadDataSizeForDisk() {
    checkUpdate();
    Map<String, Long> readDataMap = new HashMap<>();
    for (Map.Entry<String, Long> entry : incrementReadSectorCountForDisk.entrySet()) {
      // the data size in each sector is 512 byte
      readDataMap.put(entry.getKey(), entry.getValue() * 512L / 1024L);
    }
    return readDataMap;
  }

  @Override
  public Map<String, Long> getWriteDataSizeForDisk() {
    checkUpdate();
    Map<String, Long> writeDataMap = new HashMap<>();
    for (Map.Entry<String, Long> entry : incrementWriteSectorCountForDisk.entrySet()) {
      // the data size in each sector is 512 byte
      writeDataMap.put(entry.getKey(), entry.getValue() * 512L / 1024L);
    }
    return writeDataMap;
  }

  @Override
  public Map<String, Integer> getReadOperationCountForDisk() {
    checkUpdate();
    return incrementReadOperationCountForDisk;
  }

  @Override
  public Map<String, Integer> getWriteOperationCountForDisk() {
    return incrementWriteOperationCountForDisk;
  }

  @Override
  public Map<String, Long> getReadCostTimeForDisk() {
    return incrementReadTimeCostForDisk;
  }

  @Override
  public Map<String, Long> getWriteCostTimeForDisk() {
    return incrementWriteTimeCostForDisk;
  }

  @Override
  public Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    Map<String, Double> avgReadTimeCostMap = new HashMap<>();
    for (Map.Entry<String, Long> readCostEntry : incrementReadTimeCostForDisk.entrySet()) {
      int readOpsCount = incrementReadOperationCountForDisk.getOrDefault(readCostEntry.getKey(), 1);
      avgReadTimeCostMap.put(
          readCostEntry.getKey(), (double) readCostEntry.getValue() / readOpsCount);
    }
    return avgReadTimeCostMap;
  }

  @Override
  public Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk() {
    Map<String, Double> avgWriteTimeCostMap = new HashMap<>();
    for (Map.Entry<String, Long> writeCostEntry : incrementWriteTimeCostForDisk.entrySet()) {
      int writeOpsCount =
          incrementWriteOperationCountForDisk.getOrDefault(writeCostEntry.getKey(), 1);
      avgWriteTimeCostMap.put(
          writeCostEntry.getKey(), (double) writeCostEntry.getValue() / writeOpsCount);
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
  public long getReadDataSizeForDataNode() {
    return 0;
  }

  @Override
  public long getWriteDataSizeForDataNode() {
    return 0;
  }

  @Override
  public long getReadOpsCountForDataNode() {
    return 0;
  }

  @Override
  public long getWriteOpsCountForDataNode() {
    return 0;
  }

  @Override
  public long getReadCostTimeForDataNode() {
    return 0;
  }

  @Override
  public long getWriteCostTimeForDataNode() {
    return 0;
  }

  @Override
  public long getAvgReadCostTimeOfEachOpsForDataNode() {
    return 0;
  }

  @Override
  public long getAvgWriteCostTimeOfEachOpsForDataNode() {
    return 0;
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

  private void updateDiskInfo() {
    lastUpdateTime = System.currentTimeMillis();
    File diskStatsFile = new File(DISK_STATS_FILE_PATH);
    if (!diskStatsFile.exists()) {
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
        int mergedReadOperationCount = Integer.parseInt(diskInfo[DISK_MERGED_READ_COUNT_OFFSET]);
        int mergedWriteOperationCount = Integer.parseInt(diskInfo[DISK_MERGED_WRITE_COUNT_OFFSET]);
        long sectorReadCount = Long.parseLong(diskInfo[DISK_SECTOR_READ_COUNT_OFFSET]);
        long sectorWriteCount = Long.parseLong(diskInfo[DISK_SECTOR_WRITE_COUNT_OFFSET]);
        long readTimeCost = Long.parseLong(diskInfo[DISK_READ_TIME_COST_OFFSET]);
        long writeTimeCost = Long.parseLong(diskInfo[DISK_WRITE_TIME_COST_OFFSET]);

        int lastReadOperationCount = lastReadOperationCountForDisk.getOrDefault(diskId, 0);
        int lastWriteOperationCount = lastWriteOperationCountForDisk.getOrDefault(diskId, 0);
        //        int lastMergedReadOperationCount = lastM
        long lastSectorReadCount = lastReadSectorCountForDisk.getOrDefault(diskId, 0L);
        long lastSectorWriteCount = lastWriteSectorCountForDisk.getOrDefault(diskId, 0L);
        long lastReadTime = lastReadTimeCostForDisk.getOrDefault(diskId, 0L);
        long lastWriteTime = lastWriteTimeCostForDisk.getOrDefault(diskId, 0L);

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

        lastReadOperationCountForDisk.put(diskId, readOperationCount);
        lastWriteOperationCountForDisk.put(diskId, writeOperationCount);
        lastReadSectorCountForDisk.put(diskId, sectorReadCount);
        lastWriteSectorCountForDisk.put(diskId, sectorWriteCount);
        lastReadTimeCostForDisk.put(diskId, readTimeCost);
        lastWriteTimeCostForDisk.put(diskId, writeTimeCost);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkUpdate() {
    if (System.currentTimeMillis() - lastUpdateTime > UPDATE_INTERVAL) {
      updateDiskInfo();
    }
  }
}
