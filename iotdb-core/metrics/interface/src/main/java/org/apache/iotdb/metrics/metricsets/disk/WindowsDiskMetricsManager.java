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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Disk metrics manager for Windows system.
 *
 * <p>Windows does not expose Linux-like cumulative counters through procfs, so this implementation
 * periodically samples Win32 performance counters and accumulates the observed per-second values
 * into totals that match the Linux manager contract as closely as possible.
 */
public class WindowsDiskMetricsManager implements IDiskMetricsManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WindowsDiskMetricsManager.class);

  private static final double BYTES_PER_KB = 1024.0;
  private static final long UPDATE_SMALLEST_INTERVAL = 10000L;
  private static final String POWER_SHELL = "powershell";
  private static final String POWER_SHELL_NO_PROFILE = "-NoProfile";
  private static final String POWER_SHELL_COMMAND = "-Command";
  private static final String TOTAL_DISK_INSTANCE = "_Total";
  private static final Charset WINDOWS_SHELL_CHARSET = getWindowsShellCharset();
  private static final String DISK_QUERY =
      "Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk | "
          + "Where-Object { $_.Name -ne '_Total' } | "
          + "ForEach-Object { "
          + "[string]::Concat("
          + "$_.Name, [char]9, "
          + "$_.DiskReadsPerSec, [char]9, "
          + "$_.DiskWritesPerSec, [char]9, "
          + "$_.DiskReadBytesPerSec, [char]9, "
          + "$_.DiskWriteBytesPerSec, [char]9, "
          + "$_.AvgDisksecPerRead, [char]9, "
          + "$_.AvgDisksecPerWrite, [char]9, "
          + "$_.PercentIdleTime, [char]9, "
          + "$_.AvgDiskQueueLength) }";
  private static final String PROCESS_QUERY_TEMPLATE =
      "Get-CimInstance Win32_PerfFormattedData_PerfProc_Process | "
          + "Where-Object { $_.IDProcess -eq %s } | "
          + "ForEach-Object { "
          + "[string]::Concat("
          + "$_.IOReadOperationsPerSec, [char]9, "
          + "$_.IOWriteOperationsPerSec, [char]9, "
          + "$_.IOReadBytesPerSec, [char]9, "
          + "$_.IOWriteBytesPerSec) }";

  private final String processId;
  private final Set<String> diskIdSet = new HashSet<>();

  private long lastUpdateTime = 0L;
  private long updateInterval = 1L;

  private final Map<String, Long> lastReadOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteOperationCountForDisk = new HashMap<>();
  private final Map<String, Long> lastReadTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteTimeCostForDisk = new HashMap<>();
  private final Map<String, Long> lastMergedReadCountForDisk = new HashMap<>();
  private final Map<String, Long> lastMergedWriteCountForDisk = new HashMap<>();
  private final Map<String, Long> lastReadSizeForDisk = new HashMap<>();
  private final Map<String, Long> lastWriteSizeForDisk = new HashMap<>();
  private final Map<String, Double> lastIoUtilsPercentageForDisk = new HashMap<>();
  private final Map<String, Double> lastQueueSizeForDisk = new HashMap<>();
  private final Map<String, Double> lastAvgReadCostTimeOfEachOpsForDisk = new HashMap<>();
  private final Map<String, Double> lastAvgWriteCostTimeOfEachOpsForDisk = new HashMap<>();
  private final Map<String, Double> lastAvgSizeOfEachReadForDisk = new HashMap<>();
  private final Map<String, Double> lastAvgSizeOfEachWriteForDisk = new HashMap<>();

  private long lastReallyReadSizeForProcess = 0L;
  private long lastReallyWriteSizeForProcess = 0L;
  private long lastAttemptReadSizeForProcess = 0L;
  private long lastAttemptWriteSizeForProcess = 0L;
  private long lastReadOpsCountForProcess = 0L;
  private long lastWriteOpsCountForProcess = 0L;

  public WindowsDiskMetricsManager() {
    processId = String.valueOf(MetricConfigDescriptor.getInstance().getMetricConfig().getPid());
  }

  @Override
  public Map<String, Double> getReadDataSizeForDisk() {
    checkUpdate();
    return toKbMap(lastReadSizeForDisk);
  }

  @Override
  public Map<String, Double> getWriteDataSizeForDisk() {
    checkUpdate();
    return toKbMap(lastWriteSizeForDisk);
  }

  @Override
  public Map<String, Long> getReadOperationCountForDisk() {
    checkUpdate();
    return lastReadOperationCountForDisk;
  }

  @Override
  public Map<String, Long> getWriteOperationCountForDisk() {
    checkUpdate();
    return lastWriteOperationCountForDisk;
  }

  @Override
  public Map<String, Long> getReadCostTimeForDisk() {
    checkUpdate();
    return lastReadTimeCostForDisk;
  }

  @Override
  public Map<String, Long> getWriteCostTimeForDisk() {
    checkUpdate();
    return lastWriteTimeCostForDisk;
  }

  @Override
  public Map<String, Double> getIoUtilsPercentage() {
    checkUpdate();
    return lastIoUtilsPercentageForDisk;
  }

  @Override
  public Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    checkUpdate();
    return lastAvgReadCostTimeOfEachOpsForDisk;
  }

  @Override
  public Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk() {
    checkUpdate();
    return lastAvgWriteCostTimeOfEachOpsForDisk;
  }

  @Override
  public Map<String, Double> getAvgSizeOfEachReadForDisk() {
    checkUpdate();
    return lastAvgSizeOfEachReadForDisk;
  }

  @Override
  public Map<String, Double> getAvgSizeOfEachWriteForDisk() {
    checkUpdate();
    return lastAvgSizeOfEachWriteForDisk;
  }

  @Override
  public Map<String, Long> getMergedWriteOperationForDisk() {
    checkUpdate();
    return lastMergedWriteCountForDisk;
  }

  @Override
  public Map<String, Long> getMergedReadOperationForDisk() {
    checkUpdate();
    return lastMergedReadCountForDisk;
  }

  @Override
  public Map<String, Double> getQueueSizeForDisk() {
    checkUpdate();
    return lastQueueSizeForDisk;
  }

  @Override
  public double getActualReadDataSizeForProcess() {
    checkUpdate();
    return lastReallyReadSizeForProcess / BYTES_PER_KB;
  }

  @Override
  public double getActualWriteDataSizeForProcess() {
    checkUpdate();
    return lastReallyWriteSizeForProcess / BYTES_PER_KB;
  }

  @Override
  public long getReadOpsCountForProcess() {
    checkUpdate();
    return lastReadOpsCountForProcess;
  }

  @Override
  public long getWriteOpsCountForProcess() {
    checkUpdate();
    return lastWriteOpsCountForProcess;
  }

  @Override
  public double getAttemptReadSizeForProcess() {
    checkUpdate();
    return lastAttemptReadSizeForProcess / BYTES_PER_KB;
  }

  @Override
  public double getAttemptWriteSizeForProcess() {
    checkUpdate();
    return lastAttemptWriteSizeForProcess / BYTES_PER_KB;
  }

  @Override
  public Set<String> getDiskIds() {
    checkUpdate();
    return diskIdSet;
  }

  private Map<String, Double> toKbMap(Map<String, Long> source) {
    Map<String, Double> result = new HashMap<>(source.size());
    for (Map.Entry<String, Long> entry : source.entrySet()) {
      result.put(entry.getKey(), entry.getValue() / BYTES_PER_KB);
    }
    return result;
  }

  private void updateInfo() {
    long currentTime = System.currentTimeMillis();
    updateInterval = lastUpdateTime == 0L ? 0L : currentTime - lastUpdateTime;
    lastUpdateTime = currentTime;
    updateDiskInfo();
    updateProcessInfo();
  }

  private void updateDiskInfo() {
    Map<String, String[]> diskInfoMap = queryDiskInfo();
    if (diskInfoMap.isEmpty()) {
      return;
    }

    diskIdSet.clear();
    diskIdSet.addAll(diskInfoMap.keySet());
    pruneDiskMetricMaps();

    for (Map.Entry<String, String[]> entry : diskInfoMap.entrySet()) {
      String diskId = entry.getKey();
      String[] diskInfo = entry.getValue();
      long readOpsPerSec = parseLong(diskInfo[0]);
      long writeOpsPerSec = parseLong(diskInfo[1]);
      long readBytesPerSec = parseLong(diskInfo[2]);
      long writeBytesPerSec = parseLong(diskInfo[3]);
      double avgDiskSecPerRead = parseDouble(diskInfo[4]);
      double avgDiskSecPerWrite = parseDouble(diskInfo[5]);
      double percentIdleTime = parseDouble(diskInfo[6]);
      double avgDiskQueueLength = parseDouble(diskInfo[7]);

      long intervalMillis = updateInterval;
      lastReadOperationCountForDisk.put(
          diskId,
          accumulate(lastReadOperationCountForDisk.get(diskId), readOpsPerSec, intervalMillis));
      lastWriteOperationCountForDisk.put(
          diskId,
          accumulate(lastWriteOperationCountForDisk.get(diskId), writeOpsPerSec, intervalMillis));
      lastMergedReadCountForDisk.put(diskId, 0L);
      lastMergedWriteCountForDisk.put(diskId, 0L);
      lastReadSizeForDisk.put(
          diskId, accumulate(lastReadSizeForDisk.get(diskId), readBytesPerSec, intervalMillis));
      lastWriteSizeForDisk.put(
          diskId, accumulate(lastWriteSizeForDisk.get(diskId), writeBytesPerSec, intervalMillis));
      lastReadTimeCostForDisk.put(
          diskId,
          accumulateTimeCost(
              lastReadTimeCostForDisk.get(diskId),
              avgDiskSecPerRead,
              readOpsPerSec,
              intervalMillis));
      lastWriteTimeCostForDisk.put(
          diskId,
          accumulateTimeCost(
              lastWriteTimeCostForDisk.get(diskId),
              avgDiskSecPerWrite,
              writeOpsPerSec,
              intervalMillis));
      lastIoUtilsPercentageForDisk.put(diskId, clampPercentage(1.0 - percentIdleTime / 100.0));
      lastQueueSizeForDisk.put(diskId, avgDiskQueueLength);
      lastAvgReadCostTimeOfEachOpsForDisk.put(diskId, avgDiskSecPerRead * 1000.0);
      lastAvgWriteCostTimeOfEachOpsForDisk.put(diskId, avgDiskSecPerWrite * 1000.0);
      lastAvgSizeOfEachReadForDisk.put(
          diskId, readOpsPerSec == 0 ? 0.0 : ((double) readBytesPerSec) / readOpsPerSec);
      lastAvgSizeOfEachWriteForDisk.put(
          diskId, writeOpsPerSec == 0 ? 0.0 : ((double) writeBytesPerSec) / writeOpsPerSec);
    }
  }

  private void pruneDiskMetricMaps() {
    pruneDiskMetricMap(lastReadOperationCountForDisk);
    pruneDiskMetricMap(lastWriteOperationCountForDisk);
    pruneDiskMetricMap(lastReadTimeCostForDisk);
    pruneDiskMetricMap(lastWriteTimeCostForDisk);
    pruneDiskMetricMap(lastMergedReadCountForDisk);
    pruneDiskMetricMap(lastMergedWriteCountForDisk);
    pruneDiskMetricMap(lastReadSizeForDisk);
    pruneDiskMetricMap(lastWriteSizeForDisk);
    pruneDiskMetricMap(lastIoUtilsPercentageForDisk);
    pruneDiskMetricMap(lastQueueSizeForDisk);
    pruneDiskMetricMap(lastAvgReadCostTimeOfEachOpsForDisk);
    pruneDiskMetricMap(lastAvgWriteCostTimeOfEachOpsForDisk);
    pruneDiskMetricMap(lastAvgSizeOfEachReadForDisk);
    pruneDiskMetricMap(lastAvgSizeOfEachWriteForDisk);
  }

  private <T> void pruneDiskMetricMap(Map<String, T> metricMap) {
    metricMap.keySet().retainAll(diskIdSet);
  }

  private void updateProcessInfo() {
    String processInfo = queryProcessInfo();
    if (processInfo == null || processInfo.isEmpty()) {
      return;
    }

    String[] processMetricArray = processInfo.split("\t");
    if (processMetricArray.length < 4) {
      LOGGER.warn("Unexpected windows process io info format: {}", processInfo);
      return;
    }

    long readOpsPerSec = parseLong(processMetricArray[0]);
    long writeOpsPerSec = parseLong(processMetricArray[1]);
    long readBytesPerSec = parseLong(processMetricArray[2]);
    long writeBytesPerSec = parseLong(processMetricArray[3]);

    lastReadOpsCountForProcess =
        accumulate(lastReadOpsCountForProcess, readOpsPerSec, updateInterval);
    lastWriteOpsCountForProcess =
        accumulate(lastWriteOpsCountForProcess, writeOpsPerSec, updateInterval);
    lastReallyReadSizeForProcess =
        accumulate(lastReallyReadSizeForProcess, readBytesPerSec, updateInterval);
    lastReallyWriteSizeForProcess =
        accumulate(lastReallyWriteSizeForProcess, writeBytesPerSec, updateInterval);

    // Windows does not expose attempted read/write sizes directly in these counters.
    lastAttemptReadSizeForProcess = lastReallyReadSizeForProcess;
    lastAttemptWriteSizeForProcess = lastReallyWriteSizeForProcess;
  }

  private Map<String, String[]> queryDiskInfo() {
    Map<String, String[]> result = new HashMap<>();
    for (String line : executePowerShell(DISK_QUERY)) {
      if (line == null || line.isEmpty()) {
        continue;
      }
      String[] values = line.split("\t");
      if (values.length < 9) {
        LOGGER.warn("Unexpected windows disk io info format: {}", line);
        continue;
      }
      String diskId = values[0].trim();
      if (diskId.isEmpty() || TOTAL_DISK_INSTANCE.equals(diskId)) {
        continue;
      }
      String[] metricArray = new String[8];
      System.arraycopy(values, 1, metricArray, 0, metricArray.length);
      result.put(diskId, metricArray);
    }
    return result;
  }

  private String queryProcessInfo() {
    for (String line :
        executePowerShell(
            String.format(PROCESS_QUERY_TEMPLATE, escapeSingleQuotedPowerShell(processId)))) {
      if (line != null && !line.isEmpty()) {
        return line;
      }
    }
    return null;
  }

  private String escapeSingleQuotedPowerShell(String value) {
    return value.replace("'", "''");
  }

  private long accumulate(Long previousValue, long valuePerSec, long intervalMillis) {
    if (intervalMillis <= 0L) {
      return previousValue == null ? 0L : previousValue;
    }
    return (previousValue == null ? 0L : previousValue) + valuePerSec * intervalMillis / 1000L;
  }

  private long accumulate(long previousValue, long valuePerSec, long intervalMillis) {
    if (intervalMillis <= 0L) {
      return previousValue;
    }
    return previousValue + valuePerSec * intervalMillis / 1000L;
  }

  private long accumulateTimeCost(
      Long previousValue, double avgTimeInSecond, long opsPerSec, long intervalMillis) {
    if (intervalMillis <= 0L) {
      return previousValue == null ? 0L : previousValue;
    }
    long previous = previousValue == null ? 0L : previousValue;
    double operationCount = opsPerSec * intervalMillis / 1000.0;
    return previous + Math.round(avgTimeInSecond * operationCount * 1000.0);
  }

  private long parseLong(String value) {
    try {
      return Math.round(Double.parseDouble(value.trim()));
    } catch (NumberFormatException e) {
      LOGGER.warn("Failed to parse long value from windows disk metrics: {}", value, e);
      return 0L;
    }
  }

  private double parseDouble(String value) {
    try {
      return Double.parseDouble(value.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("Failed to parse double value from windows disk metrics: {}", value, e);
      return 0.0;
    }
  }

  private double clampPercentage(double value) {
    return Math.max(0.0, Math.min(1.0, value));
  }

  private List<String> executePowerShell(String command) {
    List<String> result = new ArrayList<>();
    List<String> rawOutput = new ArrayList<>();
    Process process = null;
    try {
      process =
          new ProcessBuilder(POWER_SHELL, POWER_SHELL_NO_PROFILE, POWER_SHELL_COMMAND, command)
              .redirectErrorStream(true)
              .start();
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), WINDOWS_SHELL_CHARSET))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String trimmedLine = line.trim();
          if (!trimmedLine.isEmpty()) {
            rawOutput.add(trimmedLine);
          }
        }
      }
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        LOGGER.warn(
            "Failed to collect windows disk metrics, powershell exit code: {}, command {}, output {}",
            exitCode,
            command,
            String.join(" | ", rawOutput));
      } else {
        result.addAll(rawOutput);
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to execute powershell for windows disk metrics", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while collecting windows disk metrics", e);
    } finally {
      if (process != null) {
        process.destroy();
      }
    }
    return result;
  }

  private static Charset getWindowsShellCharset() {
    String nativeEncoding = System.getProperty("sun.jnu.encoding");
    if (nativeEncoding != null && Charset.isSupported(nativeEncoding)) {
      return Charset.forName(nativeEncoding);
    }

    String fileEncoding = System.getProperty("file.encoding");
    if (fileEncoding != null && Charset.isSupported(fileEncoding)) {
      return Charset.forName(fileEncoding);
    }

    if (Charset.isSupported("GBK")) {
      return Charset.forName("GBK");
    }
    return Charset.defaultCharset();
  }

  private void checkUpdate() {
    if (System.currentTimeMillis() - lastUpdateTime > UPDATE_SMALLEST_INTERVAL) {
      updateInfo();
    }
  }
}
