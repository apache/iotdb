/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.metricsets.cpu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class LinuxCpuUsageMetricsManager extends AbstractCpuUsageMetricsManager {
  private final Logger log = LoggerFactory.getLogger(LinuxCpuUsageMetricsManager.class);
  private final String currentPid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  private final File taskFolder = new File(String.format("/proc/%s/task", currentPid));
  private final File systemCpuStatFile = new File("/proc/stat");
  private final String taskCpuStatFile = "/proc/%d/stat";
  private final Map<Long, Long> threadUserTimeMap = new HashMap<>();
  private final Map<Long, Long> threadSystemTimeMap = new HashMap<>();
  private final Map<String, Long> moduleCpuTimeMap = new HashMap<>();
  private final Map<String, Double> moduleCpuTimePercentageMap = new HashMap<>();
  private final Map<String, Long> incrementCpuTimeMap = new HashMap<>();
  private long systemCpuTime = 0L;
  private long incrementSystemCpuTime = 0L;
  private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private static final long UPDATE_INTERVAL = 10_000L;
  private long lastUpdateTime = 0L;

  public LinuxCpuUsageMetricsManager(UnaryOperator<String> threadNameToModule) {
    super(threadNameToModule);
  }

  @Override
  public Map<String, Double> getCpuUsageForPerModule() {
    checkAndMayUpdate();
    return moduleCpuTimePercentageMap;
  }

  private void checkAndMayUpdate() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastUpdateTime > UPDATE_INTERVAL) {
      lastUpdateTime = currentTime;
    }

    update();
  }

  private void update() {
    updateSystemCpuUsage();
    updateIoTDBCpuUsage();
  }

  private void updateSystemCpuUsage() {
    if (!systemCpuStatFile.exists()) {
      return;
    }
    List<String> allLines = null;
    try {
      allLines = Files.readAllLines(systemCpuStatFile.toPath());
    } catch (IOException e) {
      log.error("Meet error when read system cpu stat file", e);
      return;
    }
    String[] split = allLines.get(0).split(" ");
    long newCpuTime = 0L;
    for (int i = 1; i < split.length; i++) {
      newCpuTime += Long.parseLong(split[i]);
    }
    incrementSystemCpuTime = newCpuTime - systemCpuTime;
    systemCpuTime = newCpuTime;
  }

  private void updateIoTDBCpuUsage() {
    // update
    if (!taskFolder.exists()) {
      return;
    }
    File[] taskFiles = taskFolder.listFiles();
    if (taskFiles == null || taskFiles.length == 0) {
      return;
    }

    Map<String, Long> currentModuleCpuTimeMap = new HashMap<>();
    for (File taskFile : taskFiles) {
      long taskPid = Long.parseLong(taskFile.getName());
      File file = new File(String.format(taskCpuStatFile, taskPid));
      if (!file.exists()) {
        continue;
      }
      List<String> allLines = null;
      try {
        allLines = Files.readAllLines(file.toPath());
      } catch (IOException e) {
        log.error("Meet error when read task cpu stat file", e);
      }
      if (allLines == null || allLines.isEmpty()) {
        continue;
      }
      String[] split = allLines.get(0).split(" ");
      long userTime = Long.parseLong(split[13]);
      long systemTime = Long.parseLong(split[14]);
      threadUserTimeMap.put(taskPid, userTime);
      threadSystemTimeMap.put(taskPid, systemTime);
      String module = threadIdToModuleCache.getOrDefault(taskPid, null);
      if (module == null) {
        String threadName = threadMXBean.getThreadInfo(taskPid).getThreadName();
        module = threadNameToModule.apply(threadName);
        threadIdToModuleCache.put(taskPid, module);
      }
      currentModuleCpuTimeMap.compute(
          module, (k, v) -> v == null ? userTime + systemTime : userTime + systemTime + v);
    }

    for (Map.Entry<String, Long> entry : currentModuleCpuTimeMap.entrySet()) {
      String module = entry.getKey();
      long oldVal = moduleCpuTimeMap.getOrDefault(module, 0L);
      long newVal = entry.getValue();
      incrementCpuTimeMap.put(module, newVal - oldVal);
      moduleCpuTimeMap.put(module, newVal);
      moduleCpuTimePercentageMap.put(module, (double) (newVal - oldVal) / incrementSystemCpuTime);
    }
  }
}
