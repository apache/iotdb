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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinuxCpuUsageMetricsManager extends AbstractCpuUsageMetricsManager {
  private final Logger log = LoggerFactory.getLogger(LinuxCpuUsageMetricsManager.class);
  private final String currentPid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  private final File taskFolder = new File(String.format("/proc/%s/task", currentPid));
  private final File systemCpuStatFile = new File("/proc/stat");
  private final String taskCpuStatFile = "/proc/%d/stat";
  private final Map<Long, Long> threadCpuTimeMap = new HashMap<>();
  private final Map<String, Long> moduleCpuTimeMap = new HashMap<>();
  private final Map<String, Double> moduleCpuTimePercentageMap = new HashMap<>();
  private final Map<String, Long> moduleIncrementCpuTimeMap = new HashMap<>();
  private long jvmCpuTime = 0L;
  private long prevTotalCpuTime = 0L;
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
    //    updateSystemCpuUsage();
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
    String[] split = allLines.get(0).split("\\s+");
    long newCpuTime = 0L;
    for (int i = 1; i < split.length; i++) {
      newCpuTime += Long.parseLong(split[i]);
    }
    incrementSystemCpuTime = newCpuTime - systemCpuTime;
    systemCpuTime = newCpuTime;
  }

  private void updateIoTDBCpuUsage() {
    // update
    long[] taskIds = threadMXBean.getAllThreadIds();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(taskIds);
    long totalCpuTime = prevTotalCpuTime;
    Map<String, Long> newModuleCpuTimeMap = new HashMap<>();
    for (ThreadInfo threadInfo : threadInfos) {
      long id = threadInfo.getThreadId();
      String module =
          threadIdToModuleCache.computeIfAbsent(
              id, k -> threadNameToModule.apply(threadInfo.getThreadName()));
      if (module.equals("CLIENT_SERVICE")) {
        ThreadInfo info = threadMXBean.getThreadInfo(id);
        StackTraceElement[] stackTraceElements = info.getStackTrace();
        module = "QUERY";
        for (StackTraceElement stackTraceElement : stackTraceElements) {
          if (stackTraceElement.toString().contains("insert")) {
            module = "WRITE";
            break;
          }
        }
      }
      long cpuTime = threadMXBean.getThreadCpuTime(id);
      long prevCpuTime = threadCpuTimeMap.getOrDefault(id, 0L);
      totalCpuTime += cpuTime - prevCpuTime;
      newModuleCpuTimeMap.compute(
          module, (k, v) -> v == null ? cpuTime - prevCpuTime : v + cpuTime - prevCpuTime);
    }
    long incrementJvmCpuTime = totalCpuTime - jvmCpuTime;
    jvmCpuTime = totalCpuTime;
    for (Map.Entry<String, Long> entry : newModuleCpuTimeMap.entrySet()) {
      String module = entry.getKey();
      long cpuTime = entry.getValue();
      long oldCpuTime = moduleCpuTimeMap.getOrDefault(module, 0L);
      moduleCpuTimeMap.put(module, cpuTime);
      moduleIncrementCpuTimeMap.put(module, cpuTime - oldCpuTime);
      moduleCpuTimePercentageMap.put(
          module, (double) (cpuTime - oldCpuTime) / (double) incrementJvmCpuTime);
    }
  }
}
