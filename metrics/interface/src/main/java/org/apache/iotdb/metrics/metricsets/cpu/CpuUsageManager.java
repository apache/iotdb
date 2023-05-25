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

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

public class CpuUsageManager {
  private static final Logger log = LoggerFactory.getLogger(CpuUsageManager.class);
  private static final long UPDATE_INTERVAL = 10_000L;
  protected AbstractMetricService metricService;
  protected final UnaryOperator<String> threadNameToModule;
  protected final UnaryOperator<String> threadNameToPool;
  protected final Map<Long, String> threadIdToModuleCache = new ConcurrentHashMap<>();
  protected final Map<Long, String> threadIdToPoolCache = new ConcurrentHashMap<>();
  private final Map<String, Double> moduleCpuTimePercentageMap = new ConcurrentHashMap<>();
  private final Map<String, Double> moduleUserTimePercentageMap = new ConcurrentHashMap<>();
  private final Map<String, Double> poolCpuUsageMap = new ConcurrentHashMap<>();
  private final Map<String, Double> poolUserTimePercentageMap = new ConcurrentHashMap<>();
  private final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
  private AtomicLong lastUpdateTime = new AtomicLong(0L);

  CpuUsageManager(
      UnaryOperator<String> threadNameToModule, UnaryOperator<String> threadNameToPool) {
    this.threadNameToModule = threadNameToModule;
    this.threadNameToPool = threadNameToPool;
  }

  public void setMetricService(AbstractMetricService metricService) {
    this.metricService = metricService;
  }

  public Map<String, Double> getModuleCpuUsage() {
    checkAndMayUpdate();
    return moduleCpuTimePercentageMap;
  }

  public Map<String, Double> getPoolCpuUsage() {
    checkAndMayUpdate();
    return poolCpuUsageMap;
  }

  public Map<String, Double> getPoolUserCpuPercentage() {
    checkAndMayUpdate();
    return poolUserTimePercentageMap;
  }

  public Map<String, Double> getModuleUserTimePercentage() {
    checkAndMayUpdate();
    return moduleUserTimePercentageMap;
  }

  private synchronized void checkAndMayUpdate() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastUpdateTime.get() > UPDATE_INTERVAL) {
      lastUpdateTime.set(currentTime);
    } else {
      return;
    }
    updateCpuUsage();
  }

  private String getThreadModuleById(long id, ThreadInfo threadInfo) {
    return threadIdToModuleCache.computeIfAbsent(
        id, k -> threadNameToModule.apply(threadInfo.getThreadName()));
  }

  private String getThreadPoolById(long id, ThreadInfo threadInfo) {
    return threadIdToPoolCache.computeIfAbsent(
        id, k -> threadNameToPool.apply(threadInfo.getThreadName()));
  }

  private void updateCpuUsage() {
    if (!checkCpuMonitorEnable()) {
      return;
    }
    // update
    long[] taskIds = threadMxBean.getAllThreadIds();
    ThreadInfo[] threadInfos = threadMxBean.getThreadInfo(taskIds);
    Map<Long, Long> beforeThreadCpuTime = new HashMap<>();
    Map<Long, Long> beforeThreadUserTime = new HashMap<>();
    collectThreadCpuInfo(beforeThreadCpuTime, beforeThreadUserTime, threadInfos);

    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      log.error("Thread is interrupted", e);
      Thread.currentThread().interrupt();
    }

    Map<Long, Long> afterThreadCpuTime = new HashMap<>();
    Map<Long, Long> afterThreadUserTime = new HashMap<>();
    collectThreadCpuInfo(afterThreadCpuTime, afterThreadUserTime, threadInfos);

    Map<String, Long> moduleIncrementCpuTimeMap = new HashMap<>();
    Map<String, Long> moduleIncrementUserTimeMap = new HashMap<>();
    Map<String, Long> poolIncrementCpuTimeMap = new HashMap<>();
    Map<String, Long> poolIncrementUserTimeMap = new HashMap<>();

    long totalIncrementTime =
        computeUsageInfoForModuleAndPool(
            moduleIncrementCpuTimeMap,
            moduleIncrementUserTimeMap,
            poolIncrementCpuTimeMap,
            poolIncrementUserTimeMap,
            beforeThreadCpuTime,
            beforeThreadUserTime,
            afterThreadCpuTime,
            afterThreadUserTime,
            threadInfos);

    if (totalIncrementTime == 0L) {
      return;
    }

    updateUsageMap(
        moduleIncrementCpuTimeMap,
        moduleIncrementUserTimeMap,
        poolIncrementCpuTimeMap,
        poolIncrementUserTimeMap,
        totalIncrementTime);
  }

  private boolean checkCpuMonitorEnable() {
    if (!threadMxBean.isThreadCpuTimeSupported()) {
      return false;
    }
    if (!threadMxBean.isThreadCpuTimeEnabled()) {
      threadMxBean.setThreadCpuTimeEnabled(true);
    }
    return true;
  }

  private void collectThreadCpuInfo(
      Map<Long, Long> cpuTimeMap, Map<Long, Long> userTimeMap, ThreadInfo[] threadInfos) {
    Arrays.stream(threadInfos)
        .forEach(
            info -> {
              cpuTimeMap.put(info.getThreadId(), threadMxBean.getThreadCpuTime(info.getThreadId()));
              userTimeMap.put(
                  info.getThreadId(), threadMxBean.getThreadUserTime(info.getThreadId()));
            });
  }

  private long computeUsageInfoForModuleAndPool(
      Map<String, Long> moduleIncrementCpuTimeMap,
      Map<String, Long> moduleIncrementUserTimeMap,
      Map<String, Long> poolIncrementCpuTimeMap,
      Map<String, Long> poolIncrementUserTimeMap,
      Map<Long, Long> beforeThreadCpuTime,
      Map<Long, Long> beforeThreadUserTime,
      Map<Long, Long> afterThreadCpuTime,
      Map<Long, Long> afterThreadUserTime,
      ThreadInfo[] threadInfos) {
    long totalIncrementTime = 0L;
    for (ThreadInfo threadInfo : threadInfos) {
      long id = threadInfo.getThreadId();
      long beforeCpuTime = beforeThreadCpuTime.get(id);
      long afterCpuTime = afterThreadCpuTime.get(id);
      long beforeUserTime = beforeThreadUserTime.get(id);
      long afterUserTime = afterThreadUserTime.get(id);
      totalIncrementTime += afterCpuTime - beforeCpuTime;
      String module = getThreadModuleById(id, threadInfo);
      String pool = getThreadPoolById(id, threadInfo);
      moduleIncrementCpuTimeMap.compute(
          module,
          (k, v) -> v == null ? afterCpuTime - beforeCpuTime : v + afterCpuTime - beforeCpuTime);
      moduleIncrementUserTimeMap.compute(
          module,
          (k, v) ->
              v == null ? afterUserTime - beforeUserTime : v + afterUserTime - beforeUserTime);
      poolIncrementCpuTimeMap.compute(
          pool,
          (k, v) -> v == null ? afterCpuTime - beforeCpuTime : v + afterCpuTime - beforeCpuTime);
      poolIncrementUserTimeMap.compute(
          pool,
          (k, v) ->
              v == null ? afterUserTime - beforeUserTime : v + afterUserTime - beforeUserTime);
    }
    return totalIncrementTime;
  }

  private void updateUsageMap(
      Map<String, Long> moduleIncrementCpuTimeMap,
      Map<String, Long> moduleIncrementUserTimeMap,
      Map<String, Long> poolIncrementCpuTimeMap,
      Map<String, Long> poolIncrementUserTimeMap,
      long totalIncrementTime) {
    double processCpuLoad =
        metricService.getAutoGauge("process_cpu_load", MetricLevel.CORE, "name", "process").value();
    for (Map.Entry<String, Long> entry : moduleIncrementCpuTimeMap.entrySet()) {
      moduleCpuTimePercentageMap.put(
          entry.getKey(), entry.getValue() * 1.0 / totalIncrementTime * processCpuLoad);
      moduleUserTimePercentageMap.put(
          entry.getKey(), moduleIncrementUserTimeMap.get(entry.getKey()) * 1.0 / entry.getValue());
    }
    for (Map.Entry<String, Long> entry : poolIncrementCpuTimeMap.entrySet()) {
      poolCpuUsageMap.put(
          entry.getKey(), entry.getValue() * 1.0 / totalIncrementTime * processCpuLoad);
      poolUserTimePercentageMap.put(
          entry.getKey(), poolIncrementUserTimeMap.get(entry.getKey()) * 1.0 / entry.getValue());
    }
  }
}
