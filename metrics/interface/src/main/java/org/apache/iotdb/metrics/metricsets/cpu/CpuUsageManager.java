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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class CpuUsageManager {
  private static final Logger log = LoggerFactory.getLogger(CpuUsageManager.class);
  private static final long UPDATE_INTERVAL = 10_000L;
  protected AbstractMetricService metricService;
  protected final UnaryOperator<String> threadNameToModule;
  protected final UnaryOperator<String> threadNameToPool;
  protected final Map<Long, String> threadIdToModuleCache = new HashMap<>();
  protected final Map<Long, String> threadIdToPoolCache = new HashMap<>();
  private final Map<String, Double> moduleCpuTimePercentageMap = new HashMap<>();
  private final Map<String, Double> poolCpuUsageMap = new HashMap<>();
  private final Map<String, Double> poolUserTimePercentageMap = new HashMap<>();
  private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private long lastUpdateTime = 0L;

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

  private void checkAndMayUpdate() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastUpdateTime > UPDATE_INTERVAL) {
      lastUpdateTime = currentTime;
    }

    updateIoTDBCpuUsage();
  }

  private String getThreadModuleById(long id, ThreadInfo threadInfo) {
    return threadIdToModuleCache.computeIfAbsent(
        id, k -> threadNameToModule.apply(threadInfo.getThreadName()));
  }

  private String getThreadPoolById(long id, ThreadInfo threadInfo) {
    return threadIdToPoolCache.computeIfAbsent(
        id, k -> threadNameToPool.apply(threadInfo.getThreadName()));
  }

  private void updateIoTDBCpuUsage() {
    // update
    if (!threadMXBean.isThreadCpuTimeSupported()) {
      return;
    }
    if (!threadMXBean.isThreadCpuTimeEnabled()) {
      threadMXBean.setThreadCpuTimeEnabled(true);
    }
    long[] taskIds = threadMXBean.getAllThreadIds();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(taskIds);
    Map<Long, Long> beforeThreadCpuTime =
        Arrays.stream(threadInfos)
            .collect(
                Collectors.toMap(
                    ThreadInfo::getThreadId,
                    threadInfo -> threadMXBean.getThreadCpuTime(threadInfo.getThreadId())));
    Map<Long, Long> beforeThreadUserTime =
        Arrays.stream(threadInfos)
            .collect(
                Collectors.toMap(
                    ThreadInfo::getThreadId,
                    threadInfo -> threadMXBean.getThreadUserTime(threadInfo.getThreadId())));

    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      log.error("Thread sleep error", e);
    }

    Map<Long, Long> afterThreadCpuTime =
        Arrays.stream(threadInfos)
            .collect(
                Collectors.toMap(
                    ThreadInfo::getThreadId,
                    threadInfo -> threadMXBean.getThreadCpuTime(threadInfo.getThreadId())));
    Map<Long, Long> afterThreadUserTime =
        Arrays.stream(threadInfos)
            .collect(
                Collectors.toMap(
                    ThreadInfo::getThreadId,
                    threadInfo -> threadMXBean.getThreadUserTime(threadInfo.getThreadId())));

    long totalIncrementTime = 0L;
    Map<String, Long> moduleIncrementCpuTimeMap = new HashMap<>();
    Map<String, Long> poolIncrementCpuTimeMap = new HashMap<>();
    Map<String, Long> poolIncrementUserTimeMap = new HashMap<>();
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
          module, (k, v) -> v == null ? 0L : v + afterCpuTime - beforeCpuTime);
      poolIncrementCpuTimeMap.compute(
          pool, (k, v) -> v == null ? 0L : v + afterCpuTime - beforeCpuTime);
      poolIncrementUserTimeMap.compute(
          pool, (k, v) -> v == null ? 0L : v + afterUserTime - beforeUserTime);
    }

    double processCpuLoad =
        metricService.getAutoGauge("process_cpu_load", MetricLevel.CORE, "name", "process").value();
    for (Map.Entry<String, Long> entry : moduleIncrementCpuTimeMap.entrySet()) {
      moduleCpuTimePercentageMap.put(
          entry.getKey(), entry.getValue() * 1.0 / totalIncrementTime * processCpuLoad);
    }
    for (Map.Entry<String, Long> entry : poolIncrementCpuTimeMap.entrySet()) {
      poolCpuUsageMap.put(entry.getKey(), entry.getValue() * 1.0 / totalIncrementTime);
      poolUserTimePercentageMap.put(
          entry.getKey(), poolIncrementUserTimeMap.get(entry.getKey()) * 1.0 / entry.getValue());
    }
  }
}
