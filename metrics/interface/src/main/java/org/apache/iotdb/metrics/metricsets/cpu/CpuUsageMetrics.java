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
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

public class CpuUsageMetrics implements IMetricSet {

  private static final String MODULE_CPU_USAGE = "module_cpu_usage";
  private static final String POOL_CPU_USAGE = "pool_cpu_usage";
  private static final String POOL = "pool";
  private static final String MODULE = "module";
  private static final String MODULE_USER_TIME_PERCENTAGE = "module_user_time_percentage";
  private static final String POOL_USER_TIME_PERCENTAGE = "user_time_percentage";
  private final List<String> modules;
  private final List<String> pools;
  private static final long UPDATE_INTERVAL = 10_000L;
  protected AbstractMetricService metricService;
  protected final UnaryOperator<String> threadNameToModule;
  protected final UnaryOperator<String> threadNameToPool;
  protected final Map<Long, String> threadIdToModuleCache = new HashMap<>();
  protected final Map<Long, String> threadIdToPoolCache = new HashMap<>();
  private final Map<String, Double> moduleCpuTimePercentageMap = new HashMap<>();
  private final Map<String, Double> moduleUserTimePercentageMap = new HashMap<>();
  private final Map<String, Double> poolCpuUsageMap = new HashMap<>();
  private final Map<String, Double> poolUserTimePercentageMap = new HashMap<>();
  private final Map<Long, Long> lastThreadCpuTime = new HashMap<>();
  private final Map<Long, Long> lastThreadUserTime = new HashMap<>();
  AutoGauge processCpuLoadGauge = null;
  private final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
  private AtomicLong lastUpdateTime = new AtomicLong(0L);

  public CpuUsageMetrics(
      List<String> modules,
      List<String> pools,
      UnaryOperator<String> threadNameToModule,
      UnaryOperator<String> threadNameToPool) {
    this.modules = modules;
    this.pools = pools;
    this.threadNameToModule = threadNameToModule;
    this.threadNameToPool = threadNameToPool;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    for (String moduleName : modules) {
      metricService.createAutoGauge(
          MODULE_CPU_USAGE,
          MetricLevel.IMPORTANT,
          this,
          x -> x.getModuleCpuUsage().getOrDefault(moduleName, 0.0),
          MODULE,
          moduleName);
      metricService.createAutoGauge(
          MODULE_USER_TIME_PERCENTAGE,
          MetricLevel.IMPORTANT,
          this,
          x -> x.getModuleUserTimePercentage().getOrDefault(moduleName, 0.0),
          MODULE,
          moduleName);
    }
    for (String poolName : pools) {
      metricService.createAutoGauge(
          POOL_CPU_USAGE,
          MetricLevel.IMPORTANT,
          this,
          x -> x.getPoolCpuUsage().getOrDefault(poolName, 0.0),
          POOL,
          poolName);
      metricService.createAutoGauge(
          POOL_USER_TIME_PERCENTAGE,
          MetricLevel.IMPORTANT,
          this,
          x -> x.getPoolUserCpuPercentage().getOrDefault(poolName, 0.0),
          POOL,
          poolName);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String moduleName : modules) {
      metricService.remove(MetricType.AUTO_GAUGE, MODULE_CPU_USAGE, MODULE, moduleName);
      metricService.remove(MetricType.AUTO_GAUGE, MODULE_USER_TIME_PERCENTAGE, MODULE, moduleName);
    }
    for (String poolName : pools) {
      metricService.remove(MetricType.AUTO_GAUGE, POOL_CPU_USAGE, POOL, poolName);
      metricService.remove(MetricType.AUTO_GAUGE, POOL_USER_TIME_PERCENTAGE, POOL, poolName);
    }
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
      updateCpuUsage();
    }
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

    Map<Long, Long> currentThreadCpuTime = new HashMap<>(taskIds.length + 1, 1.0f);
    Map<Long, Long> currentThreadUserTime = new HashMap<>(taskIds.length + 1, 1.0f);
    collectThreadCpuInfo(currentThreadCpuTime, currentThreadUserTime, threadInfos);

    Map<String, Long> moduleIncrementCpuTimeMap = new HashMap<>(modules.size() + 1, 1.0f);
    Map<String, Long> moduleIncrementUserTimeMap = new HashMap<>(modules.size() + 1, 1.0f);
    Map<String, Long> poolIncrementCpuTimeMap = new HashMap<>(pools.size() + 1, 1.0f);
    Map<String, Long> poolIncrementUserTimeMap = new HashMap<>(pools.size() + 1, 1.0f);

    long totalIncrementTime =
        computeUsageInfoForModuleAndPool(
            moduleIncrementCpuTimeMap,
            moduleIncrementUserTimeMap,
            poolIncrementCpuTimeMap,
            poolIncrementUserTimeMap,
            lastThreadCpuTime,
            lastThreadUserTime,
            currentThreadCpuTime,
            currentThreadUserTime,
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
    lastThreadCpuTime.clear();
    lastThreadCpuTime.putAll(currentThreadCpuTime);
    lastThreadUserTime.putAll(currentThreadUserTime);
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

  @SuppressWarnings("java:S107")
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
      long beforeCpuTime = beforeThreadCpuTime.getOrDefault(id, 0L);
      long afterCpuTime = afterThreadCpuTime.get(id);
      long beforeUserTime = beforeThreadUserTime.getOrDefault(id, 0L);
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
    if (processCpuLoadGauge == null) {
      processCpuLoadGauge =
          metricService.getAutoGauge("process_cpu_load", MetricLevel.CORE, "name", "process");
    }
    double processCpuLoad = processCpuLoadGauge.value();
    for (Map.Entry<String, Long> entry : moduleIncrementCpuTimeMap.entrySet()) {
      moduleCpuTimePercentageMap.put(
          entry.getKey(), entry.getValue() * 1.0 / totalIncrementTime * processCpuLoad);
      if (entry.getValue() > 0.0) {
        moduleUserTimePercentageMap.put(
            entry.getKey(),
            moduleIncrementUserTimeMap.get(entry.getKey()) * 1.0 / entry.getValue());
      } else {
        moduleUserTimePercentageMap.put(entry.getKey(), 0.0);
      }
    }
    for (Map.Entry<String, Long> entry : poolIncrementCpuTimeMap.entrySet()) {
      poolCpuUsageMap.put(
          entry.getKey(), entry.getValue() * 1.0 / totalIncrementTime * processCpuLoad);
      if (entry.getValue() > 0.0) {
        poolUserTimePercentageMap.put(
            entry.getKey(), poolIncrementUserTimeMap.get(entry.getKey()) * 1.0 / entry.getValue());
      } else {
        poolUserTimePercentageMap.put(entry.getKey(), 0.0);
      }
    }
  }
}
