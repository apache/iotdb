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

package org.apache.iotdb.metrics.metricsets.jvm;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.MetricConstant;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** This file is modified from io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics */
public class JvmThreadMetrics implements IMetricSet {
  private static long lastUpdateTime = 0L;
  private static final Map<Thread.State, Integer> threadStateCountMap =
      new EnumMap<>(Thread.State.class);

  @Override
  public void bindTo(AbstractMetricService metricService) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    metricService.createAutoGauge(
        SystemMetric.JVM_THREADS_PEAK_THREADS.toString(),
        MetricLevel.CORE,
        threadBean,
        ThreadMXBean::getPeakThreadCount);

    metricService.createAutoGauge(
        SystemMetric.JVM_THREADS_DAEMON_THREADS.toString(),
        MetricLevel.CORE,
        threadBean,
        ThreadMXBean::getDaemonThreadCount);

    metricService.createAutoGauge(
        SystemMetric.JVM_THREADS_LIVE_THREADS.toString(),
        MetricLevel.CORE,
        threadBean,
        ThreadMXBean::getThreadCount);

    try {
      threadBean.getAllThreadIds();
      for (Thread.State state : Thread.State.values()) {
        metricService.createAutoGauge(
            SystemMetric.JVM_THREADS_STATUS_THREADS.toString(),
            MetricLevel.CORE,
            threadBean,
            bean -> getThreadStateCount(bean, state),
            SystemTag.STATE.toString(),
            getStateTagValue(state));
      }
    } catch (Exception exception) {
      // An error will be thrown for unsupported operations
      // e.g. SubstrateVM does not support getAllThreadIds
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    metricService.remove(MetricType.AUTO_GAUGE, SystemMetric.JVM_THREADS_PEAK_THREADS.toString());
    metricService.remove(MetricType.AUTO_GAUGE, SystemMetric.JVM_THREADS_DAEMON_THREADS.toString());
    metricService.remove(MetricType.AUTO_GAUGE, SystemMetric.JVM_THREADS_LIVE_THREADS.toString());

    try {
      threadBean.getAllThreadIds();
      for (Thread.State state : Thread.State.values()) {
        metricService.remove(
            MetricType.AUTO_GAUGE,
            SystemMetric.JVM_THREADS_STATUS_THREADS.toString(),
            SystemTag.STATE.toString(),
            getStateTagValue(state));
      }
    } catch (Exception exception) {
      // An error will be thrown for unsupported operations
      // e.g. SubstrateVM does not support getAllThreadIds
    }
  }

  // VisibleForTesting
  static long getThreadStateCount(ThreadMXBean threadBean, Thread.State state) {
    checkAndUpdate(threadBean);
    return threadStateCountMap.getOrDefault(state, 0);
  }

  private static void checkAndUpdate(ThreadMXBean threadBean) {
    if (System.currentTimeMillis() - lastUpdateTime < MetricConstant.UPDATE_INTERVAL) {
      return;
    }
    lastUpdateTime = System.currentTimeMillis();
    threadStateCountMap.clear();
    List<ThreadInfo> infoList =
        Arrays.asList(threadBean.getThreadInfo(threadBean.getAllThreadIds()));
    infoList.forEach(
        info -> {
          if (info != null) {
            Thread.State state = info.getThreadState();
            threadStateCountMap.compute(state, (k, v) -> v == null ? 1 : v + 1);
          }
        });
  }

  private static String getStateTagValue(Thread.State state) {
    return state.name().toLowerCase().replace("_", "-");
  }
}
