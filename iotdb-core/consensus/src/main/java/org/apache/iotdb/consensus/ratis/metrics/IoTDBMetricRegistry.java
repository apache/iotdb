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

package org.apache.iotdb.consensus.ratis.metrics;

import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.Timekeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class IoTDBMetricRegistry implements RatisMetricRegistry {

  private final AbstractMetricService metricService;
  private final MetricRegistryInfo info;
  private final String prefix;
  private final Map<String, String> metricNameCache = new ConcurrentHashMap<>();
  private final Map<String, CounterProxy> counterCache = new ConcurrentHashMap<>();
  private final Map<String, TimerProxy> timerCache = new ConcurrentHashMap<>();
  private final Map<String, Boolean> gaugeCache = new ConcurrentHashMap<>();

  /** Time taken to flush log. */
  public static final String RAFT_LOG_FLUSH_TIME = "flushTime";

  /** Size of SegmentedRaftLogCache::closedSegments in bytes. */
  public static final String RAFT_LOG_CACHE_CLOSED_SEGMENTS_SIZE_IN_BYTES =
      "closedSegmentsSizeInBytes";

  /** Size of SegmentedRaftLogCache::openSegment in bytes. */
  public static final String RAFT_LOG_CACHE_OPEN_SEGMENT_SIZE_IN_BYTES = "openSegmentSizeInBytes";

  /** Total time taken to append a raft log entry. */
  public static final String RAFT_LOG_APPEND_ENTRY_LATENCY = "appendEntryLatency";

  /**
   * Time taken for a Raft log operation to get into the queue after being requested. This is the
   * time that it has to wait for the queue to be non-full.
   */
  public static final String RAFT_LOG_TASK_ENQUEUE_DELAY = "queueingDelay";

  /** Time spent by a Raft log operation in the queue. */
  public static final String RAFT_LOG_TASK_QUEUE_TIME = "enqueuedTime";

  /** Time taken for a Raft log operation to complete execution. */
  public static final String RAFT_LOG_TASK_EXECUTION_TIME = "ExecutionTime";

  /** Time taken for followers to append log entries. */
  public static final String FOLLOWER_APPEND_ENTRIES_LATENCY = "follower_append_entry_latency";

  /** Time taken to process write requests from client. */
  public static final String RAFT_CLIENT_WRITE_REQUEST = "clientWriteRequest";

  private static final List<String> RATIS_METRICS = new ArrayList<>();

  static {
    RATIS_METRICS.add(RAFT_LOG_FLUSH_TIME);
    RATIS_METRICS.add(RAFT_LOG_CACHE_CLOSED_SEGMENTS_SIZE_IN_BYTES);
    RATIS_METRICS.add(RAFT_LOG_CACHE_OPEN_SEGMENT_SIZE_IN_BYTES);
    RATIS_METRICS.add(RAFT_LOG_APPEND_ENTRY_LATENCY);
    RATIS_METRICS.add(RAFT_LOG_TASK_ENQUEUE_DELAY);
    RATIS_METRICS.add(RAFT_LOG_TASK_QUEUE_TIME);
    RATIS_METRICS.add(RAFT_LOG_TASK_EXECUTION_TIME);
    RATIS_METRICS.add(FOLLOWER_APPEND_ENTRIES_LATENCY);
    RATIS_METRICS.add(RAFT_CLIENT_WRITE_REQUEST);
  }

  IoTDBMetricRegistry(MetricRegistryInfo info, AbstractMetricService service) {
    this.info = info;
    this.metricService = service;
    this.prefix =
        Utils.getConsensusGroupTypeFromPrefix(info.getPrefix()).toString()
            + info.getApplicationName()
            + info.getMetricsComponentName();
  }

  private String getMetricName(String name) {
    return metricNameCache.computeIfAbsent(name, n -> this.prefix + n);
  }

  public MetricLevel getMetricLevel(String name) {
    for (String ratisMetric : RATIS_METRICS) {
      if (name.contains(ratisMetric)) {
        return MetricLevel.IMPORTANT;
      }
    }
    return MetricLevel.CORE;
  }

  @Override
  public Timekeeper timer(String name) {
    final String fullName = getMetricName(name);
    return timerCache.computeIfAbsent(
        fullName,
        fn -> new TimerProxy(metricService.getOrCreateTimer(fn, getMetricLevel(fullName))));
  }

  @Override
  public LongCounter counter(String name) {
    final String fullName = getMetricName(name);
    return counterCache.computeIfAbsent(
        fullName,
        fn ->
            new CounterProxy(
                metricService.getOrCreateCounter(getMetricName(name), getMetricLevel(fullName))));
  }

  @Override
  public boolean remove(String name) {
    // Currently MetricService in IoTDB does not support to remove a metric by its name only.
    // Therefore, we are trying every potential type here util we remove it successfully.
    // Since metricService.remove will throw an IllegalArgument when type mismatches, so use three
    // independent try-clauses
    // TODO (szywilliam) we can add an interface like removeTypeless(name)
    try {
      metricService.remove(MetricType.COUNTER, getMetricName(name));
    } catch (IllegalArgumentException ignored) {
    }
    try {
      metricService.remove(MetricType.TIMER, getMetricName(name));
    } catch (IllegalArgumentException ignored) {
    }
    try {
      metricService.remove(MetricType.AUTO_GAUGE, getMetricName(name));
    } catch (IllegalArgumentException ignored) {
    }

    return true;
  }

  @Override
  public <T> void gauge(String name, Supplier<Supplier<T>> supplier) {
    final String fullName = getMetricName(name);
    gaugeCache.computeIfAbsent(
        fullName,
        fn -> {
          final GaugeProxy<T> gauge = new GaugeProxy<>(supplier);
          metricService.createAutoGauge(fn, getMetricLevel(fn), gauge, GaugeProxy::getDoubleValue);
          return true;
        });
  }

  @Override
  public MetricRegistryInfo getMetricRegistryInfo() {
    return info;
  }

  void removeAll() {
    counterCache.forEach((name, counter) -> metricService.remove(MetricType.COUNTER, name));
    gaugeCache.forEach((name, gauge) -> metricService.remove(MetricType.AUTO_GAUGE, name));
    timerCache.forEach((name, timer) -> metricService.remove(MetricType.TIMER, name));
    metricNameCache.clear();
    counterCache.clear();
    gaugeCache.clear();
    timerCache.clear();
  }
}
