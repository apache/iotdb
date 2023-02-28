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

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;

public class IoTDBMetricRegistry implements RatisMetricRegistry {

  private final AbstractMetricService metricService;
  private final MetricRegistryInfo info;
  private final String prefix;
  private final Map<String, String> metricNameCache = new ConcurrentHashMap<>();
  private final Map<String, CounterProxy> counterCache = new ConcurrentHashMap<>();
  private final Map<String, TimerProxy> timerCache = new ConcurrentHashMap<>();
  private final Map<String, GaugeProxy> gaugeCache = new ConcurrentHashMap<>();

  IoTDBMetricRegistry(
      MetricRegistryInfo info, AbstractMetricService service, String consensusGroupType) {
    this.info = info;
    this.metricService = service;
    prefix =
        MetricRegistry.name(
            consensusGroupType,
            info.getApplicationName(),
            info.getMetricsComponentName(),
            info.getPrefix());
  }

  private String getMetricName(String name) {
    return metricNameCache.computeIfAbsent(name, n -> MetricRegistry.name(prefix, n));
  }

  @Override
  public Timer timer(String name) {
    final String fullName = getMetricName(name);
    return timerCache.computeIfAbsent(
        fullName, fn -> new TimerProxy(metricService.getOrCreateTimer(fn, MetricLevel.IMPORTANT)));
  }

  @Override
  public Counter counter(String name) {
    final String fullName = getMetricName(name);
    return counterCache.computeIfAbsent(
        fullName,
        fn ->
            new CounterProxy(
                metricService.getOrCreateCounter(getMetricName(name), MetricLevel.IMPORTANT)));
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

  void removeAll() {
    counterCache.forEach((name, counter) -> metricService.remove(MetricType.COUNTER, name));
    gaugeCache.forEach((name, gauge) -> metricService.remove(MetricType.AUTO_GAUGE, name));
    timerCache.forEach((name, timer) -> metricService.remove(MetricType.TIMER, name));
    metricNameCache.clear();
    counterCache.clear();
    gaugeCache.clear();
    timerCache.clear();
  }

  @Override
  public Gauge gauge(String name, MetricRegistry.MetricSupplier<Gauge> metricSupplier) {
    final String fullName = getMetricName(name);
    return gaugeCache.computeIfAbsent(
        fullName,
        gaugeName -> {
          final GaugeProxy gauge = new GaugeProxy(metricSupplier);
          metricService.createAutoGauge(
              gaugeName, MetricLevel.IMPORTANT, gauge, GaugeProxy::getValueAsDouble);
          return gauge;
        });
  }

  @Override
  public Timer timer(String name, MetricRegistry.MetricSupplier<Timer> metricSupplier) {
    throw new UnsupportedOperationException("This method is not used in IoTDB project");
  }

  @Override
  public SortedMap<String, Gauge> getGauges(MetricFilter metricFilter) {
    throw new UnsupportedOperationException("This method is not used in IoTDB project");
  }

  @Override
  public Counter counter(String name, MetricRegistry.MetricSupplier<Counter> metricSupplier) {
    throw new UnsupportedOperationException("This method is not used in IoTDB project");
  }

  @Override
  public Histogram histogram(String name) {
    throw new UnsupportedOperationException("Histogram is not used in Ratis Metrics");
  }

  @Override
  public Meter meter(String name) {
    throw new UnsupportedOperationException("Meter is not used in Ratis Metrics");
  }

  @Override
  public Meter meter(String name, MetricRegistry.MetricSupplier<Meter> metricSupplier) {
    throw new UnsupportedOperationException("Meter is not used in Ratis Metrics");
  }

  @Override
  public Metric get(String name) {
    throw new UnsupportedOperationException("Meter is not used in Ratis Metrics");
  }

  @Override
  public <T extends Metric> T register(String name, T t) throws IllegalArgumentException {
    throw new UnsupportedOperationException("register is not used in Ratis Metrics");
  }

  @Override
  public MetricRegistry getDropWizardMetricRegistry() {
    throw new UnsupportedOperationException("This method is not used in IoTDB project");
  }

  @Override
  public MetricRegistryInfo getMetricRegistryInfo() {
    return info;
  }

  @Override
  public void registerAll(String s, MetricSet metricSet) {
    throw new UnsupportedOperationException("registerAll is not used in Ratis Metrics");
  }

  @Override
  public void setJmxReporter(JmxReporter jmxReporter) {
    throw new UnsupportedOperationException("JmxReporter is not used in Ratis Metrics");
  }

  @Override
  public JmxReporter getJmxReporter() {
    throw new UnsupportedOperationException("JmxReporter is not used in Ratis Metrics");
  }

  @Override
  public void setConsoleReporter(ConsoleReporter consoleReporter) {
    throw new UnsupportedOperationException("ConsoleReporter is not used in Ratis Metrics");
  }

  @Override
  public ConsoleReporter getConsoleReporter() {
    throw new UnsupportedOperationException("ConsoleReporter is not used in Ratis Metrics");
  }
}
