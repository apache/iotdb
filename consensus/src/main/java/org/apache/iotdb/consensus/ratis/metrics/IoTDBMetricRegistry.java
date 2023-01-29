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

  private final Map<String, GaugeProxy> gaugeProxyMap = new ConcurrentHashMap<>();

  IoTDBMetricRegistry(MetricRegistryInfo info, AbstractMetricService service) {
    this.info = info;
    this.metricService = service;
    prefix =
        MetricRegistry.name(
            info.getApplicationName(), info.getMetricsComponentName(), info.getPrefix());
  }

  private String getMetricName(String name) {
    // TODO cache this
    return MetricRegistry.name(prefix, name);
  }
  private String getGaugeName(String name) {
    return MetricRegistry.name("GAUGE" ,getMetricName(name));
  }

  @Override
  public Timer timer(String name) {
    return new TimerProxy(
        metricService.getOrCreateTimer(getMetricName(name), MetricLevel.IMPORTANT));
  }

  @Override
  public Counter counter(String name) {
    return new CounterProxy(
        metricService.getOrCreateCounter(getMetricName(name), MetricLevel.IMPORTANT));
  }

  @Override
  public boolean remove(String name) {
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
  public Gauge gauge(String name, MetricRegistry.MetricSupplier<Gauge> metricSupplier) {
    GaugeProxy proxy = new GaugeProxy(metricSupplier);
    gaugeProxyMap.compute(getMetricName(name), (key, oldValue) -> proxy);
    metricService.createAutoGauge(getGaugeName(name), MetricLevel.IMPORTANT, proxy, GaugeProxy::getValueAsLong);
    return null;
  }

  @Override
  public Timer timer(String name, MetricRegistry.MetricSupplier<Timer> metricSupplier) {
    throw new UnsupportedOperationException("This method is not used in IoTDB project");
  }

  @Override
  public SortedMap<String, Gauge> getGauges(MetricFilter metricFilter) {
    return null;
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
    return null;
  }

  @Override
  public <T extends Metric> T register(String name, T t) throws IllegalArgumentException {
    return null;
  }

  @Override
  public MetricRegistry getDropWizardMetricRegistry() {
    return null;
  }

  @Override
  public MetricRegistryInfo getMetricRegistryInfo() {
    return null;
  }

  @Override
  public void registerAll(String s, MetricSet metricSet) {}

  @Override
  public void setJmxReporter(JmxReporter jmxReporter) {
    // Not Implemented: JmxReporter is not used in Ratis Metrics
  }

  @Override
  public JmxReporter getJmxReporter() {
    throw new UnsupportedOperationException("JmxReporter is not used in Ratis Metrics");
  }

  @Override
  public void setConsoleReporter(ConsoleReporter consoleReporter) {
    // Not Implemented: ConsoleReporter is not used in Ratis Metrics
  }

  @Override
  public ConsoleReporter getConsoleReporter() {
    throw new UnsupportedOperationException("ConsoleReporter is not used in Ratis Metrics");
  }
}
