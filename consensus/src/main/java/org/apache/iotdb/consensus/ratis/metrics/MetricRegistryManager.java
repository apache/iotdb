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
import org.apache.iotdb.metrics.DoNothingMetricService;

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

class MetricRegistryManager extends MetricRegistries {

  private static final Logger LOG = LoggerFactory.getLogger(MetricRegistryManager.class);
  private final List<Consumer<RatisMetricRegistry>> reporterRegistrations =
      new CopyOnWriteArrayList<>();
  private final List<Consumer<RatisMetricRegistry>> stopReporters = new CopyOnWriteArrayList<>();
  private final RefCountingMap<MetricRegistryInfo, RatisMetricRegistry> registries;
  // TODO: enable ratis metrics after verifying its correctness and efficiency
  private final AbstractMetricService service = new DoNothingMetricService();

  MetricRegistryManager() {
    this.registries = new RefCountingMap<>();
  }

  @Override
  public void clear() {
    registries.values().stream()
        .map(registry -> (IoTDBMetricRegistry) registry)
        .forEach(IoTDBMetricRegistry::removeAll);
    this.registries.clear();
  }

  @Override
  public RatisMetricRegistry create(MetricRegistryInfo metricRegistryInfo) {
    return registries.put(
        metricRegistryInfo,
        () -> {
          RatisMetricRegistry registry = new IoTDBMetricRegistry(metricRegistryInfo, service);
          reporterRegistrations.forEach(reg -> reg.accept(registry));
          return registry;
        });
  }

  @Override
  public boolean remove(MetricRegistryInfo metricRegistryInfo) {
    RatisMetricRegistry registry = registries.get(metricRegistryInfo);
    if (registry != null) {
      stopReporters.forEach(reg -> reg.accept(registry));
    }

    return registries.remove(metricRegistryInfo) == null;
  }

  @Override
  public Optional<RatisMetricRegistry> get(MetricRegistryInfo metricRegistryInfo) {
    return Optional.ofNullable(registries.get(metricRegistryInfo));
  }

  @Override
  public Set<MetricRegistryInfo> getMetricRegistryInfos() {
    return Collections.unmodifiableSet(registries.keySet());
  }

  @Override
  public Collection<RatisMetricRegistry> getMetricRegistries() {
    return Collections.unmodifiableCollection(registries.values());
  }

  @Override
  public void addReporterRegistration(
      Consumer<RatisMetricRegistry> reporterRegistration,
      Consumer<RatisMetricRegistry> stopReporter) {
    if (registries.size() > 0) {
      LOG.warn(
          "New reporters are added after registries were created. Some metrics will be missing from the reporter. "
              + "Please add reporter before adding any new registry.");
    }
    this.reporterRegistrations.add(reporterRegistration);
    this.stopReporters.add(stopReporter);
  }

  @Override
  @Deprecated
  // TODO maybe we could implement it
  public void enableJmxReporter() {
    // We shall disable the JMX reporter since we already have one in MetricService
    throw new UnsupportedOperationException("JMX Reporter is disabled from RatisMetricRegistries");
  }

  @Override
  @Deprecated
  public void enableConsoleReporter(TimeDuration timeDuration) {
    // We shall disable the Console reporter since we already have one in MetricService
    throw new UnsupportedOperationException(
        "Console Reporter is disabled from RatisMetricRegistries");
  }
}
