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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public class MetricRegistryManager extends MetricRegistries {
  /** Using RefCountingMap here because of potential duplicate MetricRegistryInfos */
  private final RefCountingMap<MetricRegistryInfo, RatisMetricRegistry> registries;
  /** TODO: enable ratis metrics after verifying its correctness and efficiency */
  private final AbstractMetricService service = new DoNothingMetricService();

  private String consensusGroupType;

  public MetricRegistryManager() {
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
        () -> new IoTDBMetricRegistry(metricRegistryInfo, service, consensusGroupType));
  }

  @Override
  public boolean remove(MetricRegistryInfo metricRegistryInfo) {
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
    throw new UnsupportedOperationException("Reporter is disabled from RatisMetricRegistries");
  }

  @Override
  public void enableJmxReporter() {
    // We shall disable the JMX reporter since we already have one in MetricService
    throw new UnsupportedOperationException("JMX Reporter is disabled from RatisMetricRegistries");
  }

  @Override
  public void enableConsoleReporter(TimeDuration timeDuration) {
    // We shall disable the Console reporter since we already have one in MetricService
    throw new UnsupportedOperationException(
        "Console Reporter is disabled from RatisMetricRegistries");
  }

  public void setConsensusGroupType(String consensusGroupType) {
    this.consensusGroupType = consensusGroupType;
  }
}
