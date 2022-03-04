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

package org.apache.iotdb.metrics.micrometer.reporter;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.Reporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class MicrometerJmxReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerJmxReporter.class);
  private MetricManager metricManager;

  @Override
  public boolean start() {
    try {
      Set<MeterRegistry> meterRegistrySet =
          Metrics.globalRegistry.getRegistries().stream()
              .filter(reporter -> reporter instanceof JmxMeterRegistry)
              .collect(Collectors.toSet());
      for (MeterRegistry meterRegistry : meterRegistrySet) {
        if (meterRegistry.isClosed()) {
          ((JmxMeterRegistry) meterRegistry).start();
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to start Micrometer JmxReporter, because {}", e.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public boolean stop() {
    try {
      Set<MeterRegistry> meterRegistrySet =
          Metrics.globalRegistry.getRegistries().stream()
              .filter(reporter -> reporter instanceof JmxMeterRegistry)
              .collect(Collectors.toSet());
      for (MeterRegistry meterRegistry : meterRegistrySet) {
        if (!meterRegistry.isClosed()) {
          ((JmxMeterRegistry) meterRegistry).stop();
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to stop Micrometer JmxReporter, because {}", e.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.jmx;
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.metricManager = metricManager;
  }
}
