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

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class MicrometerJmxReporter implements JmxReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerJmxReporter.class);

  @Override
  public boolean start() {
    try {
      Set<MeterRegistry> meterRegistrySet =
          Metrics.globalRegistry.getRegistries().stream()
              .filter(reporter -> reporter instanceof JmxMeterRegistry)
              .collect(Collectors.toSet());
      if (meterRegistrySet.size() != 0) {
        LOGGER.warn("Micrometer JmxReporter already start!");
        return false;
      }
      Metrics.addRegistry(new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM));
    } catch (Exception e) {
      LOGGER.warn("Micrometer JmxReporter failed to start, because ", e);
      return false;
    }
    LOGGER.info("Micrometer JmxReporter start!");
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
          meterRegistry.close();
          Metrics.removeRegistry(meterRegistry);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Micrometer JmxReporter failed to stop, because ", e);
      return false;
    }
    LOGGER.info("Micrometer JmxReporter stop!");
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.JMX;
  }

  @Override
  public void setMetricManager(AbstractMetricManager metricManager) {
    // do nothing
  }
}
