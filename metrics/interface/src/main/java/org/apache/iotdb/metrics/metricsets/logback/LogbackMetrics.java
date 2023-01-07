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

package org.apache.iotdb.metrics.metricsets.logback;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricType;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** This file is modified from io.micrometer.core.instrument.binder.logging.LogbackMetrics */
public class LogbackMetrics implements IMetricSet {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LogbackMetrics.class);
  static ThreadLocal<Boolean> ignoreMetrics = new ThreadLocal<>();
  private final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
  private final Map<AbstractMetricService, MetricsTurboFilter> metricsTurboFilters =
      new HashMap<>();

  public LogbackMetrics() {
    loggerContext.addListener(
        new LoggerContextListener() {
          @Override
          public boolean isResetResistant() {
            return true;
          }

          @Override
          public void onReset(LoggerContext context) {
            // re-add turbo filter because reset clears the turbo filter list
            synchronized (metricsTurboFilters) {
              for (MetricsTurboFilter addMetricsTurboFilter : metricsTurboFilters.values()) {
                loggerContext.addTurboFilter(addMetricsTurboFilter);
              }
            }
          }

          @Override
          public void onStart(LoggerContext context) {
            // no-op
          }

          @Override
          public void onStop(LoggerContext context) {
            // no-op
          }

          @Override
          public void onLevelChange(Logger logger, Level level) {
            // no-op
          }
        });
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    MetricsTurboFilter filter = new MetricsTurboFilter(metricService);
    synchronized (metricsTurboFilters) {
      metricsTurboFilters.put(metricService, filter);
      loggerContext.addTurboFilter(filter);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    try {
      synchronized (metricsTurboFilters) {
        for (MetricsTurboFilter addMetricsTurboFilter : metricsTurboFilters.values()) {
          loggerContext.getTurboFilterList().remove(addMetricsTurboFilter);
        }
        metricService.remove(MetricType.COUNTER, "logback_events", "level", "error");
        metricService.remove(MetricType.COUNTER, "logback_events", "level", "warn");
        metricService.remove(MetricType.COUNTER, "logback_events", "level", "info");
        metricService.remove(MetricType.COUNTER, "logback_events", "level", "debug");
        metricService.remove(MetricType.COUNTER, "logback_events", "level", "trace");
      }
    } catch (Exception e) {
      logger.warn("Failed to remove logBackMetrics, because ", e);
    }
  }
}
