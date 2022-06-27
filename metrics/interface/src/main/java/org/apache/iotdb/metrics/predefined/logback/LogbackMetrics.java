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

package org.apache.iotdb.metrics.predefined.logback;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.predefined.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.PredefinedMetric;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.HashMap;
import java.util.Map;

/** This file is modified from io.micrometer.core.instrument.binder.logging.LogbackMetrics */
public class LogbackMetrics implements IMetricSet, AutoCloseable {
  static ThreadLocal<Boolean> ignoreMetrics = new ThreadLocal<>();
  private final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
  private final Map<MetricManager, MetricsTurboFilter> metricsTurboFilters = new HashMap<>();

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
              for (MetricsTurboFilter metricsTurboFilter : metricsTurboFilters.values()) {
                loggerContext.addTurboFilter(metricsTurboFilter);
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
  public void bindTo(MetricManager metricManager) {
    MetricsTurboFilter filter = new MetricsTurboFilter(metricManager);
    synchronized (metricsTurboFilters) {
      metricsTurboFilters.put(metricManager, filter);
      loggerContext.addTurboFilter(filter);
    }
  }

  public static void ignoreMetrics(Runnable r) {
    ignoreMetrics.set(true);
    try {
      r.run();
    } finally {
      ignoreMetrics.remove();
    }
  }

  @Override
  public PredefinedMetric getType() {
    return PredefinedMetric.LOGBACK;
  }

  @Override
  public void close() throws Exception {
    synchronized (metricsTurboFilters) {
      for (MetricsTurboFilter metricsTurboFilter : metricsTurboFilters.values()) {
        loggerContext.getTurboFilterList().remove(metricsTurboFilter);
      }
    }
  }
}

class MetricsTurboFilter extends TurboFilter {
  private Counter errorCounter;
  private Counter warnCounter;
  private Counter infoCounter;
  private Counter debugCounter;
  private Counter traceCounter;

  MetricsTurboFilter(MetricManager metricManager) {
    errorCounter =
        metricManager.getOrCreateCounter("logback.events", MetricLevel.IMPORTANT, "level", "error");

    warnCounter =
        metricManager.getOrCreateCounter("logback.events", MetricLevel.IMPORTANT, "level", "warn");

    infoCounter =
        metricManager.getOrCreateCounter("logback.events", MetricLevel.IMPORTANT, "level", "info");

    debugCounter =
        metricManager.getOrCreateCounter("logback.events", MetricLevel.IMPORTANT, "level", "debug");

    traceCounter =
        metricManager.getOrCreateCounter("logback.events", MetricLevel.IMPORTANT, "level", "trace");
  }

  @Override
  public FilterReply decide(
      Marker marker, Logger logger, Level level, String format, Object[] params, Throwable t) {
    // When filter is asked for decision for an isDebugEnabled call or similar test, there is no
    // message (ie format)
    // and no intention to log anything with this call. We will not increment counters and can
    // return immediately and
    // avoid the relatively expensive ThreadLocal access below. See also logbacks
    // Logger.callTurboFilters().
    if (format == null) {
      return FilterReply.NEUTRAL;
    }

    Boolean ignored = LogbackMetrics.ignoreMetrics.get();
    if (ignored != null && ignored) {
      return FilterReply.NEUTRAL;
    }

    // cannot use logger.isEnabledFor(level), as it would cause a StackOverflowError by calling this
    // filter again!
    if (level.isGreaterOrEqual(logger.getEffectiveLevel())) {
      switch (level.toInt()) {
        case Level.ERROR_INT:
          errorCounter.inc();
          break;
        case Level.WARN_INT:
          warnCounter.inc();
          break;
        case Level.INFO_INT:
          infoCounter.inc();
          break;
        case Level.DEBUG_INT:
          debugCounter.inc();
          break;
        case Level.TRACE_INT:
          traceCounter.inc();
          break;
      }
    }

    return FilterReply.NEUTRAL;
  }
}
