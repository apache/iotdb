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
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.Marker;

public class MetricsTurboFilter extends TurboFilter {
  private final Counter errorCounter;
  private final Counter warnCounter;
  private final Counter infoCounter;
  private final Counter debugCounter;
  private final Counter traceCounter;

  MetricsTurboFilter(AbstractMetricService metricService) {
    errorCounter =
        metricService.getOrCreateCounter("logback_events", MetricLevel.CORE, "level", "error");

    warnCounter =
        metricService.getOrCreateCounter("logback_events", MetricLevel.CORE, "level", "warn");

    infoCounter =
        metricService.getOrCreateCounter("logback_events", MetricLevel.CORE, "level", "info");

    debugCounter =
        metricService.getOrCreateCounter("logback_events", MetricLevel.CORE, "level", "debug");

    traceCounter =
        metricService.getOrCreateCounter("logback_events", MetricLevel.CORE, "level", "trace");
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
        default:
          break;
      }
    }

    return FilterReply.NEUTRAL;
  }
}
