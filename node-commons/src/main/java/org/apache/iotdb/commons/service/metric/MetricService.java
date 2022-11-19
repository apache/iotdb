/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.service.metric;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.impl.DoNothingMetric;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public class MetricService extends AbstractMetricService implements MetricServiceMBean, IService {
  private static final Logger logger = LoggerFactory.getLogger(MetricService.class);
  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());
  private InternalReporter internalReporter = new DoNothingInternalReporter();

  private MetricService() {}

  @Override
  public void start() throws StartupException {
    try {
      logger.info("Start to start metric Service.");
      JMXService.registerMBean(getInstance(), mbeanName);
      startService();
      logger.info("Finish start metric Service");
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  public void restart() {
    logger.info("Restart metric Service.");
    restartService();
    logger.info("Finish restart metric Service");
  }

  /** restart metric service */
  public void restartService() {
    logger.info("Restart Core Module");
    stopCoreModule();
    internalReporter.clear();
    startCoreModule();
    for (IMetricSet metricSet : metricSets) {
      logger.info("Restart metricSet: {}", metricSet.getClass().getName());
      metricSet.unbindFrom(this);
      metricSet.bindTo(this);
    }
  }

  @Override
  public void stop() {
    logger.info("Stop metric Service.");
    internalReporter.stop();
    internalReporter = new DoNothingInternalReporter();
    stopService();
    JMXService.deregisterMBean(mbeanName);
    logger.info("Finish stop metric Service");
  }

  public Counter getOrCreateCounterWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Counter counter = metricManager.getOrCreateCounter(metric, metricLevel, tags);
    report(counter, metric, tags);
    return counter;
  }

  public <T> Gauge getOrCreateAutoGaugeWithInternalReport(
      String metric, MetricLevel metricLevel, T obj, ToLongFunction<T> mapper, String... tags) {
    Gauge gauge = metricManager.getOrCreateAutoGauge(metric, metricLevel, obj, mapper, tags);
    report(gauge, metric, tags);
    return gauge;
  }

  public Gauge getOrCreateGaugeWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Gauge gauge = metricManager.getOrCreateGauge(metric, metricLevel, tags);
    report(gauge, metric, tags);
    return gauge;
  }

  public Rate getOrCreateRateWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Rate rate = metricManager.getOrCreateRate(metric, metricLevel, tags);
    report(rate, metric, tags);
    return rate;
  }

  public Histogram getOrCreateHistogramWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Histogram histogram = metricManager.getOrCreateHistogram(metric, metricLevel, tags);
    report(histogram, metric, tags);
    return histogram;
  }

  public Timer getOrCreateTimerWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Timer timer = metricManager.getOrCreateTimer(metric, metricLevel, tags);
    report(timer, metric, tags);
    return timer;
  }

  public void countWithInternalReport(
      long delta, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.count(delta, metric, metricLevel, tags), metric, tags);
  }

  public void gaugeWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.gauge(value, metric, metricLevel, tags), metric, tags);
  }

  public void rateWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.rate(value, metric, metricLevel, tags), metric, tags);
  }

  public void histogramWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.histogram(value, metric, metricLevel, tags), metric, tags);
  }

  public void timerWithInternalReport(
      long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.timer(delta, timeUnit, metric, metricLevel, tags), metric, tags);
  }

  private void report(IMetric metric, String name, String... tags) {
    if (metric instanceof DoNothingMetric) {
      return;
    }
    if (metric instanceof Counter) {
      Counter counter = (Counter) metric;
      internalReporter.updateValue(name, counter.count(), TSDataType.INT64, tags);
    } else if (metric instanceof Gauge) {
      Gauge gauge = (Gauge) metric;
      if (metric instanceof AutoGauge) {
        internalReporter.addAutoGauge((Gauge) metric, name, tags);
      } else {
        internalReporter.updateValue(name, gauge.value(), TSDataType.INT64, tags);
      }
    } else if (metric instanceof Rate) {
      Rate rate = (Rate) metric;
      Long time = System.currentTimeMillis();
      internalReporter.updateValue(name + "_count", rate.getCount(), TSDataType.INT64, time, tags);
      internalReporter.updateValue(
          name + "_mean", rate.getMeanRate(), TSDataType.DOUBLE, time, tags);
      internalReporter.updateValue(
          name + "_1min", rate.getOneMinuteRate(), TSDataType.DOUBLE, time, tags);
      internalReporter.updateValue(
          name + "_5min", rate.getFiveMinuteRate(), TSDataType.DOUBLE, time, tags);
      internalReporter.updateValue(
          name + "_15min", rate.getFifteenMinuteRate(), TSDataType.DOUBLE, time, tags);
    } else if (metric instanceof Histogram) {
      Histogram histogram = (Histogram) metric;
      internalReporter.writeSnapshotAndCount(name, histogram.takeSnapshot(), tags);
    } else if (metric instanceof Timer) {
      Timer timer = (Timer) metric;
      internalReporter.writeSnapshotAndCount(name, timer.takeSnapshot(), tags);
    }
  }

  @Override
  public void reloadProperties(ReloadLevel reloadLevel) {
    logger.info("Reload properties of metric service");
    synchronized (this) {
      switch (reloadLevel) {
        case RESTART_METRIC:
          restart();
          break;
        case RESTART_REPORTER:
          stopAllReporter();
          loadReporter();
          startAllReporter();
          logger.info("Finish restart metric reporters.");
          break;
        case NOTHING:
          logger.debug("There are nothing change in metric module.");
          break;
        default:
          break;
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.METRIC_SERVICE;
  }

  public void updateInternalReporter(InternalReporter internalReporter) {
    this.internalReporter = internalReporter;
  }

  public static MetricService getInstance() {
    return MetricsServiceHolder.INSTANCE;
  }

  private static class MetricsServiceHolder {

    private static final MetricService INSTANCE = new MetricService();

    private MetricsServiceHolder() {}
  }
}
