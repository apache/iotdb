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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ProcedureMetrics implements IMetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureMetrics.class);

  private final ProcedureManager procedureManager;

  private final ProcedureExecutor<ConfigNodeProcedureEnv> executor;

  private final ProcedureScheduler scheduler;

  private final Map<String, ProcedureMetricItems> metricItemsMap = new ConcurrentHashMap<>();

  private AbstractMetricService metricService;

  public ProcedureMetrics(ProcedureManager procedureManager) {
    this.procedureManager = procedureManager;
    this.executor = procedureManager.getExecutor();
    this.scheduler = procedureManager.getScheduler();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    bindThreadMetrics(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindThreadMetrics(metricService);

    for (ProcedureMetricItems items : metricItemsMap.values()) {
      items.unbindMetricItems(metricService);
    }
    metricItemsMap.clear();
  }

  private void bindThreadMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.PROCEDURE_WORKER_THREAD_COUNT.toString(),
        MetricLevel.CORE,
        executor,
        ProcedureExecutor::getWorkerThreadCount);
    metricService.createAutoGauge(
        Metric.PROCEDURE_ACTIVE_WORKER_THREAD_COUNT.toString(),
        MetricLevel.CORE,
        executor,
        ProcedureExecutor::getActiveWorkerThreadCount);
    metricService.createAutoGauge(
        Metric.PROCEDURE_QUEUE_LENGTH.toString(),
        MetricLevel.CORE,
        scheduler,
        ProcedureScheduler::size);
  }

  private void unbindThreadMetrics(AbstractMetricService metricService) {
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PROCEDURE_WORKER_THREAD_COUNT.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PROCEDURE_ACTIVE_WORKER_THREAD_COUNT.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PROCEDURE_QUEUE_LENGTH.toString());
  }

  public void updateMetricsOnSubmit(String procType) {
    Optional.ofNullable(metricService)
        .ifPresent(
            service -> {
              metricItemsMap
                  .computeIfAbsent(procType, k -> new ProcedureMetricItems(k, service))
                  .updateMetricsOnSubmit();
            });
  }

  public void updateMetricsOnFinish(String procType, long runtime, boolean success) {
    Optional.ofNullable(metricService)
        .ifPresent(
            service -> {
              metricItemsMap
                  .computeIfAbsent(procType, k -> new ProcedureMetricItems(k, service))
                  .updateMetricsOnFinish(runtime, success);
            });
  }

  class ProcedureMetricItems {
    private final String procType;
    private Counter submittedCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    private Counter failedCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    private Timer executionTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    public ProcedureMetricItems(String procType, AbstractMetricService metricService) {
      this.procType = procType;
      bindMetricItems(metricService);
    }

    public void bindMetricItems(AbstractMetricService metricService) {
      this.submittedCounter =
          metricService.getOrCreateCounter(
              Metric.PROCEDURE_SUBMITTED_COUNT.toString(),
              MetricLevel.CORE,
              Tag.TYPE.toString(),
              procType);
      this.failedCounter =
          metricService.getOrCreateCounter(
              Metric.PROCEDURE_FAILED_COUNT.toString(),
              MetricLevel.CORE,
              Tag.TYPE.toString(),
              procType);
      this.executionTimer =
          metricService.getOrCreateTimer(
              Metric.PROCEDURE_EXECUTION_TIME.toString(),
              MetricLevel.CORE,
              Tag.TYPE.toString(),
              procType);
    }

    public void unbindMetricItems(AbstractMetricService metricService) {
      metricService.remove(
          MetricType.COUNTER,
          Metric.PROCEDURE_SUBMITTED_COUNT.toString(),
          Tag.TYPE.toString(),
          procType);
      metricService.remove(
          MetricType.COUNTER,
          Metric.PROCEDURE_FAILED_COUNT.toString(),
          Tag.TYPE.toString(),
          procType);
      metricService.remove(
          MetricType.TIMER,
          Metric.PROCEDURE_EXECUTION_TIME.toString(),
          Tag.TYPE.toString(),
          procType);
    }

    public void updateMetricsOnSubmit() {
      submittedCounter.inc();
    }

    public void updateMetricsOnFinish(long runtime, boolean success) {
      if (success) {
        executionTimer.updateMillis(runtime);
      } else {
        failedCounter.inc();
      }
    }
  }
}
