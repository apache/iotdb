1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import org.apache.iotdb.commons.service.metric.enums.Metric;
1import org.apache.iotdb.commons.service.metric.enums.Tag;
1import org.apache.iotdb.confignode.manager.ProcedureManager;
1import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
1import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
1import org.apache.iotdb.metrics.AbstractMetricService;
1import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
1import org.apache.iotdb.metrics.metricsets.IMetricSet;
1import org.apache.iotdb.metrics.type.Counter;
1import org.apache.iotdb.metrics.type.Timer;
1import org.apache.iotdb.metrics.utils.MetricLevel;
1import org.apache.iotdb.metrics.utils.MetricType;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.util.Map;
1import java.util.Optional;
1import java.util.concurrent.ConcurrentHashMap;
1
1public class ProcedureMetrics implements IMetricSet {
1  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureMetrics.class);
1
1  private final ProcedureManager procedureManager;
1
1  private final ProcedureExecutor<ConfigNodeProcedureEnv> executor;
1
1  private final ProcedureScheduler scheduler;
1
1  private final Map<String, ProcedureMetricItems> metricItemsMap = new ConcurrentHashMap<>();
1
1  private AbstractMetricService metricService;
1
1  public ProcedureMetrics(ProcedureManager procedureManager) {
1    this.procedureManager = procedureManager;
1    this.executor = procedureManager.getExecutor();
1    this.scheduler = procedureManager.getScheduler();
1  }
1
1  @Override
1  public void bindTo(AbstractMetricService metricService) {
1    this.metricService = metricService;
1    bindThreadMetrics(metricService);
1  }
1
1  @Override
1  public void unbindFrom(AbstractMetricService metricService) {
1    unbindThreadMetrics(metricService);
1
1    for (ProcedureMetricItems items : metricItemsMap.values()) {
1      items.unbindMetricItems(metricService);
1    }
1    metricItemsMap.clear();
1  }
1
1  private void bindThreadMetrics(AbstractMetricService metricService) {
1    metricService.createAutoGauge(
1        Metric.PROCEDURE_WORKER_THREAD_COUNT.toString(),
1        MetricLevel.CORE,
1        executor,
1        ProcedureExecutor::getWorkerThreadCount);
1    metricService.createAutoGauge(
1        Metric.PROCEDURE_ACTIVE_WORKER_THREAD_COUNT.toString(),
1        MetricLevel.CORE,
1        executor,
1        ProcedureExecutor::getActiveWorkerThreadCount);
1    metricService.createAutoGauge(
1        Metric.PROCEDURE_QUEUE_LENGTH.toString(),
1        MetricLevel.CORE,
1        scheduler,
1        ProcedureScheduler::size);
1  }
1
1  private void unbindThreadMetrics(AbstractMetricService metricService) {
1    metricService.remove(MetricType.AUTO_GAUGE, Metric.PROCEDURE_WORKER_THREAD_COUNT.toString());
1    metricService.remove(
1        MetricType.AUTO_GAUGE, Metric.PROCEDURE_ACTIVE_WORKER_THREAD_COUNT.toString());
1    metricService.remove(MetricType.AUTO_GAUGE, Metric.PROCEDURE_QUEUE_LENGTH.toString());
1  }
1
1  public void updateMetricsOnSubmit(String procType) {
1    Optional.ofNullable(metricService)
1        .ifPresent(
1            service -> {
1              metricItemsMap
1                  .computeIfAbsent(procType, k -> new ProcedureMetricItems(k, service))
1                  .updateMetricsOnSubmit();
1            });
1  }
1
1  public void updateMetricsOnFinish(String procType, long runtime, boolean success) {
1    Optional.ofNullable(metricService)
1        .ifPresent(
1            service -> {
1              metricItemsMap
1                  .computeIfAbsent(procType, k -> new ProcedureMetricItems(k, service))
1                  .updateMetricsOnFinish(runtime, success);
1            });
1  }
1
1  class ProcedureMetricItems {
1    private final String procType;
1    private Counter submittedCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
1    private Counter failedCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
1    private Timer executionTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
1
1    public ProcedureMetricItems(String procType, AbstractMetricService metricService) {
1      this.procType = procType;
1      bindMetricItems(metricService);
1    }
1
1    public void bindMetricItems(AbstractMetricService metricService) {
1      this.submittedCounter =
1          metricService.getOrCreateCounter(
1              Metric.PROCEDURE_SUBMITTED_COUNT.toString(),
1              MetricLevel.CORE,
1              Tag.TYPE.toString(),
1              procType);
1      this.failedCounter =
1          metricService.getOrCreateCounter(
1              Metric.PROCEDURE_FAILED_COUNT.toString(),
1              MetricLevel.CORE,
1              Tag.TYPE.toString(),
1              procType);
1      this.executionTimer =
1          metricService.getOrCreateTimer(
1              Metric.PROCEDURE_EXECUTION_TIME.toString(),
1              MetricLevel.CORE,
1              Tag.TYPE.toString(),
1              procType);
1    }
1
1    public void unbindMetricItems(AbstractMetricService metricService) {
1      metricService.remove(
1          MetricType.COUNTER,
1          Metric.PROCEDURE_SUBMITTED_COUNT.toString(),
1          Tag.TYPE.toString(),
1          procType);
1      metricService.remove(
1          MetricType.COUNTER,
1          Metric.PROCEDURE_FAILED_COUNT.toString(),
1          Tag.TYPE.toString(),
1          procType);
1      metricService.remove(
1          MetricType.TIMER,
1          Metric.PROCEDURE_EXECUTION_TIME.toString(),
1          Tag.TYPE.toString(),
1          procType);
1    }
1
1    public void updateMetricsOnSubmit() {
1      submittedCounter.inc();
1    }
1
1    public void updateMetricsOnFinish(long runtime, boolean success) {
1      if (success) {
1        executionTimer.updateMillis(runtime);
1      } else {
1        failedCounter.inc();
1      }
1    }
1  }
1}
1