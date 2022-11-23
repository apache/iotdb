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

package org.apache.iotdb.metrics.reporter;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.IoTDBMetricsUtils;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SessionIoTDBReporter extends IoTDBReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionIoTDBReporter.class);
  private static final MetricConfig.IoTDBReporterConfig ioTDBReporterConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig().getIoTDBReporterConfig();
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

  /** The manager of metrics */
  protected AbstractMetricManager metricManager;
  /** The session pool to write metrics */
  protected SessionPool sessionPool;

  public SessionIoTDBReporter(AbstractMetricManager metricManager) {
    this.metricManager = metricManager;
    this.sessionPool =
        new SessionPool(
            ioTDBReporterConfig.getHost(),
            ioTDBReporterConfig.getPort(),
            ioTDBReporterConfig.getUsername(),
            ioTDBReporterConfig.getPassword(),
            ioTDBReporterConfig.getMaxConnectionNumber());
    IoTDBMetricsUtils.checkOrCreateDatabase(sessionPool);
  }

  @Override
  public void writeToIoTDB(String devicePath, String sensor, Object value, long time) {
    List<String> sensors = Collections.singletonList(sensor);
    List<TSDataType> dataTypes = Collections.singletonList(inferType(value));
    List<Object> values = Collections.singletonList(value);

    try {
      sessionPool.insertRecord(devicePath, time, sensors, dataTypes, values);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.warn("Failed to insert record");
    }
  }

  @Override
  public void writeToIoTDB(Map<Pair<String, String>, Object> metrics, long time) {
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> sensors = new ArrayList<>();
    List<List<TSDataType>> dataTypes = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();

    for (Map.Entry<Pair<String, String>, Object> metric : metrics.entrySet()) {
      deviceIds.add(metric.getKey().getLeft());
      times.add(time);
      sensors.add(Collections.singletonList(metric.getKey().getRight()));
      dataTypes.add(Collections.singletonList(inferType(metric.getValue())));
      values.add(Collections.singletonList(metric.getValue()));
    }

    try {
      sessionPool.insertRecords(deviceIds, times, sensors, dataTypes, values);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.warn("Failed to insert record");
    }
  }

  @Override
  public boolean start() {
    if (currentServiceFuture == null) {
      currentServiceFuture =
          service.scheduleAtFixedRate(
              () -> {
                try {
                  Map<Pair<String, String>, Object> metrics =
                      IoTDBMetricsUtils.exportMetricToIoTDBFormat(metricManager.getAllMetrics());
                  writeToIoTDB(metrics, System.currentTimeMillis());
                } catch (Throwable t) {
                  LOGGER.error("Schedule task failed", t);
                }
              },
              1,
              MetricConfigDescriptor.getInstance()
                  .getMetricConfig()
                  .getAsyncCollectPeriodInSecond(),
              TimeUnit.SECONDS);
    }
    return true;
  }

  @Override
  public boolean stop() {
    if (currentServiceFuture != null) {
      currentServiceFuture.cancel(true);
      currentServiceFuture = null;
    }
    if (sessionPool != null) {
      sessionPool.close();
    }
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.IOTDB;
  }

  @Override
  public void setMetricManager(AbstractMetricManager metricManager) {
    this.metricManager = metricManager;
  }
}
