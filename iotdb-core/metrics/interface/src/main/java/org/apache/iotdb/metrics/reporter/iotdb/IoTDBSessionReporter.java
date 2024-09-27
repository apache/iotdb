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

package org.apache.iotdb.metrics.reporter.iotdb;

import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfig.IoTDBReporterConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.IoTDBMetricsUtils;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class IoTDBSessionReporter extends IoTDBReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSessionReporter.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static final IoTDBReporterConfig ioTDBReporterConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig().getIoTDBReporterConfig();
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

  /** The manager of metrics. */
  protected AbstractMetricManager metricManager;

  /** The session pool to write metrics. */
  protected SessionPool sessionPool;

  public IoTDBSessionReporter(AbstractMetricManager metricManager) {
    this.metricManager = metricManager;
    this.sessionPool =
        new SessionPool(
            ioTDBReporterConfig.getHost(),
            ioTDBReporterConfig.getPort(),
            ioTDBReporterConfig.getUsername(),
            ioTDBReporterConfig.getPassword(),
            ioTDBReporterConfig.getMaxConnectionNumber());
    try (SessionDataSetWrapper result =
        this.sessionPool.executeQueryStatement(
            "SHOW DATABASES " + metricConfig.getInternalDatabase())) {
      if (!result.hasNext()) {
        try (SessionDataSetWrapper result2 =
            this.sessionPool.executeQueryStatement(
                "CREATE DATABASE "
                    + metricConfig.getInternalDatabase()
                    + " WITH SCHEMA_REPLICATION_FACTOR=1, DATA_REPLICATION_FACTOR=1, SCHEMA_REGION_GROUP_NUM=1, DATA_REGION_GROUP_NUM=1")) {
          if (!result2.hasNext()) {
            LOGGER.error("IoTDBSessionReporter checkOrCreateDatabase failed.");
          }
        }
      }
    } catch (IoTDBConnectionException e) {
      LOGGER.warn("IoTDBSessionReporter checkOrCreateDatabase failed because ", e);
    } catch (StatementExecutionException e) {
      // do nothing
    }
  }

  @Override
  @SuppressWarnings("unsafeThreadSchedule")
  public boolean start() {
    if (currentServiceFuture != null) {
      LOGGER.warn("IoTDBSessionReporter already start!");
      return false;
    }
    currentServiceFuture =
        service.scheduleAtFixedRate(
            () -> {
              try {
                Map<String, Map<String, Object>> values = new HashMap<>();
                for (Map.Entry<MetricInfo, IMetric> metricEntry :
                    metricManager.getAllMetrics().entrySet()) {
                  String prefix = IoTDBMetricsUtils.generatePath(metricEntry.getKey());
                  Map<String, Object> value = new HashMap<>();
                  metricEntry.getValue().constructValueMap(value);
                  values.put(prefix, value);
                }
                writeMetricsToIoTDB(values, System.currentTimeMillis());
              } catch (Throwable t) {
                LOGGER.error("IoTDBSessionReporter failed to start, because", t);
              }
            },
            1,
            MetricConfigDescriptor.getInstance().getMetricConfig().getAsyncCollectPeriodInSecond(),
            TimeUnit.SECONDS);
    LOGGER.info(
        "IoTDBSessionReporter start, write to {}:{}",
        ioTDBReporterConfig.getHost(),
        ioTDBReporterConfig.getPort());
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
    LOGGER.info("IoTDBSessionReporter stop!");
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.IOTDB;
  }

  @Override
  protected void writeMetricToIoTDB(Map<String, Object> valueMap, String prefix, long time) {
    List<String> sensors = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    for (Map.Entry<String, Object> sensor : valueMap.entrySet()) {
      sensors.add(sensor.getKey());
      dataTypes.add(inferType(sensor.getValue()));
      values.add(sensor.getValue());
    }

    try {
      sessionPool.insertRecord(prefix, time, sensors, dataTypes, values);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.warn("IoTDBSessionReporter failed to insert record, because ", e);
    }
  }

  @Override
  protected void writeMetricsToIoTDB(Map<String, Map<String, Object>> valueMap, long time) {
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> sensors = new ArrayList<>();
    List<List<TSDataType>> dataTypes = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();

    for (Map.Entry<String, Map<String, Object>> metric : valueMap.entrySet()) {
      deviceIds.add(metric.getKey());
      times.add(time);
      List<String> metricSensors = new ArrayList<>();
      List<TSDataType> metricDataTypes = new ArrayList<>();
      List<Object> metricValues = new ArrayList<>();
      for (Map.Entry<String, Object> sensor : metric.getValue().entrySet()) {
        metricSensors.add(sensor.getKey());
        metricDataTypes.add(inferType(sensor.getValue()));
        metricValues.add(sensor.getValue());
      }
      sensors.add(metricSensors);
      dataTypes.add(metricDataTypes);
      values.add(metricValues);
    }

    try {
      sessionPool.insertRecords(deviceIds, times, sensors, dataTypes, values);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.warn("IoTDBSessionReporter failed to insert record, because ", e);
    }
  }
}
