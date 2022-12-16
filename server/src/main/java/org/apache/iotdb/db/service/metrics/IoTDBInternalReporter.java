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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.reporter.InternalIoTDBReporter;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class IoTDBInternalReporter extends InternalIoTDBReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBInternalReporter.class);
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

  public IoTDBInternalReporter() {
    // Empty constructor
  }

  @Override
  protected void writeMetricToIoTDB(Map<String, Object> valueMap, String prefix, long time) {
    try {
      TSInsertRecordReq request = new TSInsertRecordReq();
      List<String> measurements = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      List<Object> values = new ArrayList<>();
      for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
        String measurement = entry.getKey();
        Object value = entry.getValue();
        measurements.add(measurement);
        types.add(inferType(value));
        values.add(value);
      }
      ByteBuffer buffer = SessionUtils.getValueBuffer(types, values);

      request.setPrefixPath(prefix);
      request.setTimestamp(time);
      request.setMeasurements(measurements);
      request.setValues(buffer);
      request.setIsAligned(false);

      InsertRowPlan plan =
          new InsertRowPlan(
              new PartialPath(request.getPrefixPath()),
              request.getTimestamp(),
              request.getMeasurements().toArray(new String[0]),
              request.values,
              request.isAligned);

      if (!IoTDB.serviceProvider.executeNonQuery(plan)) {
        LOGGER.error("Failed to update the value of metric with status.");
      }
    } catch (IoTDBConnectionException e1) {
      LOGGER.error("Failed to update the value of metric because of unknown type", e1);
    } catch (IllegalPathException
        | QueryProcessException
        | StorageGroupNotSetException
        | StorageEngineException e2) {
      LOGGER.error("Failed to update the value of metric because of internal error", e2);
    }
  }

  @Override
  protected void writeMetricsToIoTDB(Map<String, Map<String, Object>> valueMap, long time) {
    for (Map.Entry<String, Map<String, Object>> value : valueMap.entrySet()) {
      writeMetricToIoTDB(value.getValue(), value.getKey(), time);
    }
  }

  @Override
  public boolean start() {
    if (currentServiceFuture == null) {
      currentServiceFuture =
          service.scheduleAtFixedRate(
              () -> {
                writeMetricToIoTDB(autoGauges);
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
    clear();
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.IOTDB;
  }

  @Override
  public void setMetricManager(AbstractMetricManager metricManager) {}
}
