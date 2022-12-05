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

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandalonePartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandaloneSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.reporter.iotdb.InternalIoTDBReporter;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class IoTDBInternalReporter extends InternalIoTDBReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBInternalReporter.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private final SessionInfo SESSION_INFO;
  private final IPartitionFetcher PARTITION_FETCHER;
  private final ISchemaFetcher SCHEMA_FETCHER;
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

  public IoTDBInternalReporter() {
    if (config.isClusterMode()) {
      PARTITION_FETCHER = ClusterPartitionFetcher.getInstance();
      SCHEMA_FETCHER = ClusterSchemaFetcher.getInstance();
    } else {
      PARTITION_FETCHER = StandalonePartitionFetcher.getInstance();
      SCHEMA_FETCHER = StandaloneSchemaFetcher.getInstance();
    }
    SESSION_INFO = new SessionInfo(0, "root", ZoneId.systemDefault().getId());
  }

  @Override
  public InternalReporterType getType() {
    return InternalReporterType.IOTDB;
  }

  @Override
  public boolean start() {
    if (currentServiceFuture == null) {
      currentServiceFuture =
          ScheduledExecutorUtil.safelyScheduleAtFixedRate(
              service,
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

      Statement s = StatementGenerator.createStatement(request);
      final long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(s, queryId, SESSION_INFO, "", PARTITION_FETCHER, SCHEMA_FETCHER);
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to update the value of metric with status {}", result.status);
      }
    } catch (IoTDBConnectionException e1) {
      LOGGER.error("Failed to update the value of metric because of unknown type");
    } catch (IllegalPathException | QueryProcessException e2) {
      LOGGER.error("Failed to update the value of metric because of internal error");
    }
  }

  @Override
  protected void writeMetricsToIoTDB(Map<String, Map<String, Object>> valueMap, long time) {
    for (Map.Entry<String, Map<String, Object>> value : valueMap.entrySet()) {
      writeMetricToIoTDB(value.getValue(), value.getKey(), time);
    }
  }
}
