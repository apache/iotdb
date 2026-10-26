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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.ErrorHandlingCommonUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalReporter;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class IoTDBInternalLocalReporter extends IoTDBInternalReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBInternalLocalReporter.class);
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private final Coordinator coordinator;
  private final SessionInfo sessionInfo;
  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service;
  private volatile boolean stopped;

  public IoTDBInternalLocalReporter() {
    this(
        Executors.newSingleThreadScheduledExecutor(),
        Coordinator.getInstance(),
        new SessionInfo(
                0,
                new UserEntity(
                    AuthorityChecker.SUPER_USER_ID,
                    AuthorityChecker.SUPER_USER,
                    IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
                ZoneId.systemDefault())
            .setInternal(true),
        ClusterPartitionFetcher.getInstance(),
        ClusterSchemaFetcher.getInstance());

    IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
        ConfigNodeClientManager.getInstance();
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetDatabaseReq req =
          new TGetDatabaseReq(
              Arrays.asList(SchemaConstant.SYSTEM_DATABASE.split("\\.")),
              SchemaConstant.ALL_MATCH_SCOPE_BINARY);
      TShowDatabaseResp showDatabaseResp = client.showDatabase(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == showDatabaseResp.getStatus().getCode()
          && showDatabaseResp.getDatabaseInfoMapSize() == 0) {
        TDatabaseSchema databaseSchema = new TDatabaseSchema();
        databaseSchema.setName(SchemaConstant.SYSTEM_DATABASE);
        databaseSchema.setIsTableModel(false);
        TSStatus tsStatus = client.setDatabase(databaseSchema);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
          LOGGER.error(DataNodeMiscMessages.CHECK_OR_CREATE_DATABASE_FAILED);
        }
      }
    } catch (ClientManagerException | TException e) {
      // do nothing
      LOGGER.warn(DataNodeMiscMessages.CHECK_OR_CREATE_DATABASE_FAILED_BECAUSE, e);
    }
  }

  IoTDBInternalLocalReporter(
      ScheduledExecutorService service,
      Coordinator coordinator,
      SessionInfo sessionInfo,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    this.service = service;
    this.coordinator = coordinator;
    this.sessionInfo = sessionInfo;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
  }

  @Override
  public InternalReporterType getType() {
    return InternalReporterType.IOTDB;
  }

  @Override
  public synchronized boolean start() {
    if (isStopping()) {
      return false;
    }
    if (currentServiceFuture != null) {
      LOGGER.warn(DataNodeMiscMessages.INTERNAL_REPORTER_ALREADY_STARTED);
      return false;
    }
    currentServiceFuture =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            service,
            () -> writeMetricToIoTDB(autoGauges),
            1,
            MetricConfigDescriptor.getInstance().getMetricConfig().getAsyncCollectPeriodInSecond(),
            TimeUnit.SECONDS);
    LOGGER.info(DataNodeMiscMessages.INTERNAL_REPORTER_START);
    return true;
  }

  @Override
  public synchronized boolean stop() {
    // Publish this before interrupting an in-flight write, including during reporter reload.
    stopped = true;
    if (currentServiceFuture != null) {
      currentServiceFuture.cancel(true);
      currentServiceFuture = null;
    }
    service.shutdownNow();
    clear();
    LOGGER.info(DataNodeMiscMessages.INTERNAL_REPORTER_STOP);
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.IOTDB;
  }

  @Override
  protected void writeMetricToIoTDB(Map<String, Object> valueMap, String prefix, long time) {
    if (isStopping()) {
      return;
    }
    try {
      service.execute(() -> writeMetric(valueMap, prefix, time));
    } catch (RejectedExecutionException e) {
      // stop() can shut down the executor between the check above and submission.
      if (!stopped) {
        throw e;
      }
    }
  }

  private void writeMetric(Map<String, Object> valueMap, String prefix, long time) {
    if (isStopping()) {
      return;
    }
    try {
      ExecutionResult result = insertRecord(valueMap, prefix, time);
      if (result.status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode() && !isStopping()) {
        createTimeSeries(valueMap, prefix);
        if (isStopping()) {
          return;
        }
        result = insertRecord(valueMap, prefix, time);
      }
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (isExpectedShutdownFailure(result)) {
          LOGGER.debug(DataNodeMiscMessages.FAILED_UPDATE_METRIC_VALUE, result.status);
        } else {
          LOGGER.warn(DataNodeMiscMessages.FAILED_UPDATE_METRIC_VALUE, result.status);
        }
      }
    } catch (IoTDBConnectionException e1) {
      if (isInterruptedDuringShutdown(e1)) {
        LOGGER.debug(
            DataNodeMiscMessages
                .MISC_LOG_FAILED_TO_UPDATE_THE_VALUE_OF_METRIC_BECAUSE_OF_CONNECTION_B0FC4929,
            e1);
      } else {
        LOGGER.warn(
            DataNodeMiscMessages
                .MISC_LOG_FAILED_TO_UPDATE_THE_VALUE_OF_METRIC_BECAUSE_OF_CONNECTION_B0FC4929,
            e1);
      }
    } catch (IllegalPathException | QueryProcessException e2) {
      if (isInterruptedDuringShutdown(e2)) {
        LOGGER.debug(
            DataNodeMiscMessages
                .MISC_LOG_FAILED_TO_UPDATE_THE_VALUE_OF_METRIC_BECAUSE_OF_INTERNAL_E5C64806,
            e2);
      } else {
        LOGGER.warn(
            DataNodeMiscMessages
                .MISC_LOG_FAILED_TO_UPDATE_THE_VALUE_OF_METRIC_BECAUSE_OF_INTERNAL_E5C64806,
            e2);
      }
    }
  }

  private boolean isStopping() {
    return stopped || CommonDescriptor.getInstance().getConfig().isStopping();
  }

  private boolean isExpectedShutdownFailure(ExecutionResult result) {
    return isStopping()
        && (result.failureCausedByInterruption
            || (CommonDescriptor.getInstance().getConfig().isStopping()
                && result.status.getCode() == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()));
  }

  private boolean isInterruptedDuringShutdown(Exception e) {
    if (isStopping() && ErrorHandlingCommonUtils.getRootCause(e) instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      return true;
    }
    return false;
  }

  private ExecutionResult insertRecord(Map<String, Object> valueMap, String prefix, long time)
      throws IoTDBConnectionException, QueryProcessException, IllegalPathException {
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
    ByteBuffer buffer = SessionUtils.getValueBuffer(types, values, measurements);

    request.setPrefixPath(prefix);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    request.setValues(buffer);
    request.setIsAligned(false);

    InsertRowStatement s = StatementGenerator.createStatement(request);
    final long queryId = SESSION_MANAGER.requestQueryId();
    return coordinator.executeForTreeModel(
        s, queryId, sessionInfo, "", partitionFetcher, schemaFetcher);
  }

  private void createTimeSeries(Map<String, Object> valueMap, String prefix)
      throws IllegalPathException {
    TSCreateMultiTimeseriesReq request = new TSCreateMultiTimeseriesReq();
    List<String> paths = new ArrayList<>();
    List<Integer> types = new ArrayList<>();
    List<Integer> encodings = new ArrayList<>();
    List<Integer> compressors = new ArrayList<>();
    for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
      String measurement = entry.getKey();
      paths.add(prefix + "." + measurement);
      TSDataType type = inferType(entry.getValue());
      types.add(type.ordinal());
      encodings.add((int) getDefaultEncoding(type).serialize());
      compressors.add(
          (int) TSFileDescriptor.getInstance().getConfig().getCompressor(type).serialize());
    }
    request.setPaths(paths);
    request.setDataTypes(types);
    request.setEncodings(encodings);
    request.setCompressors(compressors);
    CreateMultiTimeSeriesStatement s = StatementGenerator.createStatement(request);
    final long queryId = SESSION_MANAGER.requestQueryId();

    ExecutionResult result =
        coordinator.executeForTreeModel(
            s, queryId, sessionInfo, "", partitionFetcher, schemaFetcher);
    if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (isExpectedShutdownFailure(result)) {
        LOGGER.debug(DataNodeMiscMessages.FAILED_AUTO_CREATE_TIMESERIES, paths, result.status);
      } else {
        LOGGER.warn(DataNodeMiscMessages.FAILED_AUTO_CREATE_TIMESERIES, paths, result.status);
      }
    }
  }

  @Override
  protected void writeMetricsToIoTDB(Map<String, Map<String, Object>> valueMap, long time) {
    for (Map.Entry<String, Map<String, Object>> value : valueMap.entrySet()) {
      writeMetricToIoTDB(value.getValue(), value.getKey(), time);
    }
  }
}
