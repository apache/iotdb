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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.exception.load.LoadReadOnlyException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.PlainDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Executes fast local loading of whole TsFiles without decoding chunks or network transport.
 * Bypasses network RPC when all data partitions route to the target DataRegion hosted on the local
 * DataNode.
 */
public class LocalLoadStrategy implements TsFileLoadStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalLoadStrategy.class);

  private static final LoadTsFileCostMetricsSet COST_METRICS =
      LoadTsFileCostMetricsSet.getInstance();

  private final MPPQueryContext queryContext;
  private final PlanFragmentId fragmentId;
  private final LoadTsFileDispatcherImpl dispatcher;

  public LocalLoadStrategy(
      final MPPQueryContext queryContext,
      final PlanFragmentId fragmentId,
      final LoadTsFileDispatcherImpl dispatcher) {
    this.queryContext =
        Objects.requireNonNull(
            queryContext, DataNodeQueryMessages.EXCEPTION_QUERYCONTEXT_CANNOT_BE_NULL_C2B25B22);
    this.fragmentId =
        Objects.requireNonNull(
            fragmentId, DataNodeQueryMessages.EXCEPTION_FRAGMENTID_CANNOT_BE_NULL_7726B33B);
    this.dispatcher =
        Objects.requireNonNull(
            dispatcher, DataNodeQueryMessages.EXCEPTION_DISPATCHER_CANNOT_BE_NULL_6118319E);
  }

  // -------------------------------------------------------------------------
  // Execution Entrypoint
  // -------------------------------------------------------------------------

  @Override
  public boolean execute(final LoadSingleTsFileNode node) throws IoTDBException {
    Objects.requireNonNull(node, StorageEngineMessages.EXCEPTION_NODE_CANNOT_BE_NULL_BC7D5BB9);
    final long startTime = System.nanoTime();
    try {
      return loadLocally(node);
    } finally {
      COST_METRICS.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.LOAD_LOCALLY, System.nanoTime() - startTime);
    }
  }

  private boolean loadLocally(final LoadSingleTsFileNode node) throws IoTDBException {
    final TsFileResource resource =
        Objects.requireNonNull(
            node.getTsFileResource(),
            DataNodeQueryMessages.EXCEPTION_TSFILERESOURCE_CANNOT_BE_NULL_C63F7B08);

    LOGGER.info(DataNodeQueryMessages.START_LOAD_TSFILE_LOCALLY, resource.getTsFile().getPath());

    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new LoadReadOnlyException();
    }

    normalizeTimeIndex(resource);

    final long remainingTimeOutMs =
        Math.max(
            1L,
            queryContext.getTimeOut() - (System.currentTimeMillis() - queryContext.getStartTime()));

    try {
      final FragmentInstance instance =
          new FragmentInstance(
              new PlanFragment(fragmentId, node),
              fragmentId.genFragmentInstanceId(),
              null,
              queryContext.getQueryType(),
              remainingTimeOutMs,
              queryContext.getSession(),
              queryContext.isDebug(),
              queryContext.isVerbose());

      instance.setExecutorAndHost(new StorageExecutor(node.getLocalRegionReplicaSet()));
      dispatcher.dispatchLocally(instance);
    } catch (final FragmentInstanceDispatchException e) {
      final TSStatus failureStatus = e.getFailureStatus();
      final TSStatusCode statusCode =
          failureStatus != null ? TSStatusCode.representOf(failureStatus.getCode()) : null;
      final String codeName = statusCode != null ? statusCode.name() : "UNKNOWN_STATUS";
      final String message = failureStatus != null ? failureStatus.getMessage() : "null";

      LOGGER.warn(
          String.format(
              DataNodeQueryMessages.DISPATCH_TSFILE_S_ERROR_TO_LOCAL_ERROR_RESULT_STATUS_CODE_S
                  + DataNodeQueryMessages.RESULT_STATUS_MESSAGE_S,
              resource.getTsFile(),
              codeName,
              message));
      return false;
    }

    recordMetrics(node);
    return true;
  }

  // -------------------------------------------------------------------------
  // TimeIndex Normalization & Metrics
  // -------------------------------------------------------------------------

  /**
   * Converts a PlainDeviceTimeIndex to ArrayDeviceTimeIndex for high-efficiency local writer
   * access.
   */
  private void normalizeTimeIndex(final TsFileResource resource) {
    final ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof PlainDeviceTimeIndex plainTimeIndex) {
      final Map<IDeviceID, Integer> sourceMap = plainTimeIndex.getDeviceToIndex();
      final Map<IDeviceID, Integer> convertedDeviceToIndex = new HashMap<>(sourceMap.size());

      for (final Map.Entry<IDeviceID, Integer> entry : sourceMap.entrySet()) {
        final IDeviceID originalId = entry.getKey();
        final IDeviceID normalizedId =
            originalId instanceof StringArrayDeviceID
                ? originalId
                : new StringArrayDeviceID(originalId.toString());
        convertedDeviceToIndex.put(normalizedId, entry.getValue());
      }

      resource.setTimeIndex(
          new ArrayDeviceTimeIndex(
              convertedDeviceToIndex,
              plainTimeIndex.getStartTimes(),
              plainTimeIndex.getEndTimes()));
    }
  }

  private void recordMetrics(final LoadSingleTsFileNode node) {
    final ConsensusGroupId consensusGroupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(
            node.getLocalRegionReplicaSet().getRegionId());

    Optional.ofNullable(StorageEngine.getInstance().getDataRegion((DataRegionId) consensusGroupId))
        .ifPresent(
            dataRegion ->
                dataRegion
                    .getNonSystemDatabaseName()
                    .ifPresent(
                        databaseName -> reportPointCountMetrics(node, dataRegion, databaseName)));
  }

  private void reportPointCountMetrics(
      final LoadSingleTsFileNode node, final DataRegion dataRegion, final String databaseName) {
    final long pointCount = node.getWritePointCount();
    final String regionIdStr = dataRegion.getDataRegionIdString();

    MemTableFlushTask.recordFlushPointsMetricInternal(pointCount, databaseName, regionIdStr);

    final MetricService metricService = MetricService.getInstance();
    metricService.count(
        pointCount,
        Metric.QUANTITY.toString(),
        MetricLevel.CORE,
        Tag.NAME.toString(),
        Metric.POINTS_IN.toString(),
        Tag.DATABASE.toString(),
        databaseName,
        Tag.REGION.toString(),
        regionIdStr,
        Tag.TYPE.toString(),
        Metric.LOAD_POINT_COUNT.toString());

    metricService.count(
        pointCount,
        Metric.LEADER_QUANTITY.toString(),
        MetricLevel.CORE,
        Tag.NAME.toString(),
        Metric.POINTS_IN.toString(),
        Tag.DATABASE.toString(),
        databaseName,
        Tag.REGION.toString(),
        regionIdStr,
        Tag.TYPE.toString(),
        Metric.LOAD_POINT_COUNT.toString());
  }
}
