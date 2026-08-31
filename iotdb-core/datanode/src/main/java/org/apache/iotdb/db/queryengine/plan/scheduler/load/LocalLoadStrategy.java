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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.PlainDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LOAD local-load strategy: loads a TsFile without decoding it, used when the file's device/time
 * ranges map to a single local region. The whole file is wrapped into one {@code FragmentInstance}
 * (reusing the scheduler's fragment id) and dispatched to the local data region through {@link
 * LoadTsFileDispatcherImpl#dispatchLocally}; nothing crosses the network.
 *
 * <p>The strategy also:
 *
 * <ul>
 *   <li>rejects loads while the node is read-only ({@link LoadReadOnlyException});
 *   <li>converts a {@code PlainDeviceTimeIndex} into an {@code ArrayDeviceTimeIndex} so the local
 *       writer can use the time index directly;
 *   <li>records flush/points metrics on the target data region;
 *   <li>measures the whole execution as the {@code LOAD_LOCALLY} phase metric.
 * </ul>
 */
public class LocalLoadStrategy implements TsFileLoadStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalLoadStrategy.class);

  private static final LoadTsFileCostMetricsSet LOAD_TSFILE_COST_METRICS_SET =
      LoadTsFileCostMetricsSet.getInstance();

  private final MPPQueryContext queryContext;
  private final PlanFragmentId fragmentId;
  private final LoadTsFileDispatcherImpl dispatcher;

  public LocalLoadStrategy(
      MPPQueryContext queryContext,
      PlanFragmentId fragmentId,
      LoadTsFileDispatcherImpl dispatcher) {
    this.queryContext = queryContext;
    this.fragmentId = fragmentId;
    this.dispatcher = dispatcher;
  }

  @Override
  public boolean execute(LoadSingleTsFileNode node) throws IoTDBException {
    final long startTime = System.nanoTime();
    try {
      return loadLocally(node);
    } finally {
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.LOAD_LOCALLY, System.nanoTime() - startTime);
    }
  }

  private boolean loadLocally(LoadSingleTsFileNode node) throws IoTDBException {
    LOGGER.info(
        DataNodeQueryMessages.START_LOAD_TSFILE_LOCALLY,
        node.getTsFileResource().getTsFile().getPath());

    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new LoadReadOnlyException();
    }

    // if the time index is PlainDeviceTimeIndex, convert it to ArrayDeviceTimeIndex
    if (node.getTsFileResource().getTimeIndex() instanceof PlainDeviceTimeIndex) {
      final PlainDeviceTimeIndex timeIndex =
          (PlainDeviceTimeIndex) node.getTsFileResource().getTimeIndex();
      final Map<IDeviceID, Integer> convertedDeviceToIndex = new ConcurrentHashMap<>();
      for (final Map.Entry<IDeviceID, Integer> entry : timeIndex.getDeviceToIndex().entrySet()) {
        convertedDeviceToIndex.put(
            entry.getKey() instanceof StringArrayDeviceID
                ? entry.getKey()
                : new StringArrayDeviceID(entry.getKey().toString()),
            entry.getValue());
      }
      node.getTsFileResource()
          .setTimeIndex(
              new ArrayDeviceTimeIndex(
                  convertedDeviceToIndex, timeIndex.getStartTimes(), timeIndex.getEndTimes()));
    }

    try {
      FragmentInstance instance =
          new FragmentInstance(
              new PlanFragment(fragmentId, node),
              fragmentId.genFragmentInstanceId(),
              null,
              queryContext.getQueryType(),
              queryContext.getTimeOut()
                  - (System.currentTimeMillis() - queryContext.getStartTime()),
              queryContext.getSession(),
              queryContext.isDebug(),
              queryContext.isVerbose());
      instance.setExecutorAndHost(new StorageExecutor(node.getLocalRegionReplicaSet()));
      dispatcher.dispatchLocally(instance);
    } catch (FragmentInstanceDispatchException e) {
      LOGGER.warn(
          String.format(
              DataNodeQueryMessages.DISPATCH_TSFILE_S_ERROR_TO_LOCAL_ERROR_RESULT_STATUS_CODE_S
                  + DataNodeQueryMessages.RESULT_STATUS_MESSAGE_S,
              node.getTsFileResource().getTsFile(),
              TSStatusCode.representOf(e.getFailureStatus().getCode()).name(),
              e.getFailureStatus().getMessage()));
      return false;
    }

    // add metrics
    Optional.ofNullable(
            StorageEngine.getInstance()
                .getDataRegion(
                    (DataRegionId)
                        ConsensusGroupId.Factory.createFromTConsensusGroupId(
                            node.getLocalRegionReplicaSet().getRegionId())))
        .ifPresent(
            dataRegion ->
                dataRegion
                    .getNonSystemDatabaseName()
                    .ifPresent(
                        databaseName -> {
                          // Report load tsFile points to IoTDB flush metrics
                          MemTableFlushTask.recordFlushPointsMetricInternal(
                              node.getWritePointCount(),
                              databaseName,
                              dataRegion.getDataRegionIdString());

                          MetricService.getInstance()
                              .count(
                                  node.getWritePointCount(),
                                  Metric.QUANTITY.toString(),
                                  MetricLevel.CORE,
                                  Tag.NAME.toString(),
                                  Metric.POINTS_IN.toString(),
                                  Tag.DATABASE.toString(),
                                  databaseName,
                                  Tag.REGION.toString(),
                                  dataRegion.getDataRegionIdString(),
                                  Tag.TYPE.toString(),
                                  Metric.LOAD_POINT_COUNT.toString());
                          MetricService.getInstance()
                              .count(
                                  node.getWritePointCount(),
                                  Metric.LEADER_QUANTITY.toString(),
                                  MetricLevel.CORE,
                                  Tag.NAME.toString(),
                                  Metric.POINTS_IN.toString(),
                                  Tag.DATABASE.toString(),
                                  databaseName,
                                  Tag.REGION.toString(),
                                  dataRegion.getDataRegionIdString(),
                                  Tag.TYPE.toString(),
                                  Metric.LOAD_POINT_COUNT.toString());
                        }));

    return true;
  }
}
