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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.extractor.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.metric.PipeConnectorMetrics;
import org.apache.iotdb.db.pipe.metric.PipeExtractorMetrics;
import org.apache.iotdb.db.pipe.metric.PipeProcessorMetrics;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.task.builder.PipeDataNodeBuilder;
import org.apache.iotdb.db.pipe.task.builder.PipeDataNodeTaskDataRegionBuilder;
import org.apache.iotdb.db.pipe.task.builder.PipeDataNodeTaskSchemaRegionBuilder;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtaskManager;
import org.apache.iotdb.db.pipe.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.ConfigRegion;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_MODE_KEY;

public class PipeTaskDataNodeAgent extends PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskDataNodeAgent.class);

  protected static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  ////////////////////////// Pipe Task Management Entry //////////////////////////

  @Override
  protected boolean isShutdown() {
    return PipeAgent.runtime().isShutdown();
  }

  @Override
  protected Map<TConsensusGroupId, PipeTask> buildPipeTasks(PipeMeta pipeMetaFromConfigNode) {
    return new PipeDataNodeBuilder(pipeMetaFromConfigNode).build();
  }

  /**
   * Using try lock method to prevent deadlock when stopping all pipes with critical exceptions and
   * {@link PipeTaskDataNodeAgent#handlePipeMetaChanges(List)}} concurrently.
   */
  public void stopAllPipesWithCriticalException() {
    try {
      int retryCount = 0;
      while (true) {
        if (tryWriteLockWithTimeOut(5)) {
          try {
            stopAllPipesWithCriticalExceptionInternal();
            LOGGER.info("Stopped all pipes with critical exception.");
            return;
          } finally {
            releaseWriteLock();
          }
        } else {
          Thread.sleep(1000);
          LOGGER.warn(
              "Failed to stop all pipes with critical exception, retry count: {}.", ++retryCount);
        }
      }
    } catch (InterruptedException e) {
      LOGGER.error(
          "Interrupted when trying to stop all pipes with critical exception, exception message: {}",
          e.getMessage(),
          e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.error(
          "Failed to stop all pipes with critical exception, exception message: {}",
          e.getMessage(),
          e);
    }
  }

  private void stopAllPipesWithCriticalExceptionInternal() {
    // 1. track exception in all pipe tasks that share the same connector that have critical
    // exceptions.
    final int currentDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    final Map<PipeParameters, PipeRuntimeConnectorCriticalException>
        reusedConnectorParameters2ExceptionMap = new HashMap<>();

    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();
              final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();

              runtimeMeta
                  .getConsensusGroupId2TaskMetaMap()
                  .values()
                  .forEach(
                      pipeTaskMeta -> {
                        if (pipeTaskMeta.getLeaderDataNodeId() != currentDataNodeId) {
                          return;
                        }

                        for (final PipeRuntimeException e : pipeTaskMeta.getExceptionMessages()) {
                          if (e instanceof PipeRuntimeConnectorCriticalException) {
                            reusedConnectorParameters2ExceptionMap.putIfAbsent(
                                staticMeta.getConnectorParameters(),
                                (PipeRuntimeConnectorCriticalException) e);
                          }
                        }
                      });
            });
    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();
              final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();

              runtimeMeta
                  .getConsensusGroupId2TaskMetaMap()
                  .values()
                  .forEach(
                      pipeTaskMeta -> {
                        if (pipeTaskMeta.getLeaderDataNodeId() == currentDataNodeId
                            && reusedConnectorParameters2ExceptionMap.containsKey(
                                staticMeta.getConnectorParameters())
                            && !pipeTaskMeta.containsExceptionMessage(
                                reusedConnectorParameters2ExceptionMap.get(
                                    staticMeta.getConnectorParameters()))) {
                          final PipeRuntimeConnectorCriticalException exception =
                              reusedConnectorParameters2ExceptionMap.get(
                                  staticMeta.getConnectorParameters());
                          pipeTaskMeta.trackExceptionMessage(exception);
                          LOGGER.warn(
                              "Pipe {} (creation time = {}) will be stopped because of critical exception "
                                  + "(occurred time {}) in connector {}.",
                              staticMeta.getPipeName(),
                              DateTimeUtils.convertLongToDate(staticMeta.getCreationTime(), "ms"),
                              DateTimeUtils.convertLongToDate(exception.getTimeStamp(), "ms"),
                              staticMeta.getConnectorParameters());
                        }
                      });
            });

    // 2. stop all pipes that have critical exceptions.
    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();
              final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();

              if (runtimeMeta.getStatus().get() == PipeStatus.RUNNING) {
                runtimeMeta
                    .getConsensusGroupId2TaskMetaMap()
                    .values()
                    .forEach(
                        pipeTaskMeta -> {
                          for (final PipeRuntimeException e : pipeTaskMeta.getExceptionMessages()) {
                            if (e instanceof PipeRuntimeCriticalException) {
                              stopPipe(staticMeta.getPipeName(), staticMeta.getCreationTime());
                              LOGGER.warn(
                                  "Pipe {} (creation time = {}) was stopped because of critical exception "
                                      + "(occurred time {}).",
                                  staticMeta.getPipeName(),
                                  DateTimeUtils.convertLongToDate(
                                      staticMeta.getCreationTime(), "ms"),
                                  DateTimeUtils.convertLongToDate(e.getTimeStamp(), "ms"));
                              return;
                            }
                          }
                        });
              }
            });
  }

  ///////////////////////// Manage by dataRegionGroupId /////////////////////////

  @Override
  protected void createPipeTask(
      TConsensusGroupId consensusGroupId,
      PipeStaticMeta pipeStaticMeta,
      PipeTaskMeta pipeTaskMeta) {
    if (consensusGroupId.getType() != ConfigRegion
        && pipeTaskMeta.getLeaderDataNodeId() == CONFIG.getDataNodeId()) {
      final PipeDataNodeTask pipeTask;
      switch (consensusGroupId.getType()) {
        case DataRegion:
          pipeTask =
              new PipeDataNodeTaskDataRegionBuilder(pipeStaticMeta, consensusGroupId, pipeTaskMeta)
                  .build();
          break;
        case SchemaRegion:
          pipeTask =
              new PipeDataNodeTaskSchemaRegionBuilder(
                      pipeStaticMeta, consensusGroupId, pipeTaskMeta)
                  .build();
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported consensus group type: " + consensusGroupId.getType());
      }
      pipeTask.create();
      pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
    }

    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .put(consensusGroupId, pipeTaskMeta);
  }

  ///////////////////////// Heartbeat /////////////////////////

  public void collectPipeMetaList(TDataNodeHeartbeatResp resp) throws TException {
    // Try the lock instead of directly acquire it to prevent the block of the cluster heartbeat
    // 10s is the half of the HEARTBEAT_TIMEOUT_TIME defined in class BaseNodeCache in ConfigNode
    if (!tryReadLockWithTimeOut(10)) {
      return;
    }
    try {
      collectPipeMetaListInternal(resp);
    } finally {
      releaseReadLock();
    }
  }

  private void collectPipeMetaListInternal(TDataNodeHeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        LOGGER.info("Reporting pipe meta: {}", pipeMeta.coreReportMessage());
      }
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
  }

  public void collectPipeMetaList(TPipeHeartbeatReq req, TPipeHeartbeatResp resp)
      throws TException {
    acquireReadLock();
    try {
      collectPipeMetaListInternal(req, resp);
    } finally {
      releaseReadLock();
    }
  }

  private void collectPipeMetaListInternal(TPipeHeartbeatReq req, TPipeHeartbeatResp resp)
      throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }
    LOGGER.info("Received pipe heartbeat request {} from config node.", req.heartbeatId);

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        LOGGER.info("Reporting pipe meta: {}", pipeMeta.coreReportMessage());
      }
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);

    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(true);
  }

  ///////////////////////// Restart Logic /////////////////////////

  public void restartAllStuckPipes() {
    if (!tryWriteLockWithTimeOut(0)) {
      return;
    }
    try {
      Map<String, IoTDBDataRegionExtractor> pipeName2ExtractorMap =
          PipeExtractorMetrics.getInstance().getPipeName2ExtractorMap();
      Map<String, PipeProcessorSubtask> pipeName2ProcessorSubtaskMap =
          PipeProcessorMetrics.getInstance().getPipeName2ProcessorSubtaskMap();
      Map<String, PipeConnectorSubtask> attributeSortedStr2PipeConnectorSubtaskMap =
          PipeConnectorMetrics.getInstance().getAttributeSortedStr2PipeConnectorSubtaskMap();

      Set<PipeMeta> restartPipes = new HashSet<>();

      for (PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        // Only restart running pipe
        if (!pipeMeta.getRuntimeMeta().getStatus().get().equals(PipeStatus.RUNNING)) {
          continue;
        }
        // Only restart for hybrid mode
        switch (pipeMeta
            .getStaticMeta()
            .getExtractorParameters()
            .getStringByKeys(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
          case EXTRACTOR_REALTIME_MODE_FILE_VALUE:
          case EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE:
          case EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE:
            continue;
          default:
            break;
        }
        // Only restart for specific connector types
        String pluginName =
            pipeMeta
                .getStaticMeta()
                .getConnectorParameters()
                .getStringOrDefault(
                    Arrays.asList(
                        PipeConnectorConstant.CONNECTOR_KEY, PipeConnectorConstant.SINK_KEY),
                    BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
                .toLowerCase();
        if (!Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName())
            && !Objects.equals(pluginName, BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_THRIFT_SYNC_SINK.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK.getPipePluginName())
            && !Objects.equals(
                pluginName, BuiltinPipePlugin.IOTDB_AIR_GAP_SINK.getPipePluginName())) {
          continue;
        }
        String pipeName = pipeMeta.getStaticMeta().getPipeName();
        IoTDBDataRegionExtractor extractor = pipeName2ExtractorMap.get(pipeName);
        PipeProcessorSubtask processorSubtask = pipeName2ProcessorSubtaskMap.get(pipeName);
        PipeConnectorSubtask connectorSubtask =
            attributeSortedStr2PipeConnectorSubtaskMap.get(
                PipeConnectorSubtaskManager.getAttributeSortedString(
                    pipeMeta.getStaticMeta().getConnectorParameters()));

        if (Objects.isNull(extractor)
            || Objects.isNull(processorSubtask)
            || Objects.isNull(connectorSubtask)) {
          continue;
        }

        boolean tsFileCountExceeded =
            extractor.getRealtimeTsFileInsertionEventCount()
                    + processorSubtask.getTsFileInsertionEventCount()
                    + connectorSubtask.getTsFileInsertionEventCount()
                > PipeConfig.getInstance().getPipeMaxAllowedTsFileCount();
        boolean connectorStuck =
            System.currentTimeMillis() - connectorSubtask.getLastSendExecutionTime()
                > PipeConfig.getInstance().getPipeMaxAllowedConnectorStuckTime();
        boolean walFull =
            WALManager.getInstance().getTotalDiskUsage()
                > IoTDBDescriptor.getInstance().getConfig().getThrottleThreshold();
        boolean pinExceeded =
            PipeResourceManager.wal().getPinnedWalCount()
                >= PipeConfig.getInstance().getPipeMaxAllowedPinnedMemTableCount() * 1.5;
        if (tsFileCountExceeded || connectorStuck || walFull || pinExceeded) {
          restartPipes.add(pipeMeta);
        }
      }

      restartPipes.parallelStream().forEach(this::restartPipeInternal);

    } finally {
      releaseWriteLock();
    }
  }

  private void restartPipeInternal(PipeMeta pipeMeta) {
    handleDropPipeInternal(pipeMeta.getStaticMeta().getPipeName());
    handleSinglePipeMetaChangesInternal(pipeMeta);
  }
}
