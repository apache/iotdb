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

package org.apache.iotdb.db.pipe.agent.task.subtask.sink;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class PipeSinkSubtaskManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSinkSubtaskManager.class);

  private static final String FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE =
      "Failed to deregister PipeConnectorSubtask. No such subtask: ";

  private final Map<PipeSinkSubtaskKey, List<PipeSinkSubtaskLifeCycle>>
      pipeSinkSubtaskKey2SubtaskLifeCycleMap = new HashMap<>();

  public synchronized String register(
      final Supplier<? extends PipeSinkSubtaskExecutor> executorSupplier,
      final PipeParameters pipeSinkParameters,
      final PipeTaskSinkRuntimeEnvironment environment) {
    final String connectorKey = getConnectorKey(pipeSinkParameters);
    PipeEventCommitManager.getInstance()
        .register(
            environment.getPipeName(),
            environment.getCreationTime(),
            environment.getRegionId(),
            connectorKey);

    final boolean isDataRegionSink = isDataRegionSink(environment.getRegionId());
    final int sinkNum = calculateSinkSubtaskNum(pipeSinkParameters, isDataRegionSink, connectorKey);
    boolean realTimeFirst = false;
    final String attributeSortedString =
        generateAttributeSortedString(pipeSinkParameters, environment.getRegionId());
    if (isDataRegionSink) {
      realTimeFirst =
          pipeSinkParameters.getBooleanOrDefault(
              Arrays.asList(
                  PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY,
                  PipeSinkConstant.SINK_REALTIME_FIRST_KEY),
              PipeSinkConstant.CONNECTOR_REALTIME_FIRST_DEFAULT_VALUE);
    }
    environment.setAttributeSortedString(attributeSortedString);
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        new PipeSinkSubtaskKey(
            environment.getPipeName(), environment.getCreationTime(), attributeSortedString);

    if (!pipeSinkSubtaskKey2SubtaskLifeCycleMap.containsKey(pipeSinkSubtaskKey)) {
      final PipeSinkSubtaskExecutor executor = executorSupplier.get();

      final List<PipeSinkSubtaskLifeCycle> pipeSinkSubtaskLifeCycleList = new ArrayList<>(sinkNum);

      AtomicInteger counter = new AtomicInteger(0);
      // Shared pending queue for all subtasks
      final UnboundedBlockingPendingQueue<Event> pendingQueue =
          realTimeFirst
              ? new PipeRealtimePriorityBlockingQueue()
              : new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

      if (realTimeFirst) {
        ((PipeRealtimePriorityBlockingQueue) pendingQueue).setOfferTsFileCounter(counter);
      }

      for (int connectorIndex = 0; connectorIndex < sinkNum; connectorIndex++) {
        final String taskID =
            String.format(
                "%s_%s_%s_%s",
                environment.getPipeName(),
                attributeSortedString,
                environment.getCreationTime(),
                connectorIndex);
        environment.setSinkTaskId(taskID);
        final PipeConnector pipeConnector =
            isDataRegionSink
                ? PipeDataNodeAgent.plugin().dataRegion().reflectSink(pipeSinkParameters)
                : PipeDataNodeAgent.plugin().schemaRegion().reflectSink(pipeSinkParameters);
        // 1. Construct, validate and customize PipeConnector, and then handshake (create
        // connection) with the target
        try {
          if (pipeConnector instanceof IoTDBDataRegionAsyncSink) {
            ((IoTDBDataRegionAsyncSink) pipeConnector).setTransferTsFileCounter(counter);
          }
          pipeConnector.validate(new PipeParameterValidator(pipeSinkParameters));
          pipeConnector.customize(
              pipeSinkParameters, new PipeTaskRuntimeConfiguration(environment));
          pipeConnector.handshake();
        } catch (final Exception e) {
          try {
            pipeConnector.close();
          } catch (final Exception closeException) {
            LOGGER.warn(
                "Failed to close connector after failed to initialize connector. "
                    + "Ignore this exception.",
                closeException);
          }
          throw new PipeException(
              "Failed to construct PipeConnector, because of " + e.getMessage(), e);
        }

        // 2. Construct PipeConnectorSubtaskLifeCycle to manage PipeConnectorSubtask's life cycle
        final PipeSinkSubtask pipeSinkSubtask =
            new PipeSinkSubtask(
                environment.getPipeName(),
                taskID,
                environment.getCreationTime(),
                attributeSortedString,
                connectorIndex,
                pendingQueue,
                pipeConnector);
        final PipeSinkSubtaskLifeCycle pipeSinkSubtaskLifeCycle =
            new PipeSinkSubtaskLifeCycle(executor, pipeSinkSubtask, pendingQueue);
        pipeSinkSubtaskLifeCycleList.add(pipeSinkSubtaskLifeCycle);
      }

      LOGGER.info(
          "Pipe connector subtasks with attributes {} is bounded with connectorExecutor {} and callbackExecutor {}.",
          attributeSortedString,
          executor.getWorkingThreadName(),
          executor.getCallbackThreadName());
      pipeSinkSubtaskKey2SubtaskLifeCycleMap.put(pipeSinkSubtaskKey, pipeSinkSubtaskLifeCycleList);
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey)) {
      lifeCycle.register();
    }

    return attributeSortedString;
  }

  public synchronized void deregister(
      final String pipeName,
      final long creationTime,
      final int regionId,
      final String attributeSortedString) {
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        new PipeSinkSubtaskKey(pipeName, creationTime, attributeSortedString);
    if (!pipeSinkSubtaskKey2SubtaskLifeCycleMap.containsKey(pipeSinkSubtaskKey)) {
      throwNoSuchSubtaskException(pipeSinkSubtaskKey);
    }

    final List<PipeSinkSubtaskLifeCycle> lifeCycles =
        pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey);

    // Shall not be empty
    final PipeSinkSubtaskExecutor executor = lifeCycles.get(0).executor;

    final CommitterKey committerKey =
        PipeEventCommitManager.getInstance().getCommitterKey(pipeName, creationTime, regionId);

    lifeCycles.removeIf(o -> o.deregister(committerKey));

    if (lifeCycles.isEmpty()) {
      pipeSinkSubtaskKey2SubtaskLifeCycleMap.remove(pipeSinkSubtaskKey);
      executor.shutdown();
      LOGGER.info(
          "The executor {} and {} has been successfully shutdown.",
          executor.getWorkingThreadName(),
          executor.getCallbackThreadName());
    }

    PipeEventCommitManager.getInstance().deregister(pipeName, creationTime, regionId);
  }

  public synchronized void start(
      final String pipeName, final long creationTime, final String attributeSortedString) {
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        new PipeSinkSubtaskKey(pipeName, creationTime, attributeSortedString);
    if (!pipeSinkSubtaskKey2SubtaskLifeCycleMap.containsKey(pipeSinkSubtaskKey)) {
      throwNoSuchSubtaskException(pipeSinkSubtaskKey);
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey)) {
      lifeCycle.start();
    }
  }

  /**
   * @deprecated Use {@link #start(String, long, String)} to identify the pipe explicitly.
   */
  @Deprecated
  public synchronized void start(final String attributeSortedString) {
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        getUniquePipeSinkSubtaskKey(attributeSortedString);
    if (pipeSinkSubtaskKey == null) {
      throwNoSuchSubtaskException(
          new PipeSinkSubtaskKey(null, Long.MIN_VALUE, attributeSortedString));
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey)) {
      lifeCycle.start();
    }
  }

  public synchronized void stop(
      final String pipeName, final long creationTime, final String attributeSortedString) {
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        new PipeSinkSubtaskKey(pipeName, creationTime, attributeSortedString);
    if (!pipeSinkSubtaskKey2SubtaskLifeCycleMap.containsKey(pipeSinkSubtaskKey)) {
      throwNoSuchSubtaskException(pipeSinkSubtaskKey);
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey)) {
      lifeCycle.stop();
    }
  }

  /**
   * @deprecated Use {@link #stop(String, long, String)} to identify the pipe explicitly.
   */
  @Deprecated
  public synchronized void stop(final String attributeSortedString) {
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        getUniquePipeSinkSubtaskKey(attributeSortedString);
    if (pipeSinkSubtaskKey == null) {
      throwNoSuchSubtaskException(
          new PipeSinkSubtaskKey(null, Long.MIN_VALUE, attributeSortedString));
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey)) {
      lifeCycle.stop();
    }
  }

  public synchronized UnboundedBlockingPendingQueue<Event> getPipeSinkPendingQueue(
      final String pipeName, final long creationTime, final String attributeSortedString) {
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        new PipeSinkSubtaskKey(pipeName, creationTime, attributeSortedString);
    if (!pipeSinkSubtaskKey2SubtaskLifeCycleMap.containsKey(pipeSinkSubtaskKey)) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    return pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey).get(0).getPendingQueue();
  }

  /**
   * @deprecated Use {@link #getPipeSinkPendingQueue(String, long, String)} to identify the pipe
   *     explicitly.
   */
  @Deprecated
  public UnboundedBlockingPendingQueue<Event> getPipeSinkPendingQueue(
      final String attributeSortedString) {
    final PipeSinkSubtaskKey pipeSinkSubtaskKey =
        getUniquePipeSinkSubtaskKey(attributeSortedString);
    if (pipeSinkSubtaskKey == null) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    // All subtasks share the same pending queue
    return pipeSinkSubtaskKey2SubtaskLifeCycleMap.get(pipeSinkSubtaskKey).get(0).getPendingQueue();
  }

  public synchronized boolean hasRegisteredSubtasks(
      final String pipeName,
      final long creationTime,
      final PipeParameters pipeSinkParameters,
      final int regionId) {
    return pipeSinkSubtaskKey2SubtaskLifeCycleMap.containsKey(
        new PipeSinkSubtaskKey(
            pipeName, creationTime, generateAttributeSortedString(pipeSinkParameters, regionId)));
  }

  /**
   * @deprecated Use {@link #hasRegisteredSubtasks(String, long, PipeParameters, int)} to identify
   *     the pipe explicitly.
   */
  @Deprecated
  public synchronized boolean hasRegisteredSubtasks(
      final PipeParameters pipeSinkParameters, final int regionId) {
    return getUniquePipeSinkSubtaskKey(generateAttributeSortedString(pipeSinkParameters, regionId))
        != null;
  }

  public static int calculateSinkSubtaskNum(
      final PipeParameters pipeSinkParameters, final int regionId) {
    final String connectorKey = getConnectorKey(pipeSinkParameters);
    return calculateSinkSubtaskNum(pipeSinkParameters, isDataRegionSink(regionId), connectorKey);
  }

  public static String generateAttributeSortedString(
      final PipeParameters pipeSinkParameters, final int regionId) {
    final String attributeSortedString = generateAttributeSortedString(pipeSinkParameters);
    if (isDataRegionSink(regionId)) {
      return PipeSinkConstant.isSerializeByRegionEnabled(pipeSinkParameters)
          ? "data_region_" + regionId + "_" + attributeSortedString
          : "data_" + attributeSortedString;
    }
    return "schema_" + attributeSortedString;
  }

  private static String getConnectorKey(final PipeParameters pipeSinkParameters) {
    return PipeSinkConstant.getConnectorOrSinkNameWithDefault(pipeSinkParameters).toLowerCase();
  }

  private static boolean isDataRegionSink(final int regionId) {
    return StorageEngine.getInstance().getAllDataRegionIds().contains(new DataRegionId(regionId))
        || PipeRuntimeMeta.isSourceExternal(regionId);
  }

  private static int calculateSinkSubtaskNum(
      final PipeParameters pipeSinkParameters,
      final boolean isDataRegionSink,
      final String connectorKey) {
    if (!isDataRegionSink) {
      // Do not allow parallel tasks for schema region connectors to avoid the potential disorder of
      // the schema region data transfer.
      return 1;
    }
    if (PipeSinkConstant.isSerializeByRegionEnabled(pipeSinkParameters)) {
      return 1;
    }
    return pipeSinkParameters.getIntOrDefault(
        Arrays.asList(
            PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY,
            PipeSinkConstant.SINK_IOTDB_PARALLEL_TASKS_KEY),
        PipeSinkConstant.SINGLE_THREAD_DEFAULT_SINK.contains(connectorKey)
            ? 1
            : PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_DEFAULT_VALUE);
  }

  private static String generateAttributeSortedString(
      final PipeParameters pipeConnectorParameters) {
    final TreeMap<String, String> sortedStringSourceMap =
        new TreeMap<>(pipeConnectorParameters.getAttribute());
    sortedStringSourceMap.remove(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY);
    return sortedStringSourceMap.toString();
  }

  private void throwNoSuchSubtaskException(final PipeSinkSubtaskKey pipeSinkSubtaskKey) {
    throw new PipeException(
        FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + pipeSinkSubtaskKey.attributeSortedString);
  }

  private PipeSinkSubtaskKey getUniquePipeSinkSubtaskKey(final String attributeSortedString) {
    PipeSinkSubtaskKey matchedKey = null;
    for (final PipeSinkSubtaskKey key : pipeSinkSubtaskKey2SubtaskLifeCycleMap.keySet()) {
      if (!Objects.equals(attributeSortedString, key.attributeSortedString)) {
        continue;
      }
      if (matchedKey != null) {
        throw new PipeException(
            "Multiple pipes match the requested sink subtask. Use the pipe-specific "
                + "PipeSinkSubtaskManager API.");
      }
      matchedKey = key;
    }
    return matchedKey;
  }

  private static final class PipeSinkSubtaskKey {

    private final String pipeName;
    private final long creationTime;
    private final String attributeSortedString;

    private PipeSinkSubtaskKey(
        final String pipeName, final long creationTime, final String attributeSortedString) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.attributeSortedString = attributeSortedString;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof PipeSinkSubtaskKey)) {
        return false;
      }
      final PipeSinkSubtaskKey that = (PipeSinkSubtaskKey) object;
      return creationTime == that.creationTime
          && Objects.equals(pipeName, that.pipeName)
          && Objects.equals(attributeSortedString, that.attributeSortedString);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pipeName, creationTime, attributeSortedString);
    }
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeSinkSubtaskManager() {
    // Do nothing
  }

  private static class PipeSubtaskManagerHolder {
    private static final PipeSinkSubtaskManager INSTANCE = new PipeSinkSubtaskManager();
  }

  public static PipeSinkSubtaskManager instance() {
    return PipeSubtaskManagerHolder.INSTANCE;
  }
}
