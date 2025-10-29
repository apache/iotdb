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
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class PipeSinkSubtaskManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSinkSubtaskManager.class);

  private static final String FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE =
      "Failed to deregister PipeConnectorSubtask. No such subtask: ";

  private final Map<String, List<PipeSinkSubtaskLifeCycle>>
      attributeSortedString2SubtaskLifeCycleMap = new HashMap<>();

  public synchronized String register(
      final Supplier<? extends PipeSinkSubtaskExecutor> executorSupplier,
      final PipeParameters pipeConnectorParameters,
      final PipeTaskSinkRuntimeEnvironment environment) {
    final String connectorKey =
        pipeConnectorParameters
            .getStringOrDefault(
                Arrays.asList(PipeSinkConstant.CONNECTOR_KEY, PipeSinkConstant.SINK_KEY),
                BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            // Convert the value of `CONNECTOR_KEY` or `SINK_KEY` to lowercase
            // for matching in `CONNECTOR_CONSTRUCTORS`
            .toLowerCase();
    PipeEventCommitManager.getInstance()
        .register(
            environment.getPipeName(),
            environment.getCreationTime(),
            environment.getRegionId(),
            connectorKey);

    final boolean isDataRegionConnector =
        StorageEngine.getInstance()
                .getAllDataRegionIds()
                .contains(new DataRegionId(environment.getRegionId()))
            || PipeRuntimeMeta.isSourceExternal(environment.getRegionId());

    final int connectorNum;
    boolean realTimeFirst = false;
    String attributeSortedString = generateAttributeSortedString(pipeConnectorParameters);
    if (isDataRegionConnector) {
      connectorNum =
          pipeConnectorParameters.getIntOrDefault(
              Arrays.asList(
                  PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY,
                  PipeSinkConstant.SINK_IOTDB_PARALLEL_TASKS_KEY),
              PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_DEFAULT_VALUE);
      realTimeFirst =
          pipeConnectorParameters.getBooleanOrDefault(
              Arrays.asList(
                  PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY,
                  PipeSinkConstant.SINK_REALTIME_FIRST_KEY),
              PipeSinkConstant.CONNECTOR_REALTIME_FIRST_DEFAULT_VALUE);
      attributeSortedString = "data_" + attributeSortedString;
    } else {
      // Do not allow parallel tasks for schema region connectors
      // to avoid the potential disorder of the schema region data transfer
      connectorNum = 1;
      attributeSortedString = "schema_" + attributeSortedString;
    }
    environment.setAttributeSortedString(attributeSortedString);

    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      final PipeSinkSubtaskExecutor executor = executorSupplier.get();
      final List<PipeSinkSubtaskLifeCycle> pipeSinkSubtaskLifeCycleList =
          new ArrayList<>(connectorNum);

      AtomicInteger counter = new AtomicInteger(0);
      // Shared pending queue for all subtasks
      final UnboundedBlockingPendingQueue<Event> pendingQueue =
          realTimeFirst
              ? new PipeRealtimePriorityBlockingQueue()
              : new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

      if (realTimeFirst) {
        ((PipeRealtimePriorityBlockingQueue) pendingQueue).setOfferTsFileCounter(counter);
      }

      for (int connectorIndex = 0; connectorIndex < connectorNum; connectorIndex++) {
        final PipeConnector pipeConnector =
            isDataRegionConnector
                ? PipeDataNodeAgent.plugin().dataRegion().reflectSink(pipeConnectorParameters)
                : PipeDataNodeAgent.plugin().schemaRegion().reflectSink(pipeConnectorParameters);
        // 1. Construct, validate and customize PipeConnector, and then handshake (create
        // connection) with the target
        try {
          if (pipeConnector instanceof IoTDBDataRegionAsyncSink) {
            ((IoTDBDataRegionAsyncSink) pipeConnector).setTransferTsFileCounter(counter);
          }
          pipeConnector.validate(new PipeParameterValidator(pipeConnectorParameters));
          pipeConnector.customize(
              pipeConnectorParameters, new PipeTaskRuntimeConfiguration(environment));
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
                String.format(
                    "%s_%s_%s",
                    attributeSortedString, environment.getCreationTime(), connectorIndex),
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
      attributeSortedString2SubtaskLifeCycleMap.put(
          attributeSortedString, pipeSinkSubtaskLifeCycleList);
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      lifeCycle.register();
    }

    return attributeSortedString;
  }

  public synchronized void deregister(
      final String pipeName,
      final long creationTime,
      final int regionId,
      final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    final List<PipeSinkSubtaskLifeCycle> lifeCycles =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);

    // Shall not be empty
    final PipeSinkSubtaskExecutor executor = lifeCycles.get(0).executor;

    lifeCycles.removeIf(o -> o.deregister(pipeName, regionId));

    if (lifeCycles.isEmpty()) {
      attributeSortedString2SubtaskLifeCycleMap.remove(attributeSortedString);
      executor.shutdown();
      LOGGER.info(
          "The executor {} and {} has been successfully shutdown.",
          executor.getWorkingThreadName(),
          executor.getCallbackThreadName());
    }

    PipeEventCommitManager.getInstance().deregister(pipeName, creationTime, regionId);
    // Reset IoTV2 replicate index to prevent index jumps. Do this when a consensus pipe no longer
    // replicates data, since extractor and processor are already dropped now.
    ReplicateProgressDataNodeManager.resetReplicateIndexForIoTV2(pipeName);
  }

  public synchronized void start(final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      lifeCycle.start();
    }
  }

  public synchronized void stop(final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    for (final PipeSinkSubtaskLifeCycle lifeCycle :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      lifeCycle.stop();
    }
  }

  public UnboundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue(
      final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    // All subtasks share the same pending queue
    return attributeSortedString2SubtaskLifeCycleMap
        .get(attributeSortedString)
        .get(0)
        .getPendingQueue();
  }

  private String generateAttributeSortedString(final PipeParameters pipeConnectorParameters) {
    final TreeMap<String, String> sortedStringSourceMap =
        new TreeMap<>(pipeConnectorParameters.getAttribute());
    sortedStringSourceMap.remove(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY);
    return sortedStringSourceMap.toString();
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
