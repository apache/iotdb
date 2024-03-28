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

package org.apache.iotdb.db.subscription.task.subtask;

import org.apache.iotdb.commons.exception.subscription.SubscriptionException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskConnectorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.progress.committer.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtaskLifeCycle;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.SUBSCRIPTION_SINK;

public class SubscriptionConnectorSubtaskManager {

  private static final String FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE =
      "Failed to deregister PipeConnectorSubtask. No such subtask: ";

  // one SubscriptionConnectorSubtask for one subscription (one consumer group with one topic)
  private final Map<String, PipeConnectorSubtaskLifeCycle>
      attributeSortedString2SubtaskLifeCycleMap = new HashMap<>();

  public synchronized String register(
      PipeConnectorSubtaskExecutor executor,
      PipeParameters pipeConnectorParameters,
      PipeTaskConnectorRuntimeEnvironment environment) {
    final String connectorKey =
        pipeConnectorParameters
            .getStringOrDefault(
                Arrays.asList(PipeConnectorConstant.CONNECTOR_KEY, PipeConnectorConstant.SINK_KEY),
                BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            // Convert the value of `CONNECTOR_KEY` or `SINK_KEY` to lowercase
            // for matching in `CONNECTOR_CONSTRUCTORS`
            .toLowerCase();
    if (!SUBSCRIPTION_SINK.getPipePluginName().equals(connectorKey)) {
      throw new SubscriptionException(
          "The SubscriptionConnectorSubtaskManager only supports subscription-sink.");
    }

    PipeEventCommitManager.getInstance()
        .register(environment.getPipeName(), environment.getRegionId(), connectorKey);

    String attributeSortedString = new TreeMap<>(pipeConnectorParameters.getAttribute()).toString();
    attributeSortedString = "__subscription_" + attributeSortedString;

    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      final BoundedBlockingPendingQueue<Event> pendingQueue =
          new BoundedBlockingPendingQueue<>(
              PipeConfig.getInstance().getPipeConnectorPendingQueueSize(),
              new PipeDataRegionEventCounter());

      final PipeConnector pipeConnector =
          PipeAgent.plugin().dataRegion().reflectConnector(pipeConnectorParameters);
      // 1. Construct, validate and customize PipeConnector, and then handshake (create connection)
      // with the target
      try {
        pipeConnector.validate(new PipeParameterValidator(pipeConnectorParameters));
        pipeConnector.customize(
            pipeConnectorParameters, new PipeTaskRuntimeConfiguration(environment));
        pipeConnector.handshake();
      } catch (Exception e) {
        throw new PipeException(
            "Failed to construct PipeConnector, because of " + e.getMessage(), e);
      }

      // 2. Fetch topic and consumer group id from connector parameters
      final String topicName =
          pipeConnectorParameters.getString(PipeConnectorConstant.SINK_TOPIC_KEY);
      final String consumerGroupId =
          pipeConnectorParameters.getString(PipeConnectorConstant.SINK_CONSUMER_GROUP_KEY);
      if (Objects.isNull(topicName) || Objects.isNull(consumerGroupId)) {
        throw new SubscriptionException(
            String.format(
                "Failed to construct subscription connector, because of %s or %s does not exist in pipe connector parameters",
                PipeConnectorConstant.SINK_TOPIC_KEY,
                PipeConnectorConstant.SINK_CONSUMER_GROUP_KEY));
      }

      // 3. Construct PipeConnectorSubtaskLifeCycle to manage PipeConnectorSubtask's life cycle
      final PipeConnectorSubtask subtask =
          new SubscriptionConnectorSubtask(
              String.format("%s_%s", attributeSortedString, environment.getCreationTime()),
              environment.getCreationTime(),
              attributeSortedString,
              0,
              pendingQueue,
              pipeConnector,
              topicName,
              consumerGroupId);
      final PipeConnectorSubtaskLifeCycle pipeConnectorSubtaskLifeCycle =
          new SubscriptionConnectorSubtaskLifeCycle(executor, subtask, pendingQueue);

      attributeSortedString2SubtaskLifeCycleMap.put(
          attributeSortedString, pipeConnectorSubtaskLifeCycle);
    }

    PipeConnectorSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    lifeCycle.register();

    return attributeSortedString;
  }

  public synchronized void deregister(
      String pipeName, int dataRegionId, String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    PipeConnectorSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    if (lifeCycle.deregister(pipeName)) {
      attributeSortedString2SubtaskLifeCycleMap.remove(attributeSortedString);
    }

    PipeEventCommitManager.getInstance().deregister(pipeName, dataRegionId);
  }

  public synchronized void start(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    PipeConnectorSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    lifeCycle.start();
  }

  public synchronized void stop(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    PipeConnectorSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    lifeCycle.stop();
  }

  public BoundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue(
      String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    return attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).getPendingQueue();
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private SubscriptionConnectorSubtaskManager() {
    // Do nothing
  }

  private static class SubscriptionConnectorSubtaskManagerHolder {
    private static final SubscriptionConnectorSubtaskManager INSTANCE =
        new SubscriptionConnectorSubtaskManager();
  }

  public static SubscriptionConnectorSubtaskManager instance() {
    return SubscriptionConnectorSubtaskManager.SubscriptionConnectorSubtaskManagerHolder.INSTANCE;
  }
}
