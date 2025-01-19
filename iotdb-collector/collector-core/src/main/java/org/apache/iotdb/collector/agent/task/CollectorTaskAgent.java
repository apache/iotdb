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

package org.apache.iotdb.collector.agent.task;

import org.apache.iotdb.collector.agent.executor.CollectorProcessorTaskExecutor;
import org.apache.iotdb.collector.agent.executor.CollectorSinkTaskExecutor;
import org.apache.iotdb.collector.agent.executor.CollectorSourceTaskExecutor;
import org.apache.iotdb.collector.agent.executor.CollectorTaskExecutorAgent;
import org.apache.iotdb.collector.agent.plugin.CollectorPluginAgent;
import org.apache.iotdb.collector.agent.plugin.CollectorPluginConstructor;
import org.apache.iotdb.collector.plugin.BuiltinCollectorPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class CollectorTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorTaskAgent.class);

  private static final CollectorPluginConstructor CONSTRUCTOR =
      CollectorPluginAgent.instance().constructor();
  private static final CollectorSourceTaskExecutor SOURCE_TASK_EXECUTOR =
      CollectorTaskExecutorAgent.instance().getSourceTaskExecutor();
  private static final CollectorProcessorTaskExecutor PROCESSOR_TASK_EXECUTOR =
      CollectorTaskExecutorAgent.instance().getProcessorTaskExecutor();
  private static final CollectorSinkTaskExecutor SINK_TASK_EXECUTOR =
      CollectorTaskExecutorAgent.instance().getSinkTaskExecutor();

  private CollectorTaskAgent() {}

  public boolean createCollectorTask(
      final Map<String, String> sourceAttribute,
      final Map<String, String> processorAttribute,
      final Map<String, String> sinkAttribute,
      final String taskId) {
    try {
      final CollectorSourceTask collectorSourceTask =
          new CollectorSourceTask(
              taskId,
              sourceAttribute,
              CONSTRUCTOR.getSource(
                  sourceAttribute.getOrDefault(
                      "source-plugin",
                      BuiltinCollectorPlugin.HTTP_SOURCE.getCollectorPluginName())));
      SOURCE_TASK_EXECUTOR.register(collectorSourceTask);

      final CollectorProcessorTask collectorProcessorTask =
          new CollectorProcessorTask(
              taskId,
              processorAttribute,
              CONSTRUCTOR.getProcessor(
                  processorAttribute.getOrDefault(
                      "processor-plugin",
                      BuiltinCollectorPlugin.DO_NOTHING_PROCESSOR.getCollectorPluginName())),
              collectorSourceTask.getEventSupplier(),
              new LinkedBlockingQueue<>());
      PROCESSOR_TASK_EXECUTOR.register(collectorProcessorTask);

      final CollectorSinkTask collectorSinkTask =
          new CollectorSinkTask(
              taskId,
              sinkAttribute,
              CONSTRUCTOR.getSink(
                  sinkAttribute.getOrDefault(
                      "sink-plugin",
                      BuiltinCollectorPlugin.IOTDB_SESSION_SINK.getCollectorPluginName())),
              collectorProcessorTask.getPendingQueue());
      SINK_TASK_EXECUTOR.register(collectorSinkTask);
    } catch (final Exception e) {
      LOGGER.warn("create collector task error", e);
      return false;
    }

    return true;
  }

  public boolean stopCollectorTask(final String taskId) {
    try {
      SOURCE_TASK_EXECUTOR.deregister(taskId);
      PROCESSOR_TASK_EXECUTOR.deregister(taskId);
      SINK_TASK_EXECUTOR.deregister(taskId);
    } catch (final Exception e) {
      LOGGER.warn("stop collector task error", e);
      return false;
    }

    return true;
  }

  public static CollectorTaskAgent instance() {
    return CollectorTaskAgentHolder.INSTANCE;
  }

  private static class CollectorTaskAgentHolder {
    private static final CollectorTaskAgent INSTANCE = new CollectorTaskAgent();
  }
}
