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

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeSinkSubtaskManagerTest {

  @Test
  public void testSubtasksAreSharedOnlyWithinSamePipe() {
    // Initialize the task agent used by PipeEventCommitManager before registering subtasks.
    PipeDataNodeAgent.task();

    final String firstPipeName = "firstPipe";
    final String secondPipeName = "secondPipe";
    final long creationTime = 1L;
    final int firstRegionId = -1;
    final int secondRegionId = -2;
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(
        PipeSinkConstant.CONNECTOR_KEY, BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName());
    attributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, "1");
    final PipeParameters parameters = new PipeParameters(attributes);
    final PipeSinkSubtaskManager manager = PipeSinkSubtaskManager.instance();

    boolean firstRegionRegistered = false;
    boolean secondRegionRegistered = false;
    boolean secondPipeRegistered = false;
    try {
      final String firstPipeSubtaskId =
          manager.register(
              PipeSinkSubtaskExecutor::new,
              parameters,
              new PipeTaskSinkRuntimeEnvironment(firstPipeName, creationTime, firstRegionId));
      firstRegionRegistered = true;
      final String firstPipeSecondRegionSubtaskId =
          manager.register(
              PipeSinkSubtaskExecutor::new,
              parameters,
              new PipeTaskSinkRuntimeEnvironment(firstPipeName, creationTime, secondRegionId));
      secondRegionRegistered = true;
      final UnboundedBlockingPendingQueue<Event> firstPipeQueue =
          manager.getPipeSinkPendingQueue(firstPipeName, creationTime, firstPipeSubtaskId);
      Assert.assertSame(firstPipeQueue, manager.getPipeSinkPendingQueue(firstPipeSubtaskId));
      Assert.assertTrue(manager.hasRegisteredSubtasks(parameters, firstRegionId));

      final String secondPipeSubtaskId =
          manager.register(
              PipeSinkSubtaskExecutor::new,
              parameters,
              new PipeTaskSinkRuntimeEnvironment(secondPipeName, creationTime, firstRegionId));
      secondPipeRegistered = true;

      Assert.assertSame(
          firstPipeQueue,
          manager.getPipeSinkPendingQueue(
              firstPipeName, creationTime, firstPipeSecondRegionSubtaskId));
      Assert.assertNotSame(
          firstPipeQueue,
          manager.getPipeSinkPendingQueue(secondPipeName, creationTime, secondPipeSubtaskId));
      Assert.assertThrows(
          PipeException.class, () -> manager.getPipeSinkPendingQueue(firstPipeSubtaskId));
      Assert.assertThrows(
          PipeException.class, () -> manager.hasRegisteredSubtasks(parameters, firstRegionId));
      Assert.assertThrows(PipeException.class, () -> manager.start(firstPipeSubtaskId));
      Assert.assertThrows(PipeException.class, () -> manager.stop(firstPipeSubtaskId));
    } finally {
      if (secondRegionRegistered) {
        manager.deregister(
            firstPipeName,
            creationTime,
            secondRegionId,
            PipeSinkSubtaskManager.generateAttributeSortedString(parameters, secondRegionId));
      }
      if (firstRegionRegistered) {
        manager.deregister(
            firstPipeName,
            creationTime,
            firstRegionId,
            PipeSinkSubtaskManager.generateAttributeSortedString(parameters, firstRegionId));
      }
      if (secondPipeRegistered) {
        manager.deregister(
            secondPipeName,
            creationTime,
            firstRegionId,
            PipeSinkSubtaskManager.generateAttributeSortedString(parameters, firstRegionId));
      }
    }
  }

  @Test
  public void testGenerateAttributeSortedStringUsesSerializeByRegionAndIgnoresRestartFlag() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("z", "1");
    attributes.put("a", "2");
    attributes.put(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.TRUE.toString());

    Assert.assertEquals(
        "data_{a=2, z=1}",
        PipeSinkSubtaskManager.generateAttributeSortedString(
            new PipeParameters(new HashMap<>(attributes)), -1));

    attributes.put(PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.TRUE.toString());
    Assert.assertEquals(
        "data_region_-1_{a=2, connector.serialize-by-region=true, z=1}",
        PipeSinkSubtaskManager.generateAttributeSortedString(
            new PipeParameters(new HashMap<>(attributes)), -1));
  }

  @Test
  public void testCalculateSinkSubtaskNumForDataRegionSink() {
    final Map<String, String> parallelAttributes = new HashMap<>();
    parallelAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.FALSE.toString());
    parallelAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, "3");
    Assert.assertEquals(
        3,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(new PipeParameters(parallelAttributes), -1));

    final Map<String, String> serializedAttributes = new HashMap<>();
    serializedAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.TRUE.toString());
    serializedAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, "3");
    Assert.assertEquals(
        1,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(
            new PipeParameters(serializedAttributes), -1));
  }

  @Test
  public void testCalculateSinkSubtaskNumUsesSingleThreadDefaultSinkAndSchemaRegionLimit() {
    final Map<String, String> singleThreadAttributes = new HashMap<>();
    singleThreadAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.FALSE.toString());
    singleThreadAttributes.put(
        PipeSinkConstant.CONNECTOR_KEY, BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName());
    Assert.assertEquals(
        1,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(
            new PipeParameters(singleThreadAttributes), -1));

    final Map<String, String> schemaRegionAttributes = new HashMap<>();
    schemaRegionAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.FALSE.toString());
    schemaRegionAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, "3");
    Assert.assertEquals(
        1,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(
            new PipeParameters(schemaRegionAttributes), Integer.MAX_VALUE));
  }
}
