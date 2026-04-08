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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeTaskInfoConsensusPipeTest {

  private PipeTaskInfo pipeTaskInfo;

  @Before
  public void setUp() {
    pipeTaskInfo = new PipeTaskInfo();
  }

  private void createPipe(String pipeName, PipeStatus initialStatus) {
    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();
    extractorAttributes.put("extractor", "iotdb-source");
    processorAttributes.put("processor", "do-nothing-processor");
    connectorAttributes.put("connector", "iotdb-thrift-sink");

    PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    ConcurrentMap<Integer, PipeTaskMeta> pipeTasks = new ConcurrentHashMap<>();
    pipeTasks.put(1, pipeTaskMeta);
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            pipeName,
            System.currentTimeMillis(),
            extractorAttributes,
            processorAttributes,
            connectorAttributes);
    PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    CreatePipePlanV2 createPlan = new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta);
    pipeTaskInfo.createPipe(createPlan);

    if (PipeStatus.RUNNING.equals(initialStatus)) {
      pipeTaskInfo.setPipeStatus(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));
    }
  }

  @Test
  public void testGetConsensusPipeStatusMapEmpty() {
    Map<String, PipeStatus> result = pipeTaskInfo.getConsensusPipeStatusMap();
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testGetConsensusPipeStatusMapFiltersOnlyConsensusPipes() {
    String userPipeName = "myUserPipe";
    createPipe(userPipeName, PipeStatus.RUNNING);

    DataRegionId regionId = new DataRegionId(100);
    String consensusPipeName1 = new ConsensusPipeName(regionId, 1, 2).toString();
    String consensusPipeName2 = new ConsensusPipeName(regionId, 2, 1).toString();
    createPipe(consensusPipeName1, PipeStatus.RUNNING);
    createPipe(consensusPipeName2, PipeStatus.STOPPED);

    Map<String, PipeStatus> result = pipeTaskInfo.getConsensusPipeStatusMap();

    Assert.assertEquals(2, result.size());
    Assert.assertFalse(result.containsKey(userPipeName));
    Assert.assertEquals(PipeStatus.RUNNING, result.get(consensusPipeName1));
    Assert.assertEquals(PipeStatus.STOPPED, result.get(consensusPipeName2));
  }

  @Test
  public void testGetConsensusPipeStatusMapWithOnlyUserPipes() {
    createPipe("userPipe1", PipeStatus.RUNNING);
    createPipe("userPipe2", PipeStatus.STOPPED);

    Map<String, PipeStatus> result = pipeTaskInfo.getConsensusPipeStatusMap();

    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testGetConsensusPipeStatusMapWithMultipleRegions() {
    DataRegionId region1 = new DataRegionId(100);
    DataRegionId region2 = new DataRegionId(200);

    String pipe1 = new ConsensusPipeName(region1, 1, 2).toString();
    String pipe2 = new ConsensusPipeName(region1, 2, 1).toString();
    String pipe3 = new ConsensusPipeName(region2, 3, 4).toString();
    String pipe4 = new ConsensusPipeName(region2, 4, 3).toString();
    createPipe(pipe1, PipeStatus.RUNNING);
    createPipe(pipe2, PipeStatus.RUNNING);
    createPipe(pipe3, PipeStatus.STOPPED);
    createPipe(pipe4, PipeStatus.RUNNING);

    Map<String, PipeStatus> result = pipeTaskInfo.getConsensusPipeStatusMap();

    Assert.assertEquals(4, result.size());
    Assert.assertEquals(PipeStatus.RUNNING, result.get(pipe1));
    Assert.assertEquals(PipeStatus.RUNNING, result.get(pipe2));
    Assert.assertEquals(PipeStatus.STOPPED, result.get(pipe3));
    Assert.assertEquals(PipeStatus.RUNNING, result.get(pipe4));
  }

  @Test
  public void testGetConsensusPipeStatusMapExcludesSubscriptionPipes() {
    String subscriptionPipeName = PipeStaticMeta.SUBSCRIPTION_PIPE_PREFIX + "topic1.group1";
    createPipe(subscriptionPipeName, PipeStatus.RUNNING);

    DataRegionId regionId = new DataRegionId(100);
    String consensusPipeName = new ConsensusPipeName(regionId, 1, 2).toString();
    createPipe(consensusPipeName, PipeStatus.RUNNING);

    Map<String, PipeStatus> result = pipeTaskInfo.getConsensusPipeStatusMap();

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey(consensusPipeName));
    Assert.assertFalse(result.containsKey(subscriptionPipeName));
  }

  @Test
  public void testProcessLoadSnapshotRestartsOnlyHealthyStoppedConsensusPipes() throws Exception {
    DataRegionId regionId = new DataRegionId(100);
    String consensusPipeToRestart = new ConsensusPipeName(regionId, 1, 2).toString();
    String consensusPipeStoppedByException = new ConsensusPipeName(regionId, 2, 1).toString();
    String userPipeName = "userPipe";

    createPipe(consensusPipeToRestart, PipeStatus.STOPPED);
    createPipe(consensusPipeStoppedByException, PipeStatus.STOPPED);
    createPipe(userPipeName, PipeStatus.STOPPED);

    pipeTaskInfo
        .getPipeMetaByPipeName(consensusPipeStoppedByException)
        .getRuntimeMeta()
        .setIsStoppedByRuntimeException(true);

    final File snapshotDir =
        java.nio.file.Files.createTempDirectory("pipe-task-info-consensus-test").toFile();
    try {
      Assert.assertTrue(pipeTaskInfo.processTakeSnapshot(snapshotDir));

      PipeTaskInfo recoveredPipeTaskInfo = new PipeTaskInfo();
      recoveredPipeTaskInfo.processLoadSnapshot(snapshotDir);

      Assert.assertEquals(
          PipeStatus.RUNNING,
          recoveredPipeTaskInfo
              .getPipeMetaByPipeName(consensusPipeToRestart)
              .getRuntimeMeta()
              .getStatus()
              .get());
      Assert.assertEquals(
          PipeStatus.STOPPED,
          recoveredPipeTaskInfo
              .getPipeMetaByPipeName(consensusPipeStoppedByException)
              .getRuntimeMeta()
              .getStatus()
              .get());
      Assert.assertTrue(
          recoveredPipeTaskInfo
              .getPipeMetaByPipeName(consensusPipeStoppedByException)
              .getRuntimeMeta()
              .getIsStoppedByRuntimeException());
      Assert.assertEquals(
          PipeStatus.STOPPED,
          recoveredPipeTaskInfo
              .getPipeMetaByPipeName(userPipeName)
              .getRuntimeMeta()
              .getStatus()
              .get());
    } finally {
      new File(snapshotDir, "pipe_task_info.bin").delete();
      snapshotDir.delete();
    }
  }
}
