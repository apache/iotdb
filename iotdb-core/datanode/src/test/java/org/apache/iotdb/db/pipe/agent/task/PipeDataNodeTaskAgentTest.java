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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.when;

public class PipeDataNodeTaskAgentTest {

  private static final int LOCAL_NODE_ID = 1;
  private static final int REGION_ID = 7;

  @Test
  public void testIsAllExpectedDataRegionCompleted() {
    final PipeDataNodeTask completedTask = Mockito.mock(PipeDataNodeTask.class);
    when(completedTask.isCompleted()).thenReturn(true);
    final PipeDataNodeTask uncompletedTask = Mockito.mock(PipeDataNodeTask.class);
    when(uncompletedTask.isCompleted()).thenReturn(false);

    final Map<Integer, PipeTask> pipeTaskMap = new HashMap<>();
    pipeTaskMap.put(1, completedTask);
    pipeTaskMap.put(2, completedTask);

    final Set<Integer> completedExpectedRegionIds = new HashSet<>(Arrays.asList(1, 2));
    Assert.assertTrue(
        PipeDataNodeTaskAgent.isAllExpectedDataRegionCompleted(
            pipeTaskMap, completedExpectedRegionIds));

    // A DataRegion that should be transferred is missing its local PipeTask.
    final Set<Integer> partiallyMissingExpectedRegionIds = new HashSet<>(Arrays.asList(1, 2, 3));
    Assert.assertFalse(
        PipeDataNodeTaskAgent.isAllExpectedDataRegionCompleted(
            pipeTaskMap, partiallyMissingExpectedRegionIds));

    // No local target DataRegion means this DataNode does not need to transfer history.
    Assert.assertTrue(
        PipeDataNodeTaskAgent.isAllExpectedDataRegionCompleted(null, Collections.emptySet()));

    // A non-empty expected set with a missing PipeTaskMap means initialization failed.
    Assert.assertFalse(
        PipeDataNodeTaskAgent.isAllExpectedDataRegionCompleted(
            null, partiallyMissingExpectedRegionIds));

    // An uncompleted PipeTask must not be reported as completed.
    pipeTaskMap.put(3, uncompletedTask);
    Assert.assertFalse(
        PipeDataNodeTaskAgent.isAllExpectedDataRegionCompleted(
            pipeTaskMap, partiallyMissingExpectedRegionIds));
  }

  @Test
  public void testGetPipeTaskProgressIndexReportsMissingTaskMeta() throws Exception {
    final PipeDataNodeTaskAgent taskAgent = new PipeDataNodeTaskAgent();
    final Field pipeMetaKeeperField = PipeTaskAgent.class.getDeclaredField("pipeMetaKeeper");
    pipeMetaKeeperField.setAccessible(true);
    final PipeMetaKeeper pipeMetaKeeper = (PipeMetaKeeper) pipeMetaKeeperField.get(taskAgent);

    final String pipeName = PipeStaticMeta.CONSENSUS_PIPE_PREFIX + "DataRegion[7]_1_2";
    pipeMetaKeeper.addPipeMeta(
        new PipeMeta(
            new PipeStaticMeta(pipeName, 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta()));

    final PipeException exception =
        Assert.assertThrows(
            PipeException.class, () -> taskAgent.getPipeTaskProgressIndex(pipeName, REGION_ID));
    Assert.assertTrue(exception.getMessage().contains(pipeName));
    Assert.assertTrue(exception.getMessage().contains(String.valueOf(REGION_ID)));
  }

  @Test
  public void testCreateMemoryCheckStillRunsWhenNoPipeTasksNeedToBeCreated() throws Exception {
    final boolean originalPipeEnableMemoryCheck =
        CommonDescriptor.getInstance().getConfig().isPipeEnableMemoryChecked();
    final long originalPipeInsertNodeQueueMemory =
        CommonDescriptor.getInstance().getConfig().getPipeInsertNodeQueueMemory();
    final double originalPipeTotalFloatingMemoryProportion =
        CommonDescriptor.getInstance().getConfig().getPipeTotalFloatingMemoryProportion();

    try {
      CommonDescriptor.getInstance().getConfig().setIsPipeEnableMemoryChecked(true);
      CommonDescriptor.getInstance().getConfig().setPipeInsertNodeQueueMemory(1);
      CommonDescriptor.getInstance().getConfig().setPipeTotalFloatingMemoryProportion(0);

      Assert.assertThrows(
          PipeException.class,
          () ->
              PipeDataNodeAgent.task()
                  .calculateMemoryUsage(
                      new PipeMeta(
                          new PipeStaticMeta(
                              "p", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
                          new PipeRuntimeMeta())));
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setIsPipeEnableMemoryChecked(originalPipeEnableMemoryCheck);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeInsertNodeQueueMemory(originalPipeInsertNodeQueueMemory);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeTotalFloatingMemoryProportion(originalPipeTotalFloatingMemoryProportion);
    }
  }

  @Test
  public void testCarryOverCommittedProgressForResumeAlter() {
    final PipeStaticMeta oldStaticMeta = createStaticMeta(1, false);
    final PipeStaticMeta updatedStaticMeta = createStaticMeta(2, false);
    final PipeMeta localOldPipeMeta =
        createPipeMeta(oldStaticMeta, new SimpleProgressIndex(1, 20L), LOCAL_NODE_ID);
    final PipeMeta updatedPipeMeta =
        createPipeMeta(updatedStaticMeta, new SimpleProgressIndex(1, 10L), LOCAL_NODE_ID);

    PipeDataNodeTaskAgent.carryOverLocalProgressIndexForAlter(
        oldStaticMeta,
        localOldPipeMeta,
        updatedPipeMeta,
        LOCAL_NODE_ID,
        (staticMeta, regionId) -> regionId == REGION_ID);

    Assert.assertEquals(
        new SimpleProgressIndex(1, 20L),
        updatedPipeMeta
            .getRuntimeMeta()
            .getConsensusGroupId2TaskMetaMap()
            .get(REGION_ID)
            .getProgressIndex());
  }

  @Test
  public void testCarryOverDoesNotOverrideCoordinatorProgress() {
    final PipeStaticMeta oldStaticMeta = createStaticMeta(1, false);
    final PipeStaticMeta updatedStaticMeta = createStaticMeta(2, false);
    final PipeMeta localOldPipeMeta =
        createPipeMeta(oldStaticMeta, new SimpleProgressIndex(1, 10L), LOCAL_NODE_ID);
    final PipeMeta updatedPipeMeta =
        createPipeMeta(updatedStaticMeta, new SimpleProgressIndex(1, 20L), LOCAL_NODE_ID);

    PipeDataNodeTaskAgent.carryOverLocalProgressIndexForAlter(
        oldStaticMeta,
        localOldPipeMeta,
        updatedPipeMeta,
        LOCAL_NODE_ID,
        (staticMeta, regionId) -> true);

    Assert.assertEquals(
        new SimpleProgressIndex(1, 20L),
        updatedPipeMeta
            .getRuntimeMeta()
            .getConsensusGroupId2TaskMetaMap()
            .get(REGION_ID)
            .getProgressIndex());
  }

  @Test
  public void testCarryOverDoesNotOverrideProgressResetOnModeChange() {
    final PipeStaticMeta oldStaticMeta = createStaticMeta(1, false);
    final PipeStaticMeta updatedStaticMeta = createStaticMeta(2, true);
    final PipeMeta localOldPipeMeta =
        createPipeMeta(oldStaticMeta, new SimpleProgressIndex(1, 20L), LOCAL_NODE_ID);
    final PipeMeta updatedPipeMeta =
        createPipeMeta(updatedStaticMeta, MinimumProgressIndex.INSTANCE, LOCAL_NODE_ID);

    PipeDataNodeTaskAgent.carryOverLocalProgressIndexForAlter(
        oldStaticMeta,
        localOldPipeMeta,
        updatedPipeMeta,
        LOCAL_NODE_ID,
        (staticMeta, regionId) -> true);

    Assert.assertSame(
        MinimumProgressIndex.INSTANCE,
        updatedPipeMeta
            .getRuntimeMeta()
            .getConsensusGroupId2TaskMetaMap()
            .get(REGION_ID)
            .getProgressIndex());
  }

  @Test
  public void testCarryOverRequiresStableLeaderAndLocalTask() {
    final PipeStaticMeta oldStaticMeta = createStaticMeta(1, false);
    final PipeStaticMeta updatedStaticMeta = createStaticMeta(2, false);
    final PipeMeta localOldPipeMeta =
        createPipeMeta(oldStaticMeta, new SimpleProgressIndex(1, 20L), LOCAL_NODE_ID);

    final PipeMeta localOldPipeMetaWithLeaderChange =
        createPipeMeta(oldStaticMeta, new SimpleProgressIndex(1, 20L), 2);
    final PipeMeta updatedWithOldLeaderChange =
        createPipeMeta(updatedStaticMeta, new SimpleProgressIndex(1, 10L), LOCAL_NODE_ID);
    PipeDataNodeTaskAgent.carryOverLocalProgressIndexForAlter(
        oldStaticMeta,
        localOldPipeMetaWithLeaderChange,
        updatedWithOldLeaderChange,
        LOCAL_NODE_ID,
        (staticMeta, regionId) -> true);
    Assert.assertEquals(
        new SimpleProgressIndex(1, 10L),
        updatedWithOldLeaderChange
            .getRuntimeMeta()
            .getConsensusGroupId2TaskMetaMap()
            .get(REGION_ID)
            .getProgressIndex());

    final PipeMeta updatedWithLeaderChange =
        createPipeMeta(updatedStaticMeta, new SimpleProgressIndex(1, 10L), 2);
    PipeDataNodeTaskAgent.carryOverLocalProgressIndexForAlter(
        oldStaticMeta,
        localOldPipeMeta,
        updatedWithLeaderChange,
        LOCAL_NODE_ID,
        (staticMeta, regionId) -> true);
    Assert.assertEquals(
        new SimpleProgressIndex(1, 10L),
        updatedWithLeaderChange
            .getRuntimeMeta()
            .getConsensusGroupId2TaskMetaMap()
            .get(REGION_ID)
            .getProgressIndex());

    final PipeMeta updatedWithoutLocalTask =
        createPipeMeta(updatedStaticMeta, new SimpleProgressIndex(1, 10L), LOCAL_NODE_ID);
    PipeDataNodeTaskAgent.carryOverLocalProgressIndexForAlter(
        oldStaticMeta,
        localOldPipeMeta,
        updatedWithoutLocalTask,
        LOCAL_NODE_ID,
        (staticMeta, regionId) -> false);
    Assert.assertEquals(
        new SimpleProgressIndex(1, 10L),
        updatedWithoutLocalTask
            .getRuntimeMeta()
            .getConsensusGroupId2TaskMetaMap()
            .get(REGION_ID)
            .getProgressIndex());
  }

  private PipeStaticMeta createStaticMeta(final long creationTime, final boolean historyEnabled) {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(
        PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY, Boolean.toString(historyEnabled));
    return new PipeStaticMeta(
        String.valueOf('p'), creationTime, sourceAttributes, new HashMap<>(), new HashMap<>());
  }

  private PipeMeta createPipeMeta(
      final PipeStaticMeta staticMeta, final ProgressIndex progressIndex, final int leaderId) {
    final ConcurrentMap<Integer, PipeTaskMeta> taskMetaMap = new ConcurrentHashMap<>();
    taskMetaMap.put(REGION_ID, new PipeTaskMeta(progressIndex, leaderId));
    return new PipeMeta(staticMeta, new PipeRuntimeMeta(taskMetaMap));
  }
}
