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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskManager;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class PipeDataNodeTaskAgentTest {

  private static final int LOCAL_NODE_ID = 1;
  private static final int REGION_ID = 7;

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
  public void testSinkCriticalExceptionIsPropagatedOnlyWithinItsPipe() throws Exception {
    final PipeDataNodeTaskAgent taskAgent = new PipeDataNodeTaskAgent();
    final PipeMetaKeeper pipeMetaKeeper = getField(taskAgent, "pipeMetaKeeper");
    final int localNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

    final PipeTaskMeta failedTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final PipeTaskMeta failedPipeSecondTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final ConcurrentMap<Integer, PipeTaskMeta> failedPipeTaskMetaMap = new ConcurrentHashMap<>();
    failedPipeTaskMetaMap.put(REGION_ID, failedTaskMeta);
    failedPipeTaskMetaMap.put(REGION_ID + 1, failedPipeSecondTaskMeta);
    final PipeMeta failedPipeMeta =
        new PipeMeta(
            new PipeStaticMeta("failedPipe", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta(failedPipeTaskMetaMap));
    failedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);

    final PipeTaskMeta unaffectedTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final ConcurrentMap<Integer, PipeTaskMeta> unaffectedPipeTaskMetaMap =
        new ConcurrentHashMap<>();
    unaffectedPipeTaskMetaMap.put(REGION_ID, unaffectedTaskMeta);
    final PipeMeta unaffectedPipeMeta =
        new PipeMeta(
            new PipeStaticMeta(
                "unaffectedPipe", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta(unaffectedPipeTaskMetaMap));
    unaffectedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);

    pipeMetaKeeper.addPipeMeta(failedPipeMeta);
    pipeMetaKeeper.addPipeMeta(unaffectedPipeMeta);

    final PipeRuntimeSinkCriticalException exception =
        new PipeRuntimeSinkCriticalException("sink failure", 1L);
    taskAgent.stopAllPipesWithCriticalExceptionAndTrackException(failedTaskMeta, exception);

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () ->
                PipeStatus.STOPPED.equals(failedPipeMeta.getRuntimeMeta().getStatus().get())
                    && failedPipeSecondTaskMeta.containsExceptionMessage(exception));

    Assert.assertEquals(PipeStatus.RUNNING, unaffectedPipeMeta.getRuntimeMeta().getStatus().get());
    Assert.assertFalse(unaffectedTaskMeta.hasExceptionMessages());
  }

  @Test
  public void testExplicitPipeIdentityDoesNotFallBackToTaskMeta() throws Exception {
    final PipeDataNodeTaskAgent taskAgent = new PipeDataNodeTaskAgent();
    final PipeMetaKeeper pipeMetaKeeper = getField(taskAgent, "pipeMetaKeeper");
    final int localNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

    final PipeTaskMeta taskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final ConcurrentMap<Integer, PipeTaskMeta> taskMetaMap = new ConcurrentHashMap<>();
    taskMetaMap.put(REGION_ID, taskMeta);
    final PipeMeta pipeMeta =
        new PipeMeta(
            new PipeStaticMeta(
                "existingPipe", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta(taskMetaMap));
    pipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    pipeMetaKeeper.addPipeMeta(pipeMeta);

    taskAgent.stopAllPipesWithCriticalExceptionAndTrackException(
        "existingPipe",
        Long.MIN_VALUE,
        taskMeta,
        new PipeRuntimeCriticalException("stale pipe identity", 1L));

    Awaitility.await()
        .during(500, TimeUnit.MILLISECONDS)
        .atMost(2, TimeUnit.SECONDS)
        .until(
            () ->
                PipeStatus.RUNNING.equals(pipeMeta.getRuntimeMeta().getStatus().get())
                    && !taskMeta.hasExceptionMessages());
  }

  @Test
  public void testDetachedTaskMetaMustIdentifyOnePipeUniquely() throws Exception {
    final PipeDataNodeTaskAgent taskAgent = new PipeDataNodeTaskAgent();
    final PipeMetaKeeper pipeMetaKeeper = getField(taskAgent, "pipeMetaKeeper");
    final int localNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

    final PipeTaskMeta firstTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final PipeTaskMeta secondTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final ConcurrentMap<Integer, PipeTaskMeta> firstTaskMetaMap = new ConcurrentHashMap<>();
    firstTaskMetaMap.put(REGION_ID, firstTaskMeta);
    final ConcurrentMap<Integer, PipeTaskMeta> secondTaskMetaMap = new ConcurrentHashMap<>();
    secondTaskMetaMap.put(REGION_ID, secondTaskMeta);
    final PipeMeta firstPipeMeta =
        new PipeMeta(
            new PipeStaticMeta("firstPipe", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta(firstTaskMetaMap));
    final PipeMeta secondPipeMeta =
        new PipeMeta(
            new PipeStaticMeta("secondPipe", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta(secondTaskMetaMap));
    firstPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    secondPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    pipeMetaKeeper.addPipeMeta(firstPipeMeta);
    pipeMetaKeeper.addPipeMeta(secondPipeMeta);

    final PipeTaskMeta detachedTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    taskAgent.stopAllPipesWithCriticalExceptionAndTrackException(
        detachedTaskMeta, new PipeRuntimeCriticalException("ambiguous task meta", 1L));

    Awaitility.await().atMost(2, TimeUnit.SECONDS).until(detachedTaskMeta::hasExceptionMessages);
    pipeMetaKeeper.acquireWriteLock();
    pipeMetaKeeper.releaseWriteLock();

    Assert.assertEquals(PipeStatus.RUNNING, firstPipeMeta.getRuntimeMeta().getStatus().get());
    Assert.assertEquals(PipeStatus.RUNNING, secondPipeMeta.getRuntimeMeta().getStatus().get());
    Assert.assertFalse(firstTaskMeta.hasExceptionMessages());
    Assert.assertFalse(secondTaskMeta.hasExceptionMessages());
  }

  @Test
  public void testDetachedTaskMetaIsRecordedOnIdentifiedPipe() throws Exception {
    final PipeDataNodeTaskAgent taskAgent = new PipeDataNodeTaskAgent();
    final PipeMetaKeeper pipeMetaKeeper = getField(taskAgent, "pipeMetaKeeper");
    final PipeTaskManager pipeTaskManager = getField(taskAgent, "pipeTaskManager");
    final int localNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

    final PipeTaskMeta localTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final ConcurrentMap<Integer, PipeTaskMeta> taskMetaMap = new ConcurrentHashMap<>();
    taskMetaMap.put(REGION_ID, localTaskMeta);
    final PipeMeta pipeMeta =
        new PipeMeta(
            new PipeStaticMeta(
                "detachedPipe", 2L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta(taskMetaMap));
    pipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    pipeMetaKeeper.addPipeMeta(pipeMeta);
    pipeTaskManager.addPipeTasks(pipeMeta.getStaticMeta(), Collections.emptyMap());

    final PipeTaskMeta detachedTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, localNodeId);
    final PipeRuntimeCriticalException exception =
        new PipeRuntimeCriticalException("detached failure", 2L);
    taskAgent.stopAllPipesWithCriticalExceptionAndTrackException(
        "detachedPipe", 2L, detachedTaskMeta, exception);

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () ->
                PipeStatus.STOPPED.equals(pipeMeta.getRuntimeMeta().getStatus().get())
                    && localTaskMeta.containsExceptionMessage(exception));
    Assert.assertTrue(pipeMeta.getRuntimeMeta().getIsStoppedByRuntimeException());
  }

  @Test
  public void testCriticalExceptionWithNullTaskMetaStopsIdentifiedPipe() throws Exception {
    final PipeDataNodeTaskAgent taskAgent = new PipeDataNodeTaskAgent();
    final PipeMetaKeeper pipeMetaKeeper = getField(taskAgent, "pipeMetaKeeper");

    final PipeMeta failedPipeMeta =
        new PipeMeta(
            new PipeStaticMeta(
                "nullTaskMetaPipe", 3L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta());
    failedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    final PipeMeta unaffectedPipeMeta =
        new PipeMeta(
            new PipeStaticMeta(
                "stillRunningPipe", 3L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta());
    unaffectedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    pipeMetaKeeper.addPipeMeta(failedPipeMeta);
    pipeMetaKeeper.addPipeMeta(unaffectedPipeMeta);

    final PipeRuntimeSinkCriticalException exception =
        new PipeRuntimeSinkCriticalException("null task meta failure", 3L);
    taskAgent.stopAllPipesWithCriticalExceptionAndTrackException(
        "nullTaskMetaPipe", 3L, null, exception);

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () ->
                PipeStatus.STOPPED.equals(failedPipeMeta.getRuntimeMeta().getStatus().get())
                    && failedPipeMeta.getRuntimeMeta().getIsStoppedByRuntimeException());
    Assert.assertEquals(PipeStatus.RUNNING, unaffectedPipeMeta.getRuntimeMeta().getStatus().get());
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

  @SuppressWarnings("unchecked")
  private <T> T getField(final PipeDataNodeTaskAgent taskAgent, final String fieldName)
      throws ReflectiveOperationException {
    final Field field = PipeTaskAgent.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(taskAgent);
  }
}
