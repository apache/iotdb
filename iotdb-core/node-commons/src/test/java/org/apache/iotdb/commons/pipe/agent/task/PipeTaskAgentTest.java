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

package org.apache.iotdb.commons.pipe.agent.task;

import org.apache.iotdb.common.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.sink.limiter.PipeEndPointRateLimiter;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSink;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class PipeTaskAgentTest {

  @Test(timeout = 20000)
  public void testSlowDropOfOnePipeDoesNotBlockAnotherPipeStop() throws Exception {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
    final long pipeNameSuffix = System.nanoTime();
    final String firstPipeName = Long.toString(pipeNameSuffix);
    final String secondPipeName = Long.toString(pipeNameSuffix + 1);
    final long firstCreationTime = 1L;
    final long secondCreationTime = 2L;
    final PipeMeta firstPipeMeta = generatePipeMeta(firstPipeName, firstCreationTime);
    final PipeMeta secondPipeMeta = generatePipeMeta(secondPipeName, secondCreationTime);
    agent.createPipeForTest(firstPipeMeta);
    agent.createPipeForTest(secondPipeMeta);
    firstPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    secondPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);

    final CountDownLatch dropStarted = new CountDownLatch(1);
    final CountDownLatch allowDropToFinish = new CountDownLatch(1);
    final CountDownLatch secondPipeStopped = new CountDownLatch(1);
    agent.addPipeTaskForTest(firstPipeMeta, new TestPipeTask(null, dropStarted, allowDropToFinish));
    agent.addPipeTaskForTest(secondPipeMeta, new TestPipeTask(secondPipeStopped, null, null));

    final PipeMeta secondPipeStoppedMeta = generatePipeMeta(secondPipeName, secondCreationTime);
    secondPipeStoppedMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<?> dropFuture = null;
    Future<?> stopFuture = null;
    try {
      dropFuture = executor.submit(() -> agent.handleDropPipe(firstPipeName));
      Assert.assertTrue(dropStarted.await(5, TimeUnit.SECONDS));

      stopFuture = executor.submit(() -> agent.handleSinglePipeMetaChanges(secondPipeStoppedMeta));
      Assert.assertNull(stopFuture.get(5, TimeUnit.SECONDS));
      Assert.assertTrue(secondPipeStopped.await(1, TimeUnit.SECONDS));
      allowDropToFinish.countDown();
      Assert.assertNull(dropFuture.get(5, TimeUnit.SECONDS));
    } finally {
      allowDropToFinish.countDown();
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      if (agent.getPipeMetaForTest(firstPipeName, firstCreationTime) != null) {
        agent.dropPipeForTest(firstPipeName, firstCreationTime);
      }
      if (agent.getPipeMetaForTest(secondPipeName, secondCreationTime) != null) {
        agent.dropPipeForTest(secondPipeName, secondCreationTime);
      }
    }
  }

  @Test(timeout = 20000)
  public void testSlowLeaderChangeDropDoesNotBlockAnotherPipeStop() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalDnConnectionTimeoutInMS = commonConfig.getDnConnectionTimeoutInMS();
    commonConfig.setDnConnectionTimeoutInMS(3000);

    final CountDownLatch dropStarted = new CountDownLatch(1);
    final CountDownLatch allowDropToFinish = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
      final String firstPipeName = "leader_change_" + System.nanoTime();
      final String secondPipeName = "leader_change_other_" + System.nanoTime();
      final long firstCreationTime = 1L;
      final long secondCreationTime = 2L;
      final PipeMeta firstPipeMeta = generatePipeMeta(firstPipeName, firstCreationTime);
      final PipeMeta secondPipeMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      agent.createPipeForTest(firstPipeMeta);
      agent.createPipeForTest(secondPipeMeta);
      firstPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      secondPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      firstPipeMeta
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
      secondPipeMeta
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
      agent.addPipeTaskForTest(
          firstPipeMeta, new TestPipeTask(null, dropStarted, allowDropToFinish));
      final CountDownLatch secondPipeStopped = new CountDownLatch(1);
      agent.addPipeTaskForTest(secondPipeMeta, new TestPipeTask(secondPipeStopped, null, null));

      final PipeMeta firstPipeMetaFromCoordinator =
          generatePipeMeta(firstPipeName, firstCreationTime);
      firstPipeMetaFromCoordinator.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      firstPipeMetaFromCoordinator
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 2));
      final PipeMeta secondPipeStoppedMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      secondPipeStoppedMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
      secondPipeStoppedMeta
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));

      final Future<?> leaderChangeFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(firstPipeMetaFromCoordinator));
      Assert.assertTrue(dropStarted.await(5, TimeUnit.SECONDS));

      final Future<?> stopFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(secondPipeStoppedMeta));
      Assert.assertNull(stopFuture.get(5, TimeUnit.SECONDS));
      Assert.assertTrue(secondPipeStopped.await(1, TimeUnit.SECONDS));

      allowDropToFinish.countDown();
      Assert.assertNull(leaderChangeFuture.get(5, TimeUnit.SECONDS));
    } finally {
      allowDropToFinish.countDown();
      executor.shutdownNow();
      try {
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        commonConfig.setDnConnectionTimeoutInMS(originalDnConnectionTimeoutInMS);
      }
    }
  }

  @Test(timeout = 20000)
  public void testSlowReceiverSessionRegistrationDoesNotBlockAnotherPipeStop() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalDnConnectionTimeoutInMS = commonConfig.getDnConnectionTimeoutInMS();
    commonConfig.setDnConnectionTimeoutInMS(3000);

    final CountDownLatch registrationStarted = new CountDownLatch(1);
    final CountDownLatch allowRegistrationToFinish = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
      final String firstPipeName = "session_register_" + System.nanoTime();
      final String secondPipeName = "session_register_other_" + System.nanoTime();
      final long firstCreationTime = 1L;
      final long secondCreationTime = 2L;
      final PipeMeta firstPipeMeta = generatePipeMeta(firstPipeName, firstCreationTime);
      final PipeMeta secondPipeMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      agent.createPipeForTest(firstPipeMeta);
      agent.createPipeForTest(secondPipeMeta);
      secondPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      agent.addPipeTaskForTest(
          firstPipeMeta,
          new TestPipeTask(
              null, null, null, registrationStarted, allowRegistrationToFinish, null, null));
      final CountDownLatch secondPipeStopped = new CountDownLatch(1);
      agent.addPipeTaskForTest(secondPipeMeta, new TestPipeTask(secondPipeStopped, null, null));

      final PipeMeta firstPipeRunningMeta = generatePipeMeta(firstPipeName, firstCreationTime);
      firstPipeRunningMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      final PipeMeta secondPipeStoppedMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      secondPipeStoppedMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);

      final Future<?> startFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(firstPipeRunningMeta));
      Assert.assertTrue(registrationStarted.await(5, TimeUnit.SECONDS));

      final Future<?> stopFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(secondPipeStoppedMeta));
      Assert.assertNull(stopFuture.get(5, TimeUnit.SECONDS));
      Assert.assertTrue(secondPipeStopped.await(1, TimeUnit.SECONDS));

      allowRegistrationToFinish.countDown();
      Assert.assertNull(startFuture.get(5, TimeUnit.SECONDS));
    } finally {
      allowRegistrationToFinish.countDown();
      executor.shutdownNow();
      try {
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        commonConfig.setDnConnectionTimeoutInMS(originalDnConnectionTimeoutInMS);
      }
    }
  }

  @Test(timeout = 20000)
  public void testSlowReceiverSessionDiscardDoesNotBlockAnotherPipeStop() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalDnConnectionTimeoutInMS = commonConfig.getDnConnectionTimeoutInMS();
    commonConfig.setDnConnectionTimeoutInMS(3000);

    final CountDownLatch discardStarted = new CountDownLatch(1);
    final CountDownLatch allowDiscardToFinish = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
      final String firstPipeName = "session_discard_" + System.nanoTime();
      final String secondPipeName = "session_discard_other_" + System.nanoTime();
      final long firstCreationTime = 1L;
      final long secondCreationTime = 2L;
      final PipeMeta firstPipeMeta = generatePipeMeta(firstPipeName, firstCreationTime);
      final PipeMeta secondPipeMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      agent.createPipeForTest(firstPipeMeta);
      agent.createPipeForTest(secondPipeMeta);
      firstPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      secondPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      agent.addPipeTaskForTest(
          firstPipeMeta,
          new TestPipeTask(null, null, null, null, null, discardStarted, allowDiscardToFinish));
      final CountDownLatch secondPipeStopped = new CountDownLatch(1);
      agent.addPipeTaskForTest(secondPipeMeta, new TestPipeTask(secondPipeStopped, null, null));

      final PipeMeta firstPipeStoppedMeta = generatePipeMeta(firstPipeName, firstCreationTime);
      firstPipeStoppedMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
      final PipeMeta secondPipeStoppedMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      secondPipeStoppedMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);

      final Future<?> stopFirstPipeFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(firstPipeStoppedMeta));
      Assert.assertTrue(discardStarted.await(5, TimeUnit.SECONDS));

      final Future<?> stopSecondPipeFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(secondPipeStoppedMeta));
      Assert.assertNull(stopSecondPipeFuture.get(5, TimeUnit.SECONDS));
      Assert.assertTrue(secondPipeStopped.await(1, TimeUnit.SECONDS));

      allowDiscardToFinish.countDown();
      Assert.assertNull(stopFirstPipeFuture.get(5, TimeUnit.SECONDS));
    } finally {
      allowDiscardToFinish.countDown();
      executor.shutdownNow();
      try {
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        commonConfig.setDnConnectionTimeoutInMS(originalDnConnectionTimeoutInMS);
      }
    }
  }

  @Test(timeout = 20000)
  public void testSlowRegionTaskStartDoesNotBlockAnotherPipeStop() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalDnConnectionTimeoutInMS = commonConfig.getDnConnectionTimeoutInMS();
    commonConfig.setDnConnectionTimeoutInMS(3000);

    final CountDownLatch startStarted = new CountDownLatch(1);
    final CountDownLatch allowStartToFinish = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
      final String firstPipeName = "task_start_" + System.nanoTime();
      final String secondPipeName = "task_start_other_" + System.nanoTime();
      final long firstCreationTime = 1L;
      final long secondCreationTime = 2L;
      final PipeMeta firstPipeMeta = generatePipeMeta(firstPipeName, firstCreationTime);
      final PipeMeta secondPipeMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      agent.createPipeForTest(firstPipeMeta);
      agent.createPipeForTest(secondPipeMeta);
      firstPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      secondPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      agent.setPipeTaskToCreateForTest(
          new TestPipeTask(
              null, null, null, null, null, null, null, startStarted, allowStartToFinish));
      final CountDownLatch secondPipeStopped = new CountDownLatch(1);
      agent.addPipeTaskForTest(secondPipeMeta, new TestPipeTask(secondPipeStopped, null, null));
      secondPipeMeta
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));

      final PipeMeta firstPipeMetaFromCoordinator =
          generatePipeMeta(firstPipeName, firstCreationTime);
      firstPipeMetaFromCoordinator.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      firstPipeMetaFromCoordinator
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 2));
      final PipeMeta secondPipeStoppedMeta = generatePipeMeta(secondPipeName, secondCreationTime);
      secondPipeStoppedMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
      secondPipeStoppedMeta
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));

      final Future<?> taskStartFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(firstPipeMetaFromCoordinator));
      Assert.assertTrue(startStarted.await(5, TimeUnit.SECONDS));

      final Future<?> stopFuture =
          executor.submit(() -> agent.handleSinglePipeMetaChanges(secondPipeStoppedMeta));
      Assert.assertNull(stopFuture.get(5, TimeUnit.SECONDS));
      Assert.assertTrue(secondPipeStopped.await(1, TimeUnit.SECONDS));

      allowStartToFinish.countDown();
      Assert.assertNull(taskStartFuture.get(5, TimeUnit.SECONDS));
    } finally {
      allowStartToFinish.countDown();
      executor.shutdownNow();
      try {
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        commonConfig.setDnConnectionTimeoutInMS(originalDnConnectionTimeoutInMS);
      }
    }
  }

  @Test
  public void testPipeLifecycleTimeoutKeepsSubmillisecondPrecision() {
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toNanos(1) * 2 / 3,
        PipeTaskAgent.calculatePipeLifecycleTimeoutInNanos(1));
    Assert.assertEquals(0L, PipeTaskAgent.calculatePipeLifecycleTimeoutInNanos(0));
  }

  @Test(timeout = 10000)
  public void testPipeLifecycleAndWriteLockWaitShareOneTimeout() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalDnConnectionTimeoutInMS = commonConfig.getDnConnectionTimeoutInMS();
    commonConfig.setDnConnectionTimeoutInMS(3000);

    final String pipeName = Long.toString(System.nanoTime());
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
    AutoCloseable lifecycleLock = acquirePipeLifecycleLockForTest(agent, pipeName);
    final long operationTimeoutInMs = commonConfig.getDnConnectionTimeoutInMS() * 2L / 3;
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> operation = null;
    boolean writeLockHeld = false;
    try {
      agent.acquireWriteLock();
      writeLockHeld = true;
      operation = executor.submit(() -> agent.handleDropPipe(pipeName));
      Assert.assertTrue(waitForPipeLifecycleLockWaiter(agent, pipeName));

      Thread.sleep(operationTimeoutInMs / 2 + 50);
      lifecycleLock.close();
      lifecycleLock = null;

      Assert.assertNotNull(operation.get(operationTimeoutInMs * 2 / 3, TimeUnit.MILLISECONDS));
    } finally {
      if (lifecycleLock != null) {
        lifecycleLock.close();
      }
      if (writeLockHeld) {
        agent.releaseWriteLock();
      }
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      commonConfig.setDnConnectionTimeoutInMS(originalDnConnectionTimeoutInMS);
    }
  }

  @Test(timeout = 10000)
  public void testDropAllPipeTasksDoesNotWaitPastConfiguredTimeout() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final long originalPipeMaxWaitFinishTime = commonConfig.getPipeMaxWaitFinishTime();
    final CountDownLatch dropStarted = new CountDownLatch(1);
    final CountDownLatch allowDropToFinish = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      commonConfig.setPipeMaxWaitFinishTime(200L);
      final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
      final String pipeName = Long.toString(System.nanoTime());
      final long creationTime = 1L;
      final PipeMeta pipeMeta = generatePipeMeta(pipeName, creationTime);
      agent.createPipeForTest(pipeMeta);
      pipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      agent.addPipeTaskForTest(pipeMeta, new TestPipeTask(null, dropStarted, allowDropToFinish));

      final Future<?> dropFuture = executor.submit(() -> agent.handleDropPipe(pipeName));
      Assert.assertTrue(dropStarted.await(5, TimeUnit.SECONDS));

      final Future<?> dropAllFuture = executor.submit(agent::dropAllPipeTasks);
      dropAllFuture.get(3, TimeUnit.SECONDS);
      Assert.assertEquals(1L, allowDropToFinish.getCount());
      Assert.assertNotNull(agent.getPipeMetaForTest(pipeName, creationTime));

      allowDropToFinish.countDown();
      Assert.assertNull(dropFuture.get(5, TimeUnit.SECONDS));
      Assert.assertNull(agent.getPipeMetaForTest(pipeName, creationTime));
    } finally {
      allowDropToFinish.countDown();
      executor.shutdownNow();
      try {
        Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      } finally {
        commonConfig.setPipeMaxWaitFinishTime(originalPipeMaxWaitFinishTime);
      }
    }
  }

  @Test
  public void testFloatingMemoryUsageSurvivesDropUntilLateRelease() throws IllegalPathException {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();

    agent.createPipeForTest(generatePipeMeta("pipe", 1L));
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 100L);
    Assert.assertEquals(100L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe"));
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe", 1L));

    agent.dropPipeForTest("pipe", 1L);
    Assert.assertEquals(100L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe"));
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe", 1L));

    agent.createPipeForTest(generatePipeMeta("pipe", 2L));
    agent.addFloatingMemoryUsageInByte("pipe", 2L, 20L);
    Assert.assertEquals(120L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(120L, agent.getFloatingMemoryUsageInByte("pipe"));
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe", 1L));
    Assert.assertEquals(20L, agent.getFloatingMemoryUsageInByte("pipe", 2L));

    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 100L);
    Assert.assertEquals(20L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(20L, agent.getFloatingMemoryUsageInByte("pipe"));
    Assert.assertEquals(0L, agent.getFloatingMemoryUsageInByte("pipe", 1L));
    Assert.assertEquals(20L, agent.getFloatingMemoryUsageInByte("pipe", 2L));

    agent.dropPipeForTest("pipe", 2L);
    agent.decreaseFloatingMemoryUsageInByte("pipe", 2L, 20L);
    Assert.assertEquals(0L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(0L, agent.getFloatingMemoryUsageInByte("pipe"));
  }

  @Test
  public void testZeroFloatingMemoryUsageCounterIsCleanedAfterDrop() throws Exception {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();

    agent.createPipeForTest(generatePipeMeta("pipe", 1L));
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 100L);
    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 100L);

    Assert.assertTrue(
        getMapField(agent, "pipeNameWithCreationTime2FloatingMemoryUsageInByteMap").isEmpty());
    Assert.assertTrue(getMapField(agent, "pipeName2CreationTimeSetMap").isEmpty());

    agent.dropPipeForTest("pipe", 1L);

    Assert.assertTrue(
        getMapField(agent, "pipeNameWithCreationTime2FloatingMemoryUsageInByteMap").isEmpty());
    Assert.assertTrue(getMapField(agent, "pipeName2CreationTimeSetMap").isEmpty());
  }

  @Test
  public void testLateFloatingMemoryAcquisitionDoesNotConsumeOutstandingUsage() throws Exception {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
    agent.createPipeForTest(generatePipeMeta("pipe", 1L));
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 100L);
    agent.dropPipeForTest("pipe", 1L);

    // An assigner can retain a matched source across drop while other events still own memory.
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 20L);
    Assert.assertEquals(120L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(120L, agent.getFloatingMemoryUsageInByte("pipe"));
    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 20L);
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe", 1L));
    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 100L);

    Assert.assertEquals(0L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertTrue(
        getMapField(agent, "pipeNameWithCreationTime2FloatingMemoryUsageInByteMap").isEmpty());
    Assert.assertTrue(getMapField(agent, "pipeName2CreationTimeSetMap").isEmpty());
  }

  @Test
  public void testLateFloatingMemoryAcquisitionAfterCounterCleanup() throws Exception {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
    agent.createPipeForTest(generatePipeMeta("pipe", 1L));
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 100L);
    agent.dropPipeForTest("pipe", 1L);
    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 100L);

    // Late acquisitions must also be tracked after the previous events released the last byte.
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 20L);
    Assert.assertEquals(20L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(20L, agent.getFloatingMemoryUsageInByte("pipe"));
    Assert.assertEquals(20L, agent.getFloatingMemoryUsageInByte("pipe", 1L));
    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 20L);

    Assert.assertTrue(
        getMapField(agent, "pipeNameWithCreationTime2FloatingMemoryUsageInByteMap").isEmpty());
    Assert.assertTrue(getMapField(agent, "pipeName2CreationTimeSetMap").isEmpty());
  }

  @Test
  public void testFloatingMemoryIncrementIsNotLostDuringDropCleanup() throws Exception {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
    final PipeMeta pipeMeta = generatePipeMeta("concurrent-pipe", 1L);
    agent.createPipeForTest(pipeMeta);
    agent.addFloatingMemoryUsageInByte("concurrent-pipe", 1L, 100L);
    agent.decreaseFloatingMemoryUsageInByte("concurrent-pipe", 1L, 100L);

    final Field counterMapField =
        PipeTaskAgent.class.getDeclaredField(
            "pipeNameWithCreationTime2FloatingMemoryUsageInByteMap");
    counterMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, AtomicLong> originalCounterMap =
        (Map<String, AtomicLong>) counterMapField.get(agent);
    final BlockingFloatingMemoryMap blockingCounterMap =
        new BlockingFloatingMemoryMap(originalCounterMap, "concurrent-pipe_1");
    counterMapField.set(agent, blockingCounterMap);

    final AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
    final Thread increaseThread =
        new Thread(
            () ->
                runAndRecordFailure(
                    () -> agent.addFloatingMemoryUsageInByte("concurrent-pipe", 1L, 100L),
                    backgroundFailure));
    final Thread dropThread =
        new Thread(
            () ->
                runAndRecordFailure(
                    () -> agent.dropPipeForTest("concurrent-pipe", 1L), backgroundFailure));

    try {
      try {
        blockingCounterMap.pauseNextUpdate();
        increaseThread.start();
        Assert.assertTrue(blockingCounterMap.updatePaused.await(10, TimeUnit.SECONDS));
        dropThread.start();
        Assert.assertTrue(waitForDrop(agent, "concurrent-pipe", 1L));
      } finally {
        blockingCounterMap.resumeUpdate.countDown();
        increaseThread.join(TimeUnit.SECONDS.toMillis(10));
        dropThread.join(TimeUnit.SECONDS.toMillis(10));
      }

      Assert.assertFalse(increaseThread.isAlive());
      Assert.assertFalse(dropThread.isAlive());
      Assert.assertNull(backgroundFailure.get());
      Assert.assertEquals(100L, agent.getAllFloatingMemoryUsageInByte());
      Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("concurrent-pipe"));

      agent.decreaseFloatingMemoryUsageInByte("concurrent-pipe", 1L, 100L);
      Assert.assertEquals(0L, agent.getAllFloatingMemoryUsageInByte());
      Assert.assertEquals(0L, agent.getFloatingMemoryUsageInByte("concurrent-pipe"));
    } finally {
      counterMapField.set(agent, originalCounterMap);
    }
  }

  @Test
  public void testPipeDropClearsSharedRuntimeState() throws Exception {
    assertPipeDropClearsSharedRuntimeState(false);
    assertPipeDropClearsSharedRuntimeState(true);
  }

  private static void assertPipeDropClearsSharedRuntimeState(final boolean dropByName)
      throws Exception {
    final String pipeName = "runtime_state_cleanup_" + System.nanoTime();
    final long creationTime = System.nanoTime();
    final int regionId = 17;
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();
    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    final Map<Pair<String, Long>, PipeEndPointRateLimiter> rateLimiters = getRateLimiterMap();
    final Pair<String, Long> pipeKey = new Pair<>(pipeName, creationTime);

    try {
      commitManager.register(pipeName, creationTime, regionId, "test-sink");
      final CommitterKey committerKey =
          commitManager.getCommitterKey(pipeName, creationTime, regionId);
      final CommitterKey restartTimesKey = new CommitterKey(pipeName, creationTime, regionId, -1);
      rateLimiters.put(pipeKey, new PipeEndPointRateLimiter(pipeName, creationTime, 1.0));
      agent.createPipeForTest(generatePipeMeta(pipeName, creationTime));

      if (dropByName) {
        Assert.assertTrue(agent.dropPipeByNameForTest(pipeName));
      } else {
        Assert.assertTrue(agent.dropPipeForTest(pipeName, creationTime));
      }

      Assert.assertFalse(
          getCommitManagerMap(commitManager, "eventCommitterMap").containsKey(committerKey));
      Assert.assertFalse(
          getCommitManagerMap(commitManager, "eventCommitterRestartTimesMap")
              .containsKey(restartTimesKey));
      Assert.assertFalse(rateLimiters.containsKey(pipeKey));
    } finally {
      commitManager.clear(pipeName, creationTime);
      IoTDBSink.clearPipeRateLimiter(pipeName, creationTime);
      final PipeMeta pipeMeta = agent.getPipeMetaForTest(pipeName, creationTime);
      if (pipeMeta != null) {
        agent.dropPipeForTest(pipeName, creationTime);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<Pair<String, Long>, PipeEndPointRateLimiter> getRateLimiterMap()
      throws ReflectiveOperationException {
    final Field field = IoTDBSink.class.getDeclaredField("PIPE_END_POINT_RATE_LIMITER_MAP");
    field.setAccessible(true);
    return (Map<Pair<String, Long>, PipeEndPointRateLimiter>) field.get(null);
  }

  private static Map<?, ?> getCommitManagerMap(
      final PipeEventCommitManager commitManager, final String fieldName)
      throws ReflectiveOperationException {
    final Field field = PipeEventCommitManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Map<?, ?>) field.get(commitManager);
  }

  @SuppressWarnings("unchecked")
  private static Map<?, ?> getMapField(final PipeTaskAgent agent, final String fieldName)
      throws NoSuchFieldException, IllegalAccessException {
    final Field field = PipeTaskAgent.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Map<?, ?>) field.get(agent);
  }

  private static PipeMeta generatePipeMeta(final String pipeName, final long creationTime) {
    return new PipeMeta(
        new PipeStaticMeta(
            pipeName, creationTime, new HashMap<>(), new HashMap<>(), new HashMap<>()),
        new PipeRuntimeMeta(new ConcurrentHashMap<>()));
  }

  private static void runAndRecordFailure(
      final Runnable operation, final AtomicReference<Throwable> failure) {
    try {
      operation.run();
    } catch (final Throwable e) {
      failure.compareAndSet(null, e);
    }
  }

  private static boolean waitForDrop(
      final DummyPipeTaskAgent agent, final String pipeName, final long creationTime)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (agent.getPipeMetaForTest(pipeName, creationTime) == null) {
        return true;
      }
      Thread.sleep(1);
    }
    return false;
  }

  private static AutoCloseable acquirePipeLifecycleLockForTest(
      final DummyPipeTaskAgent agent, final String pipeName) throws Exception {
    final Method method =
        PipeTaskAgent.class.getDeclaredMethod(
            "tryLockPipeLifecycle", String.class, long.class, long.class);
    method.setAccessible(true);
    return (AutoCloseable)
        method.invoke(agent, pipeName, System.nanoTime(), TimeUnit.SECONDS.toNanos(5));
  }

  private static boolean waitForPipeLifecycleLockWaiter(
      final DummyPipeTaskAgent agent, final String pipeName) throws Exception {
    final Field lockReferenceMapField =
        PipeTaskAgent.class.getDeclaredField("pipeName2LifecycleLockReferenceMap");
    lockReferenceMapField.setAccessible(true);
    final long deadlineInNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadlineInNanos) {
      final Object lockReference = ((Map<?, ?>) lockReferenceMapField.get(agent)).get(pipeName);
      if (lockReference != null) {
        final Field lockField = lockReference.getClass().getDeclaredField("lock");
        lockField.setAccessible(true);
        if (((ReentrantLock) lockField.get(lockReference)).hasQueuedThreads()) {
          return true;
        }
      }
      Thread.sleep(1);
    }
    return false;
  }

  private static class DummyPipeTaskAgent extends PipeTaskAgent {

    private PipeTask pipeTaskToCreate;

    private boolean createPipeForTest(final PipeMeta pipeMeta) throws IllegalPathException {
      return createPipe(pipeMeta);
    }

    private boolean dropPipeForTest(final String pipeName, final long creationTime) {
      return dropPipe(pipeName, creationTime);
    }

    private boolean dropPipeByNameForTest(final String pipeName) {
      return dropPipe(pipeName);
    }

    private void addPipeTaskForTest(final PipeMeta pipeMeta, final PipeTask pipeTask) {
      pipeTaskManager.addPipeTask(pipeMeta.getStaticMeta(), 1, pipeTask);
    }

    private void setPipeTaskToCreateForTest(final PipeTask pipeTask) {
      pipeTaskToCreate = pipeTask;
    }

    private PipeMeta getPipeMetaForTest(final String pipeName, final long creationTime) {
      return pipeMetaKeeper.getPipeMeta(pipeName, creationTime);
    }

    @Override
    protected boolean isShutdown() {
      return false;
    }

    @Override
    protected boolean shouldReleasePipeMetaWriteLockDuringTaskLifecycle() {
      return true;
    }

    @Override
    protected void thawRate(final String pipeName, final long creationTime) {
      // Do nothing
    }

    @Override
    protected void freezeRate(final String pipeName, final long creationTime) {
      // Do nothing
    }

    @Override
    protected Map<Integer, PipeTask> buildPipeTasks(final PipeMeta pipeMetaFromCoordinator) {
      return new HashMap<>();
    }

    @Override
    protected void createPipeTask(
        final int consensusGroupId,
        final PipeStaticMeta pipeStaticMeta,
        final org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta pipeTaskMeta) {
      if (pipeTaskToCreate != null) {
        pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTaskToCreate);
        pipeMetaKeeper
            .getPipeMeta(pipeStaticMeta)
            .getRuntimeMeta()
            .getConsensusGroupId2TaskMetaMap()
            .put(consensusGroupId, pipeTaskMeta);
        pipeTaskToCreate = null;
      }
    }

    @Override
    protected void collectPipeMetaListInternal(
        final TPipeHeartbeatReq req, final TPipeHeartbeatResp resp) throws TException {
      // Do nothing
    }

    @Override
    public void runPipeTasks(
        final Collection<PipeTask> pipeTasks, final Consumer<PipeTask> runSingle) {
      pipeTasks.forEach(runSingle);
    }
  }

  private static class TestPipeTask implements PipeTask {

    private final CountDownLatch stopped;
    private final CountDownLatch dropStarted;
    private final CountDownLatch allowDropToFinish;
    private final CountDownLatch registrationStarted;
    private final CountDownLatch allowRegistrationToFinish;
    private final CountDownLatch discardStarted;
    private final CountDownLatch allowDiscardToFinish;
    private final CountDownLatch startStarted;
    private final CountDownLatch allowStartToFinish;

    private TestPipeTask(
        final CountDownLatch stopped,
        final CountDownLatch dropStarted,
        final CountDownLatch allowDropToFinish) {
      this(stopped, dropStarted, allowDropToFinish, null, null, null, null);
    }

    private TestPipeTask(
        final CountDownLatch stopped,
        final CountDownLatch dropStarted,
        final CountDownLatch allowDropToFinish,
        final CountDownLatch registrationStarted,
        final CountDownLatch allowRegistrationToFinish,
        final CountDownLatch discardStarted,
        final CountDownLatch allowDiscardToFinish) {
      this(
          stopped,
          dropStarted,
          allowDropToFinish,
          registrationStarted,
          allowRegistrationToFinish,
          discardStarted,
          allowDiscardToFinish,
          null,
          null);
    }

    private TestPipeTask(
        final CountDownLatch stopped,
        final CountDownLatch dropStarted,
        final CountDownLatch allowDropToFinish,
        final CountDownLatch registrationStarted,
        final CountDownLatch allowRegistrationToFinish,
        final CountDownLatch discardStarted,
        final CountDownLatch allowDiscardToFinish,
        final CountDownLatch startStarted,
        final CountDownLatch allowStartToFinish) {
      this.stopped = stopped;
      this.dropStarted = dropStarted;
      this.allowDropToFinish = allowDropToFinish;
      this.registrationStarted = registrationStarted;
      this.allowRegistrationToFinish = allowRegistrationToFinish;
      this.discardStarted = discardStarted;
      this.allowDiscardToFinish = allowDiscardToFinish;
      this.startStarted = startStarted;
      this.allowStartToFinish = allowStartToFinish;
    }

    @Override
    public void create() {}

    @Override
    public void drop() {
      if (dropStarted != null) {
        dropStarted.countDown();
        awaitLatch(allowDropToFinish);
      }
    }

    @Override
    public void start() {
      if (startStarted != null) {
        startStarted.countDown();
        awaitLatch(allowStartToFinish);
      }
    }

    @Override
    public void stop() {
      if (stopped != null) {
        stopped.countDown();
      }
    }

    @Override
    public void registerReceiverRuntimeSessions(final String pipeName, final long creationTime) {
      if (registrationStarted != null) {
        registrationStarted.countDown();
        awaitLatch(allowRegistrationToFinish);
      }
    }

    @Override
    public void discardReceiverRuntimeSessions(final String pipeName, final long creationTime) {
      if (discardStarted != null) {
        discardStarted.countDown();
        awaitLatch(allowDiscardToFinish);
      }
    }
  }

  private static void awaitLatch(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static class BlockingFloatingMemoryMap extends ConcurrentHashMap<String, AtomicLong> {

    private static final long serialVersionUID = 1L;

    private final String pipeID;
    private final AtomicBoolean shouldPause = new AtomicBoolean();
    private final CountDownLatch updatePaused = new CountDownLatch(1);
    private final CountDownLatch resumeUpdate = new CountDownLatch(1);

    private BlockingFloatingMemoryMap(
        final Map<String, AtomicLong> originalMap, final String pipeID) {
      putAll(originalMap);
      this.pipeID = pipeID;
    }

    private void pauseNextUpdate() {
      shouldPause.set(true);
    }

    @Override
    public AtomicLong compute(
        final String key,
        final BiFunction<? super String, ? super AtomicLong, ? extends AtomicLong>
            remappingFunction) {
      return super.compute(
          key,
          (currentKey, currentValue) -> {
            final AtomicLong updatedValue = remappingFunction.apply(currentKey, currentValue);
            pauseIfNecessary(currentKey);
            return updatedValue;
          });
    }

    @Override
    public AtomicLong computeIfAbsent(
        final String key, final Function<? super String, ? extends AtomicLong> mappingFunction) {
      final AtomicLong value = super.computeIfAbsent(key, mappingFunction);
      pauseIfNecessary(key);
      return value;
    }

    private void pauseIfNecessary(final String key) {
      if (!pipeID.equals(key) || !shouldPause.compareAndSet(true, false)) {
        return;
      }
      updatePaused.countDown();
      try {
        if (!resumeUpdate.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Timed out waiting to resume floating memory update");
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }
}
