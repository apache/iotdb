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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class PipeTaskAgentTest {

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
      blockingCounterMap.pauseNextUpdate();
      increaseThread.start();
      Assert.assertTrue(blockingCounterMap.updatePaused.await(10, TimeUnit.SECONDS));
      dropThread.start();
      Assert.assertTrue(waitForDrop(agent, "concurrent-pipe", 1L));
    } finally {
      blockingCounterMap.resumeUpdate.countDown();
      increaseThread.join(TimeUnit.SECONDS.toMillis(10));
      dropThread.join(TimeUnit.SECONDS.toMillis(10));
      counterMapField.set(agent, originalCounterMap);
    }

    Assert.assertFalse(increaseThread.isAlive());
    Assert.assertFalse(dropThread.isAlive());
    Assert.assertNull(backgroundFailure.get());
    Assert.assertEquals(100L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("concurrent-pipe"));

    agent.decreaseFloatingMemoryUsageInByte("concurrent-pipe", 1L, 100L);
    Assert.assertEquals(0L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(0L, agent.getFloatingMemoryUsageInByte("concurrent-pipe"));
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

  private static class DummyPipeTaskAgent extends PipeTaskAgent {

    private boolean createPipeForTest(final PipeMeta pipeMeta) throws IllegalPathException {
      return createPipe(pipeMeta);
    }

    private boolean dropPipeForTest(final String pipeName, final long creationTime) {
      return dropPipe(pipeName, creationTime);
    }

    private PipeMeta getPipeMetaForTest(final String pipeName, final long creationTime) {
      return pipeMetaKeeper.getPipeMeta(pipeName, creationTime);
    }

    @Override
    protected boolean isShutdown() {
      return false;
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
      // Do nothing
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
