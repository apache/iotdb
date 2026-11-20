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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PipeDataNodeSinglePipeMetricsLifecycleTest {

  @Test
  public void testLateEventsDoNotRecreateMetricsAfterDrop() throws Exception {
    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    final String pipeName = "pipe_metrics_lifecycle_" + System.nanoTime();
    final long creationTime = System.nanoTime();
    final String pipeID = pipeName + "_" + creationTime;
    final PipeMetaKeeper pipeMetaKeeper = getPipeMetaKeeper();
    final PipeMeta pipeMeta = createPipeMeta(pipeName, creationTime);

    try {
      pipeMetaKeeper.addPipeMeta(pipeName, pipeMeta);

      metrics.increaseTsFileEventCount(pipeName, creationTime - 1);
      Assert.assertFalse(
          metrics.remainingEventAndTimeOperatorMap.containsKey(
              pipeName + "_" + (creationTime - 1)));

      metrics.increaseTsFileEventCount(pipeName, creationTime);
      Assert.assertTrue(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeID));

      pipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);
      metrics.deregister(pipeID);

      metrics.increaseInsertNodeEventCount(pipeName, creationTime);
      metrics.decreaseInsertNodeEventCount(pipeName, creationTime, 10);
      metrics.updateInsertNodeTransferTimer(pipeName, creationTime, 10);
      metrics.increaseRawTabletEventCount(pipeName, creationTime);
      metrics.decreaseRawTabletEventCount(pipeName, creationTime);
      metrics.increaseTsFileEventCount(pipeName, creationTime);
      metrics.decreaseTsFileEventCount(pipeName, creationTime, 10);
      metrics.updateTsFileTransferTimer(pipeName, creationTime, 10);
      metrics.increaseHeartbeatEventCount(pipeName, creationTime);
      metrics.decreaseHeartbeatEventCount(pipeName, creationTime);

      Assert.assertFalse(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeID));
      Assert.assertEquals(
          0L, (long) metrics.getRemainingEventAndTime(pipeName, creationTime).getLeft());
    } finally {
      if (metrics.remainingEventAndTimeOperatorMap.containsKey(pipeID)) {
        metrics.deregister(pipeID);
      }
      pipeMetaKeeper.removePipeMeta(pipeName);
    }
  }

  @Test
  public void testMetricCreationSerializesWithDeregister() throws Exception {
    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    final String pipeName = "pipe_metrics_drop_race_" + System.nanoTime();
    final long creationTime = System.nanoTime();
    final String pipeID = pipeName + "_" + creationTime;
    final PipeMetaKeeper pipeMetaKeeper = getPipeMetaKeeper();
    final PipeMeta pipeMeta = createPipeMeta(pipeName, creationTime);
    final Field pipeMetaMapField = PipeMetaKeeper.class.getDeclaredField("pipeNameToPipeMetaMap");
    pipeMetaMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, PipeMeta> originalPipeMetaMap =
        (Map<String, PipeMeta>) pipeMetaMapField.get(pipeMetaKeeper);
    final BlockingPipeMetaMap blockingPipeMetaMap =
        new BlockingPipeMetaMap(originalPipeMetaMap, pipeName, creationTime);
    pipeMetaMapField.set(pipeMetaKeeper, blockingPipeMetaMap);

    final CountDownLatch metadataRemoved = new CountDownLatch(1);
    final AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
    final Thread updateThread =
        new Thread(
            () -> {
              try {
                metrics.increaseTsFileEventCount(pipeName, creationTime);
              } catch (final Throwable failure) {
                backgroundFailure.compareAndSet(null, failure);
              }
            });
    final Thread deregisterThread =
        new Thread(
            () -> {
              try {
                pipeMetaKeeper.removePipeMeta(pipeName);
                metadataRemoved.countDown();
                metrics.deregister(pipeID);
              } catch (final Throwable failure) {
                backgroundFailure.compareAndSet(null, failure);
              }
            });

    try {
      pipeMetaKeeper.addPipeMeta(pipeName, pipeMeta);
      updateThread.start();
      Assert.assertTrue(blockingPipeMetaMap.lookupPaused.await(10, TimeUnit.SECONDS));
      deregisterThread.start();
      Assert.assertTrue(metadataRemoved.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(Thread.State.BLOCKED, waitForBlockedOrTerminated(deregisterThread));
    } finally {
      blockingPipeMetaMap.resumeLookup.countDown();
      updateThread.join(TimeUnit.SECONDS.toMillis(10));
      deregisterThread.join(TimeUnit.SECONDS.toMillis(10));
      pipeMetaKeeper.removePipeMeta(pipeName);
      pipeMetaMapField.set(pipeMetaKeeper, originalPipeMetaMap);
      if (metrics.remainingEventAndTimeOperatorMap.containsKey(pipeID)) {
        metrics.deregister(pipeID);
      }
    }

    Assert.assertFalse(updateThread.isAlive());
    Assert.assertFalse(deregisterThread.isAlive());
    Assert.assertNull(backgroundFailure.get());
    Assert.assertFalse(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeID));
  }

  private static PipeMeta createPipeMeta(final String pipeName, final long creationTime) {
    return new PipeMeta(
        new PipeStaticMeta(
            pipeName, creationTime, new HashMap<>(), new HashMap<>(), new HashMap<>()),
        new PipeRuntimeMeta(new ConcurrentHashMap<>()));
  }

  private static PipeMetaKeeper getPipeMetaKeeper() throws Exception {
    final Field pipeMetaKeeperField = PipeTaskAgent.class.getDeclaredField("pipeMetaKeeper");
    pipeMetaKeeperField.setAccessible(true);
    return (PipeMetaKeeper) pipeMetaKeeperField.get(PipeDataNodeAgent.task());
  }

  private static Thread.State waitForBlockedOrTerminated(final Thread thread)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    Thread.State state = thread.getState();
    while (state != Thread.State.BLOCKED
        && state != Thread.State.TERMINATED
        && System.nanoTime() < deadline) {
      Thread.sleep(1);
      state = thread.getState();
    }
    return state;
  }

  private static class BlockingPipeMetaMap extends ConcurrentHashMap<String, PipeMeta> {

    private static final long serialVersionUID = 1L;

    private final String pipeName;
    private final long creationTime;
    private final AtomicBoolean pauseNextLookup = new AtomicBoolean(true);
    private final CountDownLatch lookupPaused = new CountDownLatch(1);
    private final CountDownLatch resumeLookup = new CountDownLatch(1);

    private BlockingPipeMetaMap(
        final Map<String, PipeMeta> originalMap, final String pipeName, final long creationTime) {
      putAll(originalMap);
      this.pipeName = pipeName;
      this.creationTime = creationTime;
    }

    @Override
    public PipeMeta get(final Object key) {
      final PipeMeta pipeMeta = super.get(key);
      if (pipeMeta == null
          || !pipeName.equals(pipeMeta.getStaticMeta().getPipeName())
          || creationTime != pipeMeta.getStaticMeta().getCreationTime()
          || !pauseNextLookup.compareAndSet(true, false)) {
        return pipeMeta;
      }
      lookupPaused.countDown();
      try {
        if (!resumeLookup.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Timed out waiting to resume pipe metadata lookup");
        }
      } catch (final InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(failure);
      }
      return pipeMeta;
    }
  }
}
