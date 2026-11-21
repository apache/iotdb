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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.consensus.statemachine.schemaregion.SchemaRegionStateMachine;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionListenerMetrics;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.db.pipe.source.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeSchemaRegionListenerManagerTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testLeaderStateDoesNotCreateListener() {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(1);

    manager.notifyLeaderReady(schemaRegionId);

    Assert.assertTrue(manager.isLeaderReady(schemaRegionId));
    Assert.assertTrue(manager.regionIds().isEmpty());
    Assert.assertNull(manager.listenerIfPresent(schemaRegionId));
  }

  @Test
  public void testCleanupUnusedClosedListener() {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(2);

    final SchemaRegionListeningQueue listeningQueue = manager.listener(schemaRegionId);
    Assert.assertEquals(1, manager.increaseAndGetReferenceCount(schemaRegionId));

    listeningQueue.open();
    Assert.assertEquals(0, manager.decreaseAndGetReferenceCount(schemaRegionId));
    Assert.assertNotNull(manager.listenerIfPresent(schemaRegionId));

    listeningQueue.close();
    manager.cleanupListenerIfUnused(schemaRegionId);

    Assert.assertNull(manager.listenerIfPresent(schemaRegionId));
    Assert.assertTrue(manager.regionIds().isEmpty());
  }

  @Test
  public void testAutoCleanupAfterLastReferenceReleased() {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(3);

    manager.increaseAndGetReferenceCount(schemaRegionId);

    Assert.assertEquals(0, manager.decreaseAndGetReferenceCount(schemaRegionId));
    Assert.assertNull(manager.listenerIfPresent(schemaRegionId));
    Assert.assertTrue(manager.regionIds().isEmpty());
  }

  @Test
  public void testMetricsDoNotRecreateListenerAfterRegionCleanup() throws Exception {
    final SchemaRegionId schemaRegionId = new SchemaRegionId(Integer.MAX_VALUE - 10);
    final TestSchemaRegionSource source = new TestSchemaRegionSource();
    final Field schemaRegionIdField =
        IoTDBSchemaRegionSource.class.getDeclaredField("schemaRegionId");
    schemaRegionIdField.setAccessible(true);
    schemaRegionIdField.set(source, schemaRegionId);

    try {
      final SchemaRegionListeningQueue listener =
          PipeDataNodeAgent.runtime().schemaListener(schemaRegionId);
      listener.open();
      listener.close();
      PipeDataNodeAgent.runtime().clearSchemaRegionState(schemaRegionId);

      Assert.assertEquals(0L, source.getUnTransferredEventCount());
      Assert.assertNull(PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));

      source.close();
      Assert.assertEquals(0L, source.getUnTransferredEventCount());
      Assert.assertNull(PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));
    } finally {
      final SchemaRegionListeningQueue listener =
          PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId);
      if (listener != null) {
        listener.close();
      }
      PipeDataNodeAgent.runtime().clearSchemaRegionState(schemaRegionId);
    }
  }

  @Test
  public void testCreateListenerForSnapshotLoadOnlyWhenSnapshotExists() throws IOException {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(4);
    final File snapshotDir = temporaryFolder.newFolder();

    Assert.assertNull(manager.listenerForSnapshotLoad(schemaRegionId, snapshotDir));
    Assert.assertTrue(manager.regionIds().isEmpty());

    final SchemaRegionListeningQueue originalListener = manager.listener(schemaRegionId);
    originalListener.open();
    Assert.assertTrue(originalListener.createSnapshot(snapshotDir));
    originalListener.close();
    manager.clearSchemaRegionState(schemaRegionId);

    final SchemaRegionListeningQueue restoredListener =
        manager.listenerForSnapshotLoad(schemaRegionId, snapshotDir);
    Assert.assertNotNull(restoredListener);
    restoredListener.loadSnapshot(snapshotDir);
    Assert.assertTrue(restoredListener.isOpened());

    restoredListener.close();
    manager.clearSchemaRegionState(schemaRegionId);
  }

  @Test
  public void testLoadClosedListenerSnapshotCleansUnusedListener() throws IOException {
    final SchemaRegionId schemaRegionId = new SchemaRegionId(Integer.MAX_VALUE - 13);
    final File snapshotDir = temporaryFolder.newFolder();
    final ISchemaRegion schemaRegion = mock(ISchemaRegion.class);
    when(schemaRegion.getSchemaRegionId()).thenReturn(schemaRegionId);
    when(schemaRegion.loadSnapshot(snapshotDir)).thenReturn(true);
    final SchemaRegionStateMachine stateMachine = new SchemaRegionStateMachine(schemaRegion);
    final SchemaRegionListeningQueue originalListener =
        PipeDataNodeAgent.runtime().schemaListener(schemaRegionId);
    originalListener.close();
    Assert.assertTrue(originalListener.createSnapshot(snapshotDir));
    PipeDataNodeAgent.runtime().clearSchemaRegionState(schemaRegionId);

    try {
      Assert.assertTrue(stateMachine.loadSnapshot(snapshotDir));
      Assert.assertNull(PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));
    } finally {
      PipeDataNodeAgent.runtime().clearSchemaRegionState(schemaRegionId);
    }
  }

  @Test
  public void testSnapshotWithoutListenerClearsStaleFollowerQueue() throws Exception {
    assertSnapshotWithoutListenerClearsStaleQueue(false);
  }

  @Test
  public void testSnapshotWithoutListenerRetainsReferencedClosedQueue() throws Exception {
    assertSnapshotWithoutListenerClearsStaleQueue(true);
  }

  private void assertSnapshotWithoutListenerClearsStaleQueue(final boolean hasLocalReference)
      throws Exception {
    final SchemaRegionId schemaRegionId =
        new SchemaRegionId(Integer.MAX_VALUE - (hasLocalReference ? 12 : 11));
    final File snapshotDir = temporaryFolder.newFolder();
    final ISchemaRegion schemaRegion = mock(ISchemaRegion.class);
    when(schemaRegion.getSchemaRegionId()).thenReturn(schemaRegionId);
    when(schemaRegion.createSnapshot(snapshotDir)).thenReturn(true);
    when(schemaRegion.loadSnapshot(snapshotDir)).thenReturn(true);
    final SchemaRegionStateMachine stateMachine = new SchemaRegionStateMachine(schemaRegion);
    final Field queueMapField =
        PipeSchemaRegionListenerMetrics.class.getDeclaredField("listeningQueueMap");
    queueMapField.setAccessible(true);
    final Map<?, ?> metricQueues =
        (Map<?, ?>) queueMapField.get(PipeSchemaRegionListenerMetrics.getInstance());

    try {
      // The leader has applied queue-close and cleaned its listener before taking this snapshot.
      Assert.assertTrue(stateMachine.takeSnapshot(snapshotDir));
      Assert.assertFalse(SchemaRegionListeningQueue.hasSnapshot(snapshotDir));
      Assert.assertNull(PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));

      // A lagging follower can still have the queue from before the close log entry.
      final SchemaRegionListeningQueue staleQueue =
          PipeDataNodeAgent.runtime().schemaListener(schemaRegionId);
      if (hasLocalReference) {
        PipeDataNodeAgent.runtime().increaseAndGetSchemaListenerReferenceCount(schemaRegionId);
      }
      staleQueue.open();
      final PlanNode node = mock(PlanNode.class);
      when(node.getType()).thenReturn(PlanNodeType.CREATE_TIME_SERIES);
      staleQueue.tryListenToNode(node);
      Assert.assertEquals(1L, staleQueue.getSize());

      Assert.assertTrue(stateMachine.loadSnapshot(snapshotDir));

      Assert.assertFalse(staleQueue.isOpened());
      Assert.assertEquals(0L, staleQueue.getSize());
      if (hasLocalReference) {
        Assert.assertSame(
            staleQueue, PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));
        Assert.assertTrue(metricQueues.containsKey(schemaRegionId.getId()));
        Assert.assertEquals(
            0,
            PipeDataNodeAgent.runtime().decreaseAndGetSchemaListenerReferenceCount(schemaRegionId));
      }
      Assert.assertNull(PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));
      Assert.assertFalse(metricQueues.containsKey(schemaRegionId.getId()));
    } finally {
      final SchemaRegionListeningQueue listener =
          PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId);
      if (listener != null) {
        listener.close();
      }
      PipeDataNodeAgent.runtime().clearSchemaRegionState(schemaRegionId);
    }
  }

  private static class TestSchemaRegionSource extends IoTDBSchemaRegionSource {

    private TestSchemaRegionSource() {
      pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0);
    }
  }
}
