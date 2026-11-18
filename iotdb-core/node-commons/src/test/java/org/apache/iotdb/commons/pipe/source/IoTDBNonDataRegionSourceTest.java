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

package org.apache.iotdb.commons.pipe.source;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

public class IoTDBNonDataRegionSourceTest {

  @Test
  public void testLogicalBackupParsesMetadataInsteadOfTransferringSnapshotFiles() throws Exception {
    final AbstractPipeListeningQueue queue = Mockito.mock(AbstractPipeListeningQueue.class);
    final TestNonDataRegionSource source =
        new TestNonDataRegionSource(queue, new PipeTaskMeta(new MetaProgressIndex(10L), 0));
    final java.lang.reflect.Field finite =
        IoTDBNonDataRegionSource.class.getDeclaredField("finiteSnapshot");
    finite.setAccessible(true);
    finite.set(source, true);
    final java.lang.reflect.Field snapshots =
        IoTDBNonDataRegionSource.class.getDeclaredField("historicalEvents");
    snapshots.setAccessible(true);
    snapshots.set(
        source,
        new LinkedList<>(
            Collections.singletonList(
                Mockito.mock(PipeSnapshotEvent.class, Mockito.RETURNS_SELF))));
    source.skipSnapshotPrivilegeCheck = true;
    source.hasBeenStarted.set(true);
    Assert.assertTrue(source.supply() instanceof ProgressReportEvent);
    Assert.assertTrue(source.snapshotParsed);
    Mockito.verifyZeroInteractions(queue);
  }

  @Test
  public void testFiniteSnapshotReportsOrderedCompletionAndNeverReadsRealtime() throws Exception {
    final AbstractPipeListeningQueue queue = Mockito.mock(AbstractPipeListeningQueue.class);
    final PipeTaskMeta task = new PipeTaskMeta(new MetaProgressIndex(10L), 0);
    final TestNonDataRegionSource source = new TestNonDataRegionSource(queue, task);
    final java.lang.reflect.Field finite =
        IoTDBNonDataRegionSource.class.getDeclaredField("finiteSnapshot");
    finite.setAccessible(true);
    finite.set(source, true);
    source.hasBeenStarted.set(true);
    final EnrichedEvent completion = source.supply();
    Assert.assertTrue(completion instanceof ProgressReportEvent);
    Assert.assertEquals(
        new MetaProgressIndex(IoTDBNonDataRegionSource.LOGICAL_BACKUP_COMPLETED_INDEX),
        completion.getProgressIndex());
    Assert.assertNull(source.supply());
    task.updateProgressIndex(completion.getProgressIndex());
    Assert.assertEquals(0L, source.getUnTransferredEventCount());
    Mockito.verifyZeroInteractions(queue);

    final TestNonDataRegionSource restarted = new TestNonDataRegionSource(queue, task);
    finite.set(restarted, true);
    restarted.start();
    Assert.assertNull(restarted.supply());
    Mockito.verifyZeroInteractions(queue);
  }

  @Test
  public void testStartWithStateWrappedHybridProgressIndex() throws Exception {
    final AbstractPipeListeningQueue listeningQueue =
        Mockito.mock(AbstractPipeListeningQueue.class);
    Mockito.when(listeningQueue.isGivenNextIndexValid(11L)).thenReturn(true);

    final ProgressIndex hybridProgressIndex =
        new HybridProgressIndex(new MetaProgressIndex(10L))
            .updateToMinimumEqualOrIsAfterProgressIndex(new SimpleProgressIndex(1, 2L));
    final StateProgressIndex stateProgressIndex =
        new StateProgressIndex(1L, Collections.emptyMap(), hybridProgressIndex);
    final TestNonDataRegionSource source =
        new TestNonDataRegionSource(listeningQueue, new PipeTaskMeta(stateProgressIndex, 0));

    source.start();

    Mockito.verify(listeningQueue).newIterator(11L);
  }

  @Test
  public void testGetUnTransferredEventCountWithHybridProgressIndex() {
    final AbstractPipeListeningQueue listeningQueue =
        Mockito.mock(AbstractPipeListeningQueue.class);
    Mockito.when(listeningQueue.getTailIndex()).thenReturn(20L);

    final ProgressIndex hybridProgressIndex =
        new HybridProgressIndex(new MetaProgressIndex(10L))
            .updateToMinimumEqualOrIsAfterProgressIndex(new SimpleProgressIndex(1, 2L));
    final TestNonDataRegionSource source =
        new TestNonDataRegionSource(listeningQueue, new PipeTaskMeta(hybridProgressIndex, 0));

    Assert.assertEquals(9L, source.getUnTransferredEventCount());
  }

  @Test
  public void testGetUnTransferredEventCountWithHybridProgressIndexWithoutMetaIndex() {
    final AbstractPipeListeningQueue listeningQueue =
        Mockito.mock(AbstractPipeListeningQueue.class);
    Mockito.when(listeningQueue.getSize()).thenReturn(7L);

    final TestNonDataRegionSource source =
        new TestNonDataRegionSource(
            listeningQueue,
            new PipeTaskMeta(new HybridProgressIndex(new SimpleProgressIndex(1, 2L)), 0));

    Assert.assertEquals(7L, source.getUnTransferredEventCount());
  }

  private static final class TestNonDataRegionSource extends IoTDBNonDataRegionSource {

    private final AbstractPipeListeningQueue listeningQueue;
    private boolean skipSnapshotPrivilegeCheck;
    private boolean snapshotParsed;

    private TestNonDataRegionSource(
        final AbstractPipeListeningQueue listeningQueue, final PipeTaskMeta pipeTaskMeta) {
      this.listeningQueue = listeningQueue;
      this.pipeTaskMeta = pipeTaskMeta;
    }

    @Override
    protected AbstractPipeListeningQueue getListeningQueue() {
      return listeningQueue;
    }

    @Override
    protected boolean needTransferSnapshot() {
      return false;
    }

    @Override
    protected void triggerSnapshot() {
      // Do nothing
    }

    @Override
    protected long getMaxBlockingTimeMs() {
      return 0L;
    }

    @Override
    protected boolean canSkipSnapshotPrivilegeCheck(final PipeSnapshotEvent event) {
      return skipSnapshotPrivilegeCheck;
    }

    @Override
    protected void initSnapshotGenerator(final PipeSnapshotEvent event)
        throws IOException, IllegalPathException {
      snapshotParsed = true;
    }

    @Override
    protected boolean hasNextEventInCurrentSnapshot() {
      return false;
    }

    @Override
    protected PipeWritePlanEvent getNextEventInCurrentSnapshot() {
      return null;
    }

    @Override
    protected Optional<PipeWritePlanEvent> trimRealtimeEventByPrivilege(
        final PipeWritePlanEvent event) throws AccessDeniedException {
      return Optional.of(event);
    }

    @Override
    protected Optional<PipeWritePlanEvent> trimRealtimeEventByPipePattern(
        final PipeWritePlanEvent event) {
      return Optional.of(event);
    }

    @Override
    protected boolean isTypeListened(final PipeWritePlanEvent event) {
      return true;
    }

    @Override
    protected void confineHistoricalEventTransferTypes(final PipeSnapshotEvent event) {
      // Do nothing
    }

    @Override
    protected void login(final @Nonnull String password) {
      // Do nothing
    }
  }
}
