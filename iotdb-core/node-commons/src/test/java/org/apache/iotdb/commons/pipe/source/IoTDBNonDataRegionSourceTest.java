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
import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class IoTDBNonDataRegionSourceTest {

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

  @Test
  public void testSupplySplitEventsBeforeAdvancingRealtimeProgress() throws Exception {
    final PipeWritePlanEvent original = newWritePlanEvent();
    final PipeWritePlanEvent first = newWritePlanEvent();
    final PipeWritePlanEvent second = newWritePlanEvent();
    final PipeWritePlanEvent next = newWritePlanEvent();
    final ConcurrentIterableLinkedQueue<Event> queue = new ConcurrentIterableLinkedQueue<>();
    queue.add(original);
    queue.add(next);
    final TestNonDataRegionSource source = newSource(queue);
    source.privilegeTrimmer =
        event -> event == original ? List.of(first, second) : Collections.singletonList(event);

    Assert.assertSame(first, source.supply());
    Mockito.verify(first, Mockito.never()).bindProgressIndex(Mockito.any());
    Assert.assertSame(second, source.supply());
    Mockito.verify(second).bindProgressIndex(new MetaProgressIndex(0));
    Assert.assertSame(next, source.supply());
    Mockito.verify(next).bindProgressIndex(new MetaProgressIndex(1));
    source.close();
  }

  @Test
  public void testSnapshotSplitsBindProgressOnlyToFinalPlan() throws Exception {
    final PipeWritePlanEvent original = newWritePlanEvent();
    final PipeWritePlanEvent next = newWritePlanEvent();
    final PipeWritePlanEvent first = newWritePlanEvent();
    final PipeWritePlanEvent second = newWritePlanEvent();
    final PipeWritePlanEvent third = newWritePlanEvent();
    final PipeWritePlanEvent fourth = newWritePlanEvent();
    final TestNonDataRegionSource source = newSource(new ConcurrentIterableLinkedQueue<>());
    source.snapshotPlans.add(original);
    source.snapshotPlans.add(next);
    source.privilegeTrimmer =
        event -> event == original ? List.of(first, second) : List.of(third, fourth);

    Assert.assertSame(first, source.supply());
    Assert.assertSame(second, source.supply());
    Assert.assertSame(third, source.supply());
    Assert.assertSame(fourth, source.supply());
    for (final PipeWritePlanEvent event : List.of(first, second, third)) {
      Mockito.verify(event, Mockito.never()).bindProgressIndex(Mockito.any());
    }
    Mockito.verify(fourth).bindProgressIndex(new MetaProgressIndex(-1));
    source.close();
  }

  @Test
  public void testSnapshotPrivilegeRetryDoesNotConsumeNextPlan() throws Exception {
    final PipeWritePlanEvent original = newWritePlanEvent();
    final PipeWritePlanEvent next = newWritePlanEvent();
    final TestNonDataRegionSource source = newSource(new ConcurrentIterableLinkedQueue<>());
    source.snapshotPlans.add(original);
    source.snapshotPlans.add(next);
    final AtomicBoolean firstAttempt = new AtomicBoolean(true);
    source.privilegeTrimmer =
        event -> {
          if (firstAttempt.getAndSet(false)) {
            throw new AccessDeniedException("test");
          }
          return Collections.singletonList(event);
        };

    Assert.assertThrows(AccessDeniedException.class, source::supply);
    Assert.assertSame(original, source.supply());
    Assert.assertSame(next, source.supply());
    source.close();
  }

  private static TestNonDataRegionSource newSource(
      final ConcurrentIterableLinkedQueue<Event> queue) {
    final AbstractPipeListeningQueue listeningQueue =
        Mockito.mock(AbstractPipeListeningQueue.class);
    Mockito.when(listeningQueue.isGivenNextIndexValid(0L)).thenReturn(true);
    Mockito.when(listeningQueue.newIterator(0L)).thenReturn(queue.iterateFrom(0));
    return new TestNonDataRegionSource(
        listeningQueue, new PipeTaskMeta(new MetaProgressIndex(-1), 0));
  }

  private static PipeWritePlanEvent newWritePlanEvent() {
    final PipeWritePlanEvent event = Mockito.mock(PipeWritePlanEvent.class);
    Mockito.when(
            event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenReturn(event);
    return event;
  }

  private static final class TestNonDataRegionSource extends IoTDBNonDataRegionSource {

    private final AbstractPipeListeningQueue listeningQueue;
    private final Deque<PipeWritePlanEvent> snapshotPlans = new ArrayDeque<>();
    private Function<PipeWritePlanEvent, List<PipeWritePlanEvent>> privilegeTrimmer =
        Collections::singletonList;

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
      return false;
    }

    @Override
    protected void initSnapshotGenerator(final PipeSnapshotEvent event)
        throws IOException, IllegalPathException {
      // Do nothing
    }

    @Override
    protected boolean hasNextEventInCurrentSnapshot() {
      return !snapshotPlans.isEmpty();
    }

    @Override
    protected PipeWritePlanEvent getNextEventInCurrentSnapshot() {
      return snapshotPlans.removeFirst();
    }

    @Override
    protected List<PipeWritePlanEvent> trimRealtimeEventByPrivilege(final PipeWritePlanEvent event)
        throws AccessDeniedException {
      return privilegeTrimmer.apply(event);
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
