/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeDataNodeCompletionOperatorTest {

  @Test
  public void testCompletionFollowsCommittedGenerationAndFailsClosed() {
    final AtomicBoolean remainingZero = new AtomicBoolean(true);
    final AtomicBoolean supported = new AtomicBoolean(true);
    final AtomicReference<Map<Integer, PipeRealtimeDataRegionSource>> expected =
        new AtomicReference<>(new HashMap<>());
    final AtomicReference<CommitterKey> currentCommitter = new AtomicReference<>();
    final PipeDataNodeCompletionOperator operator =
        new PipeDataNodeCompletionOperator(
            remainingZero::get,
            () -> new Pair<>(supported.get(), expected.get()),
            key -> key == currentCommitter.get());

    final PipeTaskMeta taskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0);
    final PipeTaskMeta staleTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0);
    final TestSource source = new TestSource(1, taskMeta);
    final PipeDataRegionAssigner assigner = mock(PipeDataRegionAssigner.class);
    final CommitterKey committerKey = mock(CommitterKey.class);
    final CommitterKey nextCommitterKey = mock(CommitterKey.class);
    final AtomicLong publishedGeneration = new AtomicLong(0);
    final AtomicLong publicationFailureEpoch = new AtomicLong(0);

    when(assigner.getAssignerEpoch()).thenReturn(10L);
    when(assigner.getPublishedDataGeneration()).thenAnswer(i -> publishedGeneration.get());
    when(assigner.getPublicationFailureEpoch()).thenAnswer(i -> publicationFailureEpoch.get());
    currentCommitter.set(committerKey);
    expected.set(Collections.singletonMap(1, source));

    operator.registerDataRegionSource(source);
    operator.register(source, assigner);
    assertEquals(0, operator.getCompletion());

    operator.markCompleted(1, staleTaskMeta, 10L, 0, source.getCompletionSourceId(), committerKey);
    assertEquals(0, operator.getCompletion());

    operator.markCompleted(1, taskMeta, 10L, 0, source.getCompletionSourceId(), committerKey);
    assertEquals(1, operator.getCompletion());

    publishedGeneration.incrementAndGet();
    assertEquals(0, operator.getCompletion());
    operator.markCompleted(1, taskMeta, 10L, 1, source.getCompletionSourceId(), committerKey);
    assertEquals(1, operator.getCompletion());

    source.setDegraded(true);
    assertEquals(0, operator.getCompletion());
    source.setDegraded(false);
    assertEquals(1, operator.getCompletion());

    remainingZero.set(false);
    assertEquals(0, operator.getCompletion());
    remainingZero.set(true);
    supported.set(false);
    assertEquals(0, operator.getCompletion());
    supported.set(true);
    assertEquals(1, operator.getCompletion());

    currentCommitter.set(nextCommitterKey);
    assertEquals(0, operator.getCompletion());
    operator.markCompleted(1, taskMeta, 10L, 1, source.getCompletionSourceId(), nextCommitterKey);
    assertEquals(1, operator.getCompletion());

    publicationFailureEpoch.incrementAndGet();
    assertEquals(0, operator.getCompletion());

    operator.markInvalid(1, taskMeta, source.getCompletionSourceId());
    operator.markCompleted(
        1, taskMeta, 10L, Long.MAX_VALUE, source.getCompletionSourceId(), nextCommitterKey);
    assertEquals(0, operator.getCompletion());

    operator.deregister(source, assigner);
    assertEquals(0, operator.getCompletion());
  }

  @Test
  public void testMembershipMustMatchExactlyAndEmptyLocalMembershipIsNeutral() {
    final AtomicReference<Map<Integer, PipeRealtimeDataRegionSource>> expected =
        new AtomicReference<>(new HashMap<>());
    final PipeDataNodeCompletionOperator operator =
        new PipeDataNodeCompletionOperator(
            () -> true, () -> new Pair<>(true, expected.get()), key -> true);

    final PipeTaskMeta firstTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0);
    final PipeTaskMeta secondTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0);
    final TestSource firstSource = new TestSource(1, firstTaskMeta);
    final TestSource secondSource = new TestSource(2, secondTaskMeta);
    final PipeDataRegionAssigner firstAssigner = mock(PipeDataRegionAssigner.class);
    final PipeDataRegionAssigner secondAssigner = mock(PipeDataRegionAssigner.class);
    final CommitterKey firstCommitter = mock(CommitterKey.class);
    final CommitterKey secondCommitter = mock(CommitterKey.class);

    when(firstAssigner.getAssignerEpoch()).thenReturn(11L);
    when(secondAssigner.getAssignerEpoch()).thenReturn(12L);

    operator.registerDataRegionSource(firstSource);
    operator.register(firstSource, firstAssigner);
    operator.markCompleted(
        1, firstTaskMeta, 11L, 0, firstSource.getCompletionSourceId(), firstCommitter);
    expected.set(Collections.singletonMap(1, firstSource));
    assertEquals(1, operator.getCompletion());

    operator.registerDataRegionSource(secondSource);
    operator.register(secondSource, secondAssigner);
    operator.markCompleted(
        2, secondTaskMeta, 12L, 0, secondSource.getCompletionSourceId(), secondCommitter);
    final Map<Integer, PipeRealtimeDataRegionSource> bothExpected = new HashMap<>();
    bothExpected.put(1, firstSource);
    bothExpected.put(2, secondSource);
    expected.set(bothExpected);
    assertEquals(1, operator.getCompletion());

    expected.set(Collections.singletonMap(2, secondSource));
    assertEquals(0, operator.getCompletion());
    operator.deregister(firstSource, firstAssigner);
    operator.deregisterDataRegionSource(firstSource);
    assertEquals(1, operator.getCompletion());

    expected.set(Collections.emptyMap());
    assertEquals(0, operator.getCompletion());
    operator.deregister(secondSource, secondAssigner);
    operator.deregisterDataRegionSource(secondSource);
    assertEquals(1, operator.getCompletion());
  }

  @Test
  public void testStaleBarrierCannotCompleteReplacementSource() {
    final PipeTaskMeta sharedTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0);
    final PipeDataRegionAssigner sharedAssigner = mock(PipeDataRegionAssigner.class);
    final CommitterKey committerKey = mock(CommitterKey.class);
    when(sharedAssigner.getAssignerEpoch()).thenReturn(20L);

    final AtomicReference<Map<Integer, PipeRealtimeDataRegionSource>> expected =
        new AtomicReference<>(new HashMap<>());
    final PipeDataNodeCompletionOperator operator =
        new PipeDataNodeCompletionOperator(
            () -> true, () -> new Pair<>(true, expected.get()), key -> true);

    final TestSource oldSource = new TestSource(1, sharedTaskMeta);
    operator.registerDataRegionSource(oldSource);
    operator.register(oldSource, sharedAssigner);
    operator.deregister(oldSource, sharedAssigner);
    operator.deregisterDataRegionSource(oldSource);

    final TestSource newSource = new TestSource(1, sharedTaskMeta);
    expected.set(Collections.singletonMap(1, newSource));
    operator.registerDataRegionSource(newSource);
    operator.register(newSource, sharedAssigner);

    operator.markCompleted(
        1, sharedTaskMeta, 20L, 0, oldSource.getCompletionSourceId(), committerKey);
    assertEquals(0, operator.getCompletion());

    operator.markCompleted(
        1, sharedTaskMeta, 20L, 0, newSource.getCompletionSourceId(), committerKey);
    assertEquals(1, operator.getCompletion());

    operator.markInvalid(1, sharedTaskMeta, oldSource.getCompletionSourceId());
    assertEquals(1, operator.getCompletion());

    operator.deregister(oldSource, sharedAssigner);
    operator.deregisterDataRegionSource(oldSource);
    assertEquals(1, operator.getCompletion());

    operator.markInvalid(1, sharedTaskMeta, newSource.getCompletionSourceId());
    assertEquals(0, operator.getCompletion());
  }

  private static class TestSource extends PipeRealtimeDataRegionSource {

    private final AtomicBoolean degraded = new AtomicBoolean(false);

    private TestSource(final int dataRegionId, final PipeTaskMeta pipeTaskMeta) {
      this.dataRegionId = dataRegionId;
      this.pipeTaskMeta = pipeTaskMeta;
    }

    private void setDegraded(final boolean degraded) {
      if (this.degraded.getAndSet(degraded) != degraded) {
        markCompletionStateChanged();
      }
    }

    @Override
    protected void doExtract(final PipeRealtimeEvent event) {
      // Do nothing.
    }

    @Override
    public Event supply() {
      return null;
    }

    @Override
    public boolean isTsFileEpochDegraded() {
      return degraded.get();
    }

    @Override
    public boolean isNeedListenToTsFile() {
      return false;
    }

    @Override
    public boolean isNeedListenToInsertNode() {
      return false;
    }
  }
}
