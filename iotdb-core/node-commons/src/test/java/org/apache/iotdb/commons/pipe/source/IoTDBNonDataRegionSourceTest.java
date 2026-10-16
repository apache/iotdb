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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

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

  private static final class TestNonDataRegionSource extends IoTDBNonDataRegionSource {

    private final AbstractPipeListeningQueue listeningQueue;

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
  }
}
