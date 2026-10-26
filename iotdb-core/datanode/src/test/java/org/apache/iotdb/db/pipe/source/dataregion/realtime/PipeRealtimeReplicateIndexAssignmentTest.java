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

package org.apache.iotdb.db.pipe.source.dataregion.realtime;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeRealtimeReplicateIndexAssignmentTest {

  @Test
  public void assignReplicateIndexShouldBeLazyAndIdempotent() {
    final TestPipeRealtimeDataRegionSource source = new TestPipeRealtimeDataRegionSource();
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent();

    Assert.assertEquals(EnrichedEvent.NO_COMMIT_ID, event.getReplicateIndexForIoTV2());

    final Event suppliedEvent = source.assign(event);
    Assert.assertSame(event, suppliedEvent);
    Assert.assertEquals(1L, event.getReplicateIndexForIoTV2());
    Assert.assertEquals(1L, source.assignedCount.get());

    source.assign(event);
    Assert.assertEquals(1L, event.getReplicateIndexForIoTV2());
    Assert.assertEquals(1L, source.assignedCount.get());
  }

  private static class TestPipeRealtimeDataRegionSource extends PipeRealtimeDataRegionLogSource {
    private final AtomicLong nextReplicateIndex = new AtomicLong(1);
    private final AtomicLong assignedCount = new AtomicLong(0);

    private Event assign(final PipeDeleteDataNodeEvent event) {
      final TsFileResource resource = mock(TsFileResource.class);
      when(resource.getTsFilePath()).thenReturn("target/test.tsfile");
      final PipeRealtimeEvent realtimeEvent =
          new PipeRealtimeEvent(event, new TsFileEpoch(resource), null);
      return assignReplicateIndexIfNeeded(realtimeEvent, event);
    }

    @Override
    protected boolean shouldAssignReplicateIndex(final Event suppliedEvent) {
      return true;
    }

    @Override
    protected long assignReplicateIndexForRealtimeEvent() {
      assignedCount.incrementAndGet();
      return nextReplicateIndex.getAndIncrement();
    }
  }
}
