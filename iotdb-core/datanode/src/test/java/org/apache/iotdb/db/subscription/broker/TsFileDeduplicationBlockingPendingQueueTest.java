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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

public class TsFileDeduplicationBlockingPendingQueueTest {

  @Test
  public void testDifferentPathsWithSameHashCodeAreNotDeduplicated() {
    final UnboundedBlockingPendingQueue<Event> inputPendingQueue =
        new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());
    final TsFileDeduplicationBlockingPendingQueue queue =
        new TsFileDeduplicationBlockingPendingQueue(inputPendingQueue);

    final PipeTsFileInsertionEvent firstEvent = Mockito.mock(PipeTsFileInsertionEvent.class);
    Mockito.when(firstEvent.getTsFile()).thenReturn(new CollidingFile("first.tsfile"));
    Mockito.when(firstEvent.isGeneratedByHistoricalExtractor()).thenReturn(false);

    final PipeTsFileInsertionEvent secondEvent = Mockito.mock(PipeTsFileInsertionEvent.class);
    Mockito.when(secondEvent.getTsFile()).thenReturn(new CollidingFile("second.tsfile"));
    Mockito.when(secondEvent.isGeneratedByHistoricalExtractor()).thenReturn(true);

    queue.directOffer(firstEvent);
    queue.directOffer(secondEvent);

    Assert.assertSame(firstEvent, queue.waitedPoll());
    Assert.assertSame(secondEvent, queue.waitedPoll());
  }

  private static class CollidingFile extends File {

    private CollidingFile(final String pathname) {
      super(pathname);
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }
}
