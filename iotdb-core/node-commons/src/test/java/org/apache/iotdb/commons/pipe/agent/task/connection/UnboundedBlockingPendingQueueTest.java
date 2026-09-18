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

package org.apache.iotdb.commons.pipe.agent.task.connection;

import org.apache.iotdb.commons.pipe.metric.PipeEventCounter;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class UnboundedBlockingPendingQueueTest {

  @Test
  public void pollLastDecreasesEventCount() {
    final CountingEventCounter eventCounter = new CountingEventCounter();
    final UnboundedBlockingPendingQueue<Event> queue =
        new UnboundedBlockingPendingQueue<>(eventCounter);
    final Event event = new Event() {};

    queue.offer(event);
    Assert.assertEquals(1, eventCounter.getEventCount());

    Assert.assertSame(event, queue.pollLast());
    Assert.assertEquals(0, eventCounter.getEventCount());
  }

  private static class CountingEventCounter extends PipeEventCounter {

    private final AtomicInteger eventCount = new AtomicInteger();

    private int getEventCount() {
      return eventCount.get();
    }

    @Override
    public void increaseEventCount(final Event event) {
      if (event != null) {
        eventCount.incrementAndGet();
      }
    }

    @Override
    public void decreaseEventCount(final Event event) {
      if (event != null) {
        eventCount.decrementAndGet();
      }
    }

    @Override
    public void reset() {
      eventCount.set(0);
    }
  }
}
