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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionPipeTsFileEventBatchTest {

  @Test
  public void testEmptyInnerBatchConsumesOuterBatchWithoutPayload() throws Exception {
    final TestSubscriptionPipeTsFileEventBatch batch =
        new TestSubscriptionPipeTsFileEventBatch(
            new SubscriptionPrefetchingTsFileQueue("broker", "topic", null, new AtomicLong()));
    batch.addEnrichedEvent(
        new PipeRawTabletInsertionEvent(
            true,
            "root.db",
            "db",
            "root.db",
            createTablet(),
            false,
            "pipe",
            1L,
            null,
            null,
            false));

    Assert.assertTrue(batch.shouldEmitForTest());

    final List<SubscriptionEvent> events = batch.generateSubscriptionEventsForTest();
    Assert.assertNotNull(events);
    Assert.assertTrue(events.isEmpty());
    Assert.assertEquals(0, batch.getPipeEventCount());

    batch.cleanUp(true);
  }

  private static Tablet createTablet() {
    final Tablet tablet =
        new Tablet(
            "sensors",
            Arrays.asList("device", "temperature"),
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, 36.5);
    tablet.setRowSize(1);
    return tablet;
  }

  private static class TestSubscriptionPipeTsFileEventBatch
      extends SubscriptionPipeTsFileEventBatch {

    private TestSubscriptionPipeTsFileEventBatch(
        final SubscriptionPrefetchingTsFileQueue prefetchingQueue) {
      super(1, prefetchingQueue, 100_000, 1024 * 1024);
    }

    private void addEnrichedEvent(final PipeRawTabletInsertionEvent event) {
      enrichedEvents.add(event);
    }

    private boolean shouldEmitForTest() {
      return shouldEmit();
    }

    private List<SubscriptionEvent> generateSubscriptionEventsForTest() throws Exception {
      return generateSubscriptionEvents();
    }
  }
}
