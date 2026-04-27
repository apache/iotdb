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

package org.apache.iotdb.db.pipe.agent.task.connection;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeRealtimePriorityBlockingQueue;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PipeEventCollectorTest {

  @Test
  public void testCollectorDoesNotOfferEventsOfDroppedPipeToUnboundedPendingQueue() {
    verifyCollectorDoesNotOfferEventsOfDroppedPipe(
        new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter()));
  }

  @Test
  public void testCollectorDoesNotOfferEventsOfDroppedPipeToRealtimePendingQueue() {
    verifyCollectorDoesNotOfferEventsOfDroppedPipe(new PipeRealtimePriorityBlockingQueue());
  }

  private void verifyCollectorDoesNotOfferEventsOfDroppedPipe(
      final UnboundedBlockingPendingQueue<Event> pendingQueue) {
    pendingQueue.discardEventsOfPipe("pipe", 1L, 1);

    final PipeEventCollector droppedPipeCollector =
        new PipeEventCollector(pendingQueue, 1L, 1, false, false, false);
    final PipeRawTabletInsertionEvent droppedPipeEvent =
        createPipeRawTabletInsertionEvent("pipe", 1L);
    droppedPipeCollector.collect(droppedPipeEvent);

    Assert.assertTrue(droppedPipeEvent.isReleased());
    Assert.assertEquals(0, pendingQueue.size());

    final PipeEventCollector recreatedPipeCollector =
        new PipeEventCollector(pendingQueue, 2L, 1, false, false, false);
    final PipeRawTabletInsertionEvent recreatedPipeEvent =
        createPipeRawTabletInsertionEvent("pipe", 2L);
    recreatedPipeCollector.collect(recreatedPipeEvent);

    Assert.assertFalse(recreatedPipeEvent.isReleased());
    Assert.assertEquals(1, pendingQueue.size());

    pendingQueue.discardAllEvents();
    Assert.assertTrue(recreatedPipeEvent.isReleased());
  }

  private PipeRawTabletInsertionEvent createPipeRawTabletInsertionEvent(
      final String pipeName, final long creationTime) {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(new MeasurementSchema("s1", TSDataType.INT64));
    final Tablet tablet = new Tablet("root.db.d1", schemaList, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue("s1", 0, 1L);
    return new PipeRawTabletInsertionEvent(
        false, "root.db", "db", "root.db", tablet, false, pipeName, creationTime, null, null, false);
  }
}
