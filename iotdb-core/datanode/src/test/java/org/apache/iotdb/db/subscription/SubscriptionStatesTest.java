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

package org.apache.iotdb.db.subscription;

import org.apache.iotdb.db.subscription.broker.SubscriptionStates;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEmptyEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SubscriptionStatesTest {

  @Test
  public void testFilter() {
    final String device = "root.test.g_0.d_0";
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    final Tablet tablet = new Tablet(device, schemaList, 42);
    final TabletsPayload payload = new TabletsPayload(Collections.singletonList(tablet), -1);

    final SubscriptionEvent event1 =
        new SubscriptionEvent(
            new SubscriptionPipeEmptyEvent(),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                payload,
                new SubscriptionCommitContext(-1, -1, "topic1", "cg1", 0)));
    final SubscriptionEvent event2 =
        new SubscriptionEvent(
            new SubscriptionPipeEmptyEvent(),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                payload,
                new SubscriptionCommitContext(-1, -1, "topic2", "cg1", 0)));
    final SubscriptionEvent event3 =
        new SubscriptionEvent(
            new SubscriptionPipeEmptyEvent(),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                payload,
                new SubscriptionCommitContext(-1, -1, "topic3", "cg1", 0)));
    final SubscriptionEvent event4 =
        new SubscriptionEvent(
            new SubscriptionPipeEmptyEvent(),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                payload,
                new SubscriptionCommitContext(-1, -1, "topic4", "cg1", 0)));

    final SubscriptionStates states = new SubscriptionStates();
    Pair<List<SubscriptionEvent>, List<SubscriptionEvent>> eventsToPollWithEventsToNack;

    // the size of the response payload is 110
    eventsToPollWithEventsToNack =
        states.filter(Arrays.asList(event1, event2, event3, event4), 300);
    Assert.assertEquals(2, eventsToPollWithEventsToNack.left.size());
    Assert.assertEquals(2, eventsToPollWithEventsToNack.right.size());
    Assert.assertTrue(
        Objects.equals(eventsToPollWithEventsToNack.left.get(0), event1)
                && Objects.equals(eventsToPollWithEventsToNack.left.get(1), event2)
            || Objects.equals(eventsToPollWithEventsToNack.left.get(1), event1)
                && Objects.equals(eventsToPollWithEventsToNack.left.get(0), event2));

    eventsToPollWithEventsToNack =
        states.filter(Arrays.asList(event1, event2, event3, event4), 300);
    Assert.assertEquals(2, eventsToPollWithEventsToNack.left.size());
    Assert.assertEquals(2, eventsToPollWithEventsToNack.right.size());
    Assert.assertTrue(
        Objects.equals(eventsToPollWithEventsToNack.left.get(0), event3)
                && Objects.equals(eventsToPollWithEventsToNack.left.get(1), event4)
            || Objects.equals(eventsToPollWithEventsToNack.left.get(1), event3)
                && Objects.equals(eventsToPollWithEventsToNack.left.get(0), event4));

    eventsToPollWithEventsToNack =
        states.filter(Arrays.asList(event1, event2, event3, event4), 150);
    Assert.assertEquals(1, eventsToPollWithEventsToNack.left.size());
    Assert.assertEquals(3, eventsToPollWithEventsToNack.right.size());
    Assert.assertEquals(eventsToPollWithEventsToNack.left.get(0), event1);

    eventsToPollWithEventsToNack =
        states.filter(Arrays.asList(event1, event2, event3, event4), 100);
    Assert.assertEquals(0, eventsToPollWithEventsToNack.left.size());
    Assert.assertEquals(4, eventsToPollWithEventsToNack.right.size());
  }
}
