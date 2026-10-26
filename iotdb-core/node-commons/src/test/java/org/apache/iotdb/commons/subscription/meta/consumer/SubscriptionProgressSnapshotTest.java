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

package org.apache.iotdb.commons.subscription.meta.consumer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SubscriptionProgressSnapshotTest {

  @Test
  public void testSerializationRoundTrip() {
    final SubscriptionProgressSnapshot snapshot =
        new SubscriptionProgressSnapshot(
            3,
            "group",
            "topic",
            "3_1",
            true,
            true,
            100L,
            90L,
            11L,
            12L,
            1L,
            2L,
            3L,
            4L,
            5L,
            6L,
            7L,
            "consumer",
            8L,
            9L,
            10L,
            11L,
            SubscriptionProgressSnapshot.STATUS_STALLED);

    final SubscriptionProgressSnapshot restored =
        SubscriptionProgressSnapshot.deserialize(snapshot.serialize());

    assertEquals(snapshot.getDataNodeId(), restored.getDataNodeId());
    assertEquals(snapshot.getConsumerGroupId(), restored.getConsumerGroupId());
    assertEquals(snapshot.getTopicName(), restored.getTopicName());
    assertEquals(snapshot.getRegionId(), restored.getRegionId());
    assertTrue(restored.isActive());
    assertTrue(restored.isInitialized());
    assertEquals(snapshot.getCurrentWalSearchIndex(), restored.getCurrentWalSearchIndex());
    assertEquals(snapshot.getNextReadSearchIndex(), restored.getNextReadSearchIndex());
    assertEquals(snapshot.getRawWalGap(), restored.getRawWalGap());
    assertEquals(snapshot.getApproximateLag(), restored.getApproximateLag());
    assertEquals(snapshot.getRemainingEventCount(), restored.getRemainingEventCount());
    assertEquals(snapshot.getLastPollTimeMs(), restored.getLastPollTimeMs());
    assertEquals(snapshot.getLastProgressTimeMs(), restored.getLastProgressTimeMs());
    assertEquals(snapshot.getLastConsumerId(), restored.getLastConsumerId());
    assertEquals(snapshot.getSeekGeneration(), restored.getSeekGeneration());
    assertEquals(snapshot.getStatus(), restored.getStatus());
    assertTrue(restored.serialize().remaining() > 0);
  }
}
