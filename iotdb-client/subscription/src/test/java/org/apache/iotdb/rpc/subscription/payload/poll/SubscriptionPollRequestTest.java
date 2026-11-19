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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SubscriptionPollRequestTest {

  @Test
  public void testRoundTripWithProgressByTopic() throws IOException {
    final Map<WriterId, WriterProgress> writerPositions = new LinkedHashMap<>();
    writerPositions.put(new WriterId("1_100", 7), new WriterProgress(1001L, 11L));
    writerPositions.put(new WriterId("1_100", 8), new WriterProgress(999L, 9L));

    final TopicProgress topicProgress =
        new TopicProgress(Collections.singletonMap("1_100", new RegionProgress(writerPositions)));
    final Map<String, TopicProgress> progressByTopic = new LinkedHashMap<>();
    progressByTopic.put("topicA", topicProgress);

    final SubscriptionPollRequest original =
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL.getType(),
            new PollPayload(Collections.singleton("topicA")),
            1234L,
            4096L,
            progressByTopic);

    final ByteBuffer serialized = SubscriptionPollRequest.serialize(original);
    final SubscriptionPollRequest parsed = SubscriptionPollRequest.deserialize(serialized);

    assertEquals(original.getRequestType(), parsed.getRequestType());
    assertEquals(original.getTimeoutMs(), parsed.getTimeoutMs());
    assertEquals(original.getMaxBytes(), parsed.getMaxBytes());
    assertEquals(original.getPayload(), parsed.getPayload());
    assertEquals(progressByTopic, parsed.getProgressByTopic());
  }

  @Test
  public void testAllRequestPayloadTypesRoundTrip() throws IOException {
    final SubscriptionCommitContext context =
        new SubscriptionCommitContext(1, 2, "topic", "group", 3L);
    assertRequestPayloadRoundTrip(
        SubscriptionPollRequestType.POLL, new PollPayload(Collections.singleton("topic")));
    assertRequestPayloadRoundTrip(
        SubscriptionPollRequestType.POLL_FILE, new PollFilePayload(context, 99L));
    assertRequestPayloadRoundTrip(
        SubscriptionPollRequestType.POLL_TABLETS, new PollTabletsPayload(context, 7));
  }

  @Test
  public void testNullProgressDefaultsToEmptyAndTypeLookupRejectsUnknownValues() {
    final SubscriptionPollRequest request =
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL.getType(),
            new PollPayload(Collections.singleton("topic")),
            1L,
            2L,
            null);
    assertTrue(request.getProgressByTopic().isEmpty());
    assertTrue(
        SubscriptionPollRequestType.isValidatedRequestType(
            SubscriptionPollRequestType.POLL.getType()));
    assertFalse(SubscriptionPollRequestType.isValidatedRequestType(Short.MAX_VALUE));
    assertNull(SubscriptionPollRequestType.valueOf(Short.MAX_VALUE));
  }

  private static void assertRequestPayloadRoundTrip(
      final SubscriptionPollRequestType type, final SubscriptionPollPayload payload)
      throws IOException {
    final SubscriptionPollRequest original =
        new SubscriptionPollRequest(type.getType(), payload, 123L, 456L);
    final SubscriptionPollRequest parsed =
        SubscriptionPollRequest.deserialize(SubscriptionPollRequest.serialize(original));
    assertEquals(type.getType(), parsed.getRequestType());
    assertEquals(payload, parsed.getPayload());
    assertEquals(123L, parsed.getTimeoutMs());
    assertEquals(456L, parsed.getMaxBytes());
  }
}
