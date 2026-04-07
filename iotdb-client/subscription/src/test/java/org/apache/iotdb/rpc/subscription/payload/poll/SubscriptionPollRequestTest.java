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

public class SubscriptionPollRequestTest {

  @Test
  public void testRoundTripWithProgressByTopic() throws IOException {
    final Map<WriterId, WriterProgress> writerPositions = new LinkedHashMap<>();
    writerPositions.put(new WriterId("1_100", 7, 2L), new WriterProgress(1001L, 11L));
    writerPositions.put(new WriterId("1_100", 8, 1L), new WriterProgress(999L, 9L));

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
}
