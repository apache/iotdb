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

package org.apache.iotdb.rpc.subscription.payload.request;

import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PipeSubscribeSeekReqTest {

  @Test
  public void testTopicProgressSeekRoundTrip() throws IOException {
    final Map<WriterId, WriterProgress> writerPositions = new LinkedHashMap<>();
    writerPositions.put(new WriterId("1_100", 1, 2L), new WriterProgress(1000L, 10L));
    final TopicProgress original =
        new TopicProgress(Collections.singletonMap("1_100", new RegionProgress(writerPositions)));

    final PipeSubscribeSeekReq req =
        PipeSubscribeSeekReq.toTPipeSubscribeSeekAfterReq("topicA", original);
    final PipeSubscribeSeekReq parsed = PipeSubscribeSeekReq.fromTPipeSubscribeReq(req);

    assertEquals(PipeSubscribeSeekReq.SEEK_AFTER_TOPIC_PROGRESS, parsed.getSeekType());
    assertEquals("topicA", parsed.getTopicName());
    assertEquals(original, parsed.getTopicProgress());
  }
}
