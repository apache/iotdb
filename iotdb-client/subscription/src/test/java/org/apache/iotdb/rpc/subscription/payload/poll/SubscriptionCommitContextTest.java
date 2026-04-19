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

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SubscriptionCommitContextTest {

  @Test
  public void testDeserializeCurrentCommitIdContext() throws IOException {
    final SubscriptionCommitContext original =
        new SubscriptionCommitContext(1, 2, "topic", "group", 3L);
    final ByteBuffer buffer = SubscriptionCommitContext.serialize(original);

    final SubscriptionCommitContext context = SubscriptionCommitContext.deserialize(buffer);

    assertEquals(1, context.getDataNodeId());
    assertEquals(2, context.getRebootTimes());
    assertEquals("topic", context.getTopicName());
    assertEquals("group", context.getConsumerGroupId());
    assertEquals(3L, context.getCommitId());
    assertEquals(0L, context.getSeekGeneration());
    assertEquals("", context.getRegionId());
    assertEquals(0L, context.getPhysicalTime());
    assertFalse(context.hasWriterProgress());
    assertTrue(context.isCommittable());
  }

  @Test
  public void testDeserializeCurrentPhysicalTimeContext() throws IOException {
    final SubscriptionCommitContext original =
        new SubscriptionCommitContext(1, 2, "topic", "group", 3L, 4L, "region", 5L);

    final ByteBuffer buffer = SubscriptionCommitContext.serialize(original);
    final SubscriptionCommitContext parsed = SubscriptionCommitContext.deserialize(buffer);

    assertEquals(original, parsed);
    assertFalse(parsed.hasWriterProgress());
    assertTrue(parsed.isCommittable());
  }

  @Test
  public void testDeserializeV2() throws IOException {
    final WriterId writerId = new WriterId("region", 7, 8L);
    final WriterProgress writerProgress = new WriterProgress(9L, 10L);
    final SubscriptionCommitContext original =
        new SubscriptionCommitContext(1, 2, "topic", "group", 3L, writerId, writerProgress);

    final ByteBuffer buffer = SubscriptionCommitContext.serialize(original);
    final SubscriptionCommitContext parsed = SubscriptionCommitContext.deserialize(buffer);

    assertEquals(original, parsed);
    assertEquals(writerId, parsed.getWriterId());
    assertEquals(writerProgress, parsed.getWriterProgress());
    assertEquals("region", parsed.getRegionId());
    assertEquals(9L, parsed.getPhysicalTime());
    assertEquals(10L, parsed.getLocalSeq());
    assertTrue(parsed.hasWriterProgress());
    assertTrue(parsed.isCommittable());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeserializeUnsupportedVersion() throws IOException {
    final ByteBuffer buffer = buildCurrentBufferWithVersion((byte) 1, 1, 2, "topic", "group", 3L);
    SubscriptionCommitContext.deserialize(buffer);
  }

  private static ByteBuffer buildCurrentBuffer(
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId)
      throws IOException {
    return buildCurrentBufferWithVersion(
        (byte) 2, dataNodeId, rebootTimes, topicName, consumerGroupId, commitId);
  }

  private static ByteBuffer buildCurrentBufferWithVersion(
      final byte version,
      final int dataNodeId,
      final int rebootTimes,
      final String topicName,
      final String consumerGroupId,
      final long commitId)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(version, outputStream);
      ReadWriteIOUtils.write(dataNodeId, outputStream);
      ReadWriteIOUtils.write(rebootTimes, outputStream);
      ReadWriteIOUtils.write(topicName, outputStream);
      ReadWriteIOUtils.write(consumerGroupId, outputStream);
      ReadWriteIOUtils.write(commitId, outputStream);
      ReadWriteIOUtils.write(0L, outputStream);
      ReadWriteIOUtils.write("", outputStream);
      ReadWriteIOUtils.write(0L, outputStream);
      ReadWriteIOUtils.write((byte) 0, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }
}
