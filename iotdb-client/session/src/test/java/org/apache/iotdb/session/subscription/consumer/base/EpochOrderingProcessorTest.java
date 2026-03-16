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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EpochOrderingProcessorTest {

  private static final String REGION_A = "regionA";
  private static final String REGION_B = "regionB";
  private static final String TOPIC = "topic1";
  private static final String GROUP = "group1";

  private EpochOrderingProcessor processor;

  @Before
  public void setUp() {
    // Use short timeout for timeout tests
    processor = new EpochOrderingProcessor(200);
  }

  // ──────────────────────────────────────────────────
  // Helper methods
  // ──────────────────────────────────────────────────

  /** Create a normal data message for a given region, epoch, and dataNodeId. */
  private static SubscriptionMessage dataMsg(
      final String regionId, final long epoch, final int dataNodeId) {
    final SubscriptionCommitContext ctx =
        new SubscriptionCommitContext(dataNodeId, 0, TOPIC, GROUP, 0, regionId, epoch);
    // Use the Tablet-based constructor with empty map for a lightweight data message
    return new SubscriptionMessage(ctx, Collections.emptyMap());
  }

  /** Create a sentinel message for the given region and endingEpoch. */
  private static SubscriptionMessage sentinel(final String regionId, final long endingEpoch) {
    final SubscriptionCommitContext ctx =
        new SubscriptionCommitContext(0, 0, TOPIC, GROUP, 0, regionId, endingEpoch);
    // Sentinel constructor (no handler)
    return new SubscriptionMessage(ctx);
  }

  /** Create a non-consensus message (empty regionId). */
  private static SubscriptionMessage nonConsensusMsg() {
    final SubscriptionCommitContext ctx =
        new SubscriptionCommitContext(1, 0, TOPIC, GROUP, 0, "", 0);
    return new SubscriptionMessage(ctx, Collections.emptyMap());
  }

  /** Assert that the output contains exactly the expected messages in order. */
  private static void assertOutput(
      final List<SubscriptionMessage> actual, final SubscriptionMessage... expected) {
    Assert.assertEquals("Output size mismatch", expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertSame("Mismatch at index " + i, expected[i], actual.get(i));
    }
  }

  /** Assert that the output contains the expected messages (order-independent). */
  private static void assertOutputContainsAll(
      final List<SubscriptionMessage> actual, final SubscriptionMessage... expected) {
    Assert.assertEquals("Output size mismatch", expected.length, actual.size());
    for (final SubscriptionMessage msg : expected) {
      Assert.assertTrue("Missing message in output", actual.contains(msg));
    }
  }

  // ──────────────────────────────────────────────────
  // Test 1: Normal single-region flow
  // ──────────────────────────────────────────────────

  @Test
  public void testSingleRegionSameEpochPassThrough() {
    final SubscriptionMessage m1 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage m2 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage m3 = dataMsg(REGION_A, 0, 1);

    final List<SubscriptionMessage> result = processor.process(Arrays.asList(m1, m2, m3));

    assertOutput(result, m1, m2, m3);
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 2: Non-consensus messages pass through
  // ──────────────────────────────────────────────────

  @Test
  public void testNonConsensusMessagesPassThrough() {
    final SubscriptionMessage nc1 = nonConsensusMsg();
    final SubscriptionMessage nc2 = nonConsensusMsg();

    final List<SubscriptionMessage> result = processor.process(Arrays.asList(nc1, nc2));

    assertOutput(result, nc1, nc2);
  }

  // ──────────────────────────────────────────────────
  // Test 3: Normal epoch switch with sentinel
  // ──────────────────────────────────────────────────

  @Test
  public void testNormalEpochSwitchWithSentinel() {
    final SubscriptionMessage oldData1 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage oldData2 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData1 = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage sent = sentinel(REGION_A, 0);

    // Phase 1: old epoch data → INITIAL→STABLE
    List<SubscriptionMessage> result = processor.process(Arrays.asList(oldData1, oldData2));
    assertOutput(result, oldData1, oldData2);

    // Phase 2: new epoch data arrives → STABLE→BUFFERING
    result = processor.process(Collections.singletonList(newData1));
    Assert.assertEquals("New epoch data should be buffered", 0, result.size());
    Assert.assertEquals(1, processor.getBufferedCount());

    // Phase 3: sentinel arrives → releases buffer, resets to INITIAL
    result = processor.process(Collections.singletonList(sent));
    // Output: released buffered newData1 + sentinel
    Assert.assertEquals(2, result.size());
    Assert.assertSame(newData1, result.get(0));
    Assert.assertSame(sent, result.get(1));
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 4: sentinelSeen optimization
  // ──────────────────────────────────────────────────

  @Test
  public void testSentinelSeenOptimization() {
    final SubscriptionMessage oldData = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage sent = sentinel(REGION_A, 0);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);

    // Phase 1: old epoch data
    processor.process(Collections.singletonList(oldData));

    // Phase 2: sentinel arrives while in STABLE → sentinelSeen = true
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(sent));
    assertOutput(result, sent); // sentinel passes through

    // Phase 3: new epoch data arrives → with sentinelSeen, skips BUFFERING
    result = processor.process(Collections.singletonList(newData));
    assertOutput(result, newData); // immediately accepted
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 5: BUFFERING passes old-epoch data through
  // ──────────────────────────────────────────────────

  @Test
  public void testBufferingPassesOldEpochData() {
    final SubscriptionMessage old1 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage old2 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage sent = sentinel(REGION_A, 0);

    // INITIAL → STABLE with epoch 0
    processor.process(Collections.singletonList(old1));

    // New epoch → STABLE → BUFFERING
    processor.process(Collections.singletonList(newData));
    Assert.assertEquals(1, processor.getBufferedCount());

    // Old epoch data arrives in BUFFERING → passes through
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(old2));
    assertOutput(result, old2);
    Assert.assertEquals(1, processor.getBufferedCount()); // newData still buffered

    // Sentinel releases buffer
    result = processor.process(Collections.singletonList(sent));
    Assert.assertEquals(2, result.size());
    Assert.assertSame(newData, result.get(0));
    Assert.assertSame(sent, result.get(1));
  }

  // ──────────────────────────────────────────────────
  // Test 6: Timeout releases buffer
  // ──────────────────────────────────────────────────

  @Test
  public void testTimeoutReleasesBuffer() throws InterruptedException {
    final SubscriptionMessage oldData = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);

    // INITIAL → STABLE
    processor.process(Collections.singletonList(oldData));

    // STABLE → BUFFERING
    processor.process(Collections.singletonList(newData));
    Assert.assertEquals(1, processor.getBufferedCount());

    // Wait for timeout (processor has 200ms timeout)
    Thread.sleep(300);

    // Next process call should trigger timeout release
    List<SubscriptionMessage> result = processor.process(Collections.emptyList());
    Assert.assertTrue("Timeout should release buffer", result.size() > 0);
    Assert.assertSame(newData, result.get(0));
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 7: releaseBufferedForDataNode
  // ──────────────────────────────────────────────────

  @Test
  public void testReleaseBufferedForDataNode() {
    final SubscriptionMessage old1 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);

    processor.process(Collections.singletonList(old1));
    processor.process(Collections.singletonList(newData));
    Assert.assertEquals(1, processor.getBufferedCount());

    // Release for wrong node → nothing released
    List<SubscriptionMessage> released = processor.releaseBufferedForDataNode(999);
    Assert.assertTrue(released.isEmpty());
    Assert.assertEquals(1, processor.getBufferedCount());

    // Release for correct node (dataNodeId=1, currentEpoch producer)
    released = processor.releaseBufferedForDataNode(1);
    assertOutput(released, newData);
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 8: releaseBufferedForUnavailableNodes
  // ──────────────────────────────────────────────────

  @Test
  public void testReleaseBufferedForUnavailableNodes() {
    final SubscriptionMessage oldData = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);

    processor.process(Collections.singletonList(oldData));
    processor.process(Collections.singletonList(newData));
    Assert.assertEquals(1, processor.getBufferedCount());

    // DataNode 1 is still available → nothing released
    Set<Integer> available = new HashSet<>(Arrays.asList(1, 2, 3));
    List<SubscriptionMessage> output = new ArrayList<>();
    processor.releaseBufferedForUnavailableNodes(available, output);
    Assert.assertTrue(output.isEmpty());

    // DataNode 1 is no longer available → release
    available = new HashSet<>(Arrays.asList(2, 3));
    processor.releaseBufferedForUnavailableNodes(available, output);
    assertOutput(output, newData);
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 9: flush releases all buffers
  // ──────────────────────────────────────────────────

  @Test
  public void testFlushReleasesAll() {
    final SubscriptionMessage oldA = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newA = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage oldB = dataMsg(REGION_B, 0, 1);
    final SubscriptionMessage newB = dataMsg(REGION_B, 1000, 2);

    // Put both regions into BUFFERING
    processor.process(Collections.singletonList(oldA));
    processor.process(Collections.singletonList(newA));
    processor.process(Collections.singletonList(oldB));
    processor.process(Collections.singletonList(newB));
    Assert.assertEquals(2, processor.getBufferedCount());

    // flush() releases all
    List<SubscriptionMessage> flushed = processor.flush();
    Assert.assertEquals(2, flushed.size());
    Assert.assertTrue(flushed.contains(newA));
    Assert.assertTrue(flushed.contains(newB));
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 10: Multi-region independence
  // ──────────────────────────────────────────────────

  @Test
  public void testMultiRegionIndependence() {
    final SubscriptionMessage aOld = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage aNew = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage bData = dataMsg(REGION_B, 0, 3);
    final SubscriptionMessage sentA = sentinel(REGION_A, 0);

    // Region A: INITIAL → STABLE
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(aOld));
    assertOutput(result, aOld);

    // Region A: STABLE → BUFFERING; Region B: INITIAL → STABLE
    // Process both in one batch: aNew first (region A changes), then bData (region B first msg)
    result = processor.process(Arrays.asList(aNew, bData));
    // aNew should be buffered, bData should pass through
    assertOutput(result, bData);
    Assert.assertEquals(1, processor.getBufferedCount()); // only region A buffering

    // Region A sentinel → releases buffer. Region B unaffected.
    result = processor.process(Collections.singletonList(sentA));
    Assert.assertEquals(2, result.size());
    Assert.assertSame(aNew, result.get(0));
    Assert.assertSame(sentA, result.get(1));
  }

  // ──────────────────────────────────────────────────
  // Test 11: Duplicate sentinels are no-op
  // ──────────────────────────────────────────────────

  @Test
  public void testDuplicateSentinelIsNoOp() {
    final SubscriptionMessage data = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage sent1 = sentinel(REGION_A, 0);
    final SubscriptionMessage sent2 = sentinel(REGION_A, 0);

    processor.process(Collections.singletonList(data));
    processor.process(Collections.singletonList(newData));
    Assert.assertEquals(1, processor.getBufferedCount());

    // First sentinel releases buffer
    processor.process(Collections.singletonList(sent1));
    Assert.assertEquals(0, processor.getBufferedCount());

    // Second sentinel is a no-op (state is now INITIAL, epoch doesn't match)
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(sent2));
    // Sentinel still passes through (for downstream stripping)
    assertOutput(result, sent2);
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test 12: Sentinel with wrong epoch is ignored
  // ──────────────────────────────────────────────────

  @Test
  public void testSentinelWrongEpochIgnored() {
    final SubscriptionMessage data = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage wrongSent = sentinel(REGION_A, 999); // wrong epoch

    processor.process(Collections.singletonList(data));
    processor.process(Collections.singletonList(newData));
    Assert.assertEquals(1, processor.getBufferedCount());

    // Sentinel with epoch 999 doesn't match currentEpoch 0 → no-op, buffer not released
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(wrongSent));
    assertOutput(result, wrongSent); // sentinel passes through
    Assert.assertEquals(1, processor.getBufferedCount()); // buffer NOT released
  }

  // ──────────────────────────────────────────────────
  // Test 13: Consecutive epoch transitions
  // ──────────────────────────────────────────────────

  @Test
  public void testConsecutiveEpochTransitions() {
    // epoch 0 → 1000 → 2000

    final SubscriptionMessage d0 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage d1 = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage s0 = sentinel(REGION_A, 0);
    final SubscriptionMessage d2 = dataMsg(REGION_A, 2000, 3);
    final SubscriptionMessage s1 = sentinel(REGION_A, 1000);

    // epoch 0
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(d0));
    assertOutput(result, d0);

    // epoch 1000 arrives → BUFFERING
    result = processor.process(Collections.singletonList(d1));
    Assert.assertEquals(0, result.size());
    Assert.assertEquals(1, processor.getBufferedCount());

    // sentinel(0) → releases d1
    result = processor.process(Collections.singletonList(s0));
    Assert.assertEquals(2, result.size());
    Assert.assertSame(d1, result.get(0));
    Assert.assertSame(s0, result.get(1));

    // Now in INITIAL state. d1 was released but not "seen by STABLE".
    // d2 with epoch 2000 arrives → since INITIAL, goes to STABLE(epoch=2000)
    // Wait, after sentinel release, state is INITIAL. Let me trace through:
    // After sentinel(0): state=INITIAL. Next d2(epoch=2000) → INITIAL→STABLE(2000)
    // But we need d1 to transition to STABLE(1000) first.
    // Let me fix: after sentinel release, the buffered d1 is in output, but processor is in
    // INITIAL. The next message should set the epoch. Since d1 was released (already in output),
    // the processor sees d2 next → INITIAL→STABLE(2000).

    result = processor.process(Collections.singletonList(d2));
    assertOutput(result, d2); // INITIAL → STABLE(2000)
  }

  // ──────────────────────────────────────────────────
  // Test 14: getBufferedCount accuracy
  // ──────────────────────────────────────────────────

  @Test
  public void testGetBufferedCount() {
    Assert.assertEquals(0, processor.getBufferedCount());

    final SubscriptionMessage old = dataMsg(REGION_A, 0, 1);
    processor.process(Collections.singletonList(old));
    Assert.assertEquals(0, processor.getBufferedCount());

    final SubscriptionMessage new1 = dataMsg(REGION_A, 1000, 2);
    processor.process(Collections.singletonList(new1));
    Assert.assertEquals(1, processor.getBufferedCount());

    final SubscriptionMessage new2 = dataMsg(REGION_A, 1000, 2);
    processor.process(Collections.singletonList(new2));
    Assert.assertEquals(2, processor.getBufferedCount());

    // sentinel releases all
    final SubscriptionMessage sent = sentinel(REGION_A, 0);
    processor.process(Collections.singletonList(sent));
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test: Mixed batch with data, sentinel, and new data
  // ──────────────────────────────────────────────────

  @Test
  public void testMixedBatchInSingleProcess() {
    // Single batch: old-epoch data, sentinel, new-epoch data
    final SubscriptionMessage old1 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage old2 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newData = dataMsg(REGION_A, 1000, 2);
    final SubscriptionMessage sent = sentinel(REGION_A, 0);

    // Process: old1, old2, newData, sent in one batch
    // old1: INITIAL→STABLE(0) → output
    // old2: STABLE, same epoch → output
    // newData: STABLE, different epoch → BUFFERING, buffered
    // sent: BUFFERING, epoch matches → release buffer (newData first), then sentinel
    List<SubscriptionMessage> result = processor.process(Arrays.asList(old1, old2, newData, sent));

    Assert.assertEquals(4, result.size());
    Assert.assertSame(old1, result.get(0));
    Assert.assertSame(old2, result.get(1));
    Assert.assertSame(newData, result.get(2));
    Assert.assertSame(sent, result.get(3));
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test: Initial epoch = 0, then route change to timestamp
  // ──────────────────────────────────────────────────

  @Test
  public void testInitialEpochZeroToTimestamp() {
    // Simulates real scenario: server starts with epoch=0, then route change sets epoch to
    // a timestamp value like 1700000000000
    final long timestamp = 1700000000000L;

    final SubscriptionMessage d1 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage d2 = dataMsg(REGION_A, 0, 1);
    final SubscriptionMessage newD = dataMsg(REGION_A, timestamp, 2);
    final SubscriptionMessage sent = sentinel(REGION_A, 0);

    // epoch=0 data
    List<SubscriptionMessage> result = processor.process(Arrays.asList(d1, d2));
    assertOutput(result, d1, d2);

    // New epoch (large timestamp) → BUFFERING
    result = processor.process(Collections.singletonList(newD));
    Assert.assertEquals(0, result.size());
    Assert.assertEquals(1, processor.getBufferedCount());

    // Sentinel ends epoch 0
    result = processor.process(Collections.singletonList(sent));
    Assert.assertEquals(2, result.size());
    Assert.assertSame(newD, result.get(0));
    Assert.assertSame(sent, result.get(1));
  }

  // ──────────────────────────────────────────────────
  // Test: Empty input
  // ──────────────────────────────────────────────────

  @Test
  public void testEmptyInput() {
    final List<SubscriptionMessage> result = processor.process(Collections.emptyList());
    Assert.assertTrue(result.isEmpty());
  }

  // ──────────────────────────────────────────────────
  // Test: Sentinel in INITIAL state is no-op
  // ──────────────────────────────────────────────────

  @Test
  public void testSentinelInInitialState() {
    final SubscriptionMessage sent = sentinel(REGION_A, 0);

    // Sentinel arrives before any data → no matching state → passes through
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(sent));
    assertOutput(result, sent); // sentinel always passes through
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test: Same-node epoch update (routing update race)
  // ──────────────────────────────────────────────────

  @Test
  public void testSameNodeEpochUpdateStaysStable() {
    // Simulates routing update race: new leader writes with epoch=0 before
    // onRegionRouteChanged sets the epoch to the broadcast timestamp.
    // Same dataNodeId should NOT trigger BUFFERING.
    final long newEpoch = 1700000000000L;

    final SubscriptionMessage earlyData = dataMsg(REGION_A, 0, 2); // NodeB, epoch=0
    final SubscriptionMessage lateData = dataMsg(REGION_A, newEpoch, 2); // NodeB, epoch=newEpoch
    final SubscriptionMessage moreData = dataMsg(REGION_A, newEpoch, 2);

    // NodeB sends data with epoch=0 → INITIAL → STABLE(0, nodeB)
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(earlyData));
    assertOutput(result, earlyData);

    // NodeB sends data with epoch=newEpoch → same node, epoch changed internally
    // Should stay STABLE (no BUFFERING), update epoch
    result = processor.process(Collections.singletonList(lateData));
    assertOutput(result, lateData);
    Assert.assertEquals(0, processor.getBufferedCount()); // NOT buffered

    // Subsequent messages with newEpoch pass through normally
    result = processor.process(Collections.singletonList(moreData));
    assertOutput(result, moreData);
    Assert.assertEquals(0, processor.getBufferedCount());
  }

  // ──────────────────────────────────────────────────
  // Test: Same-node epoch update followed by real leader transition
  // ──────────────────────────────────────────────────

  @Test
  public void testSameNodeEpochUpdateThenRealTransition() {
    // Full scenario: NodeA (old leader) → NodeB (new leader with routing race)
    final long oldEpoch = 1000;
    final long newEpoch = 2000;

    final SubscriptionMessage oldData = dataMsg(REGION_A, oldEpoch, 1); // NodeA
    final SubscriptionMessage earlyNewData = dataMsg(REGION_A, 0, 2); // NodeB, epoch=0 (race)
    final SubscriptionMessage lateNewData = dataMsg(REGION_A, newEpoch, 2); // NodeB, epoch=newEpoch
    final SubscriptionMessage sentOld = sentinel(REGION_A, oldEpoch);

    // Phase 1: old leader data
    List<SubscriptionMessage> result = processor.process(Collections.singletonList(oldData));
    assertOutput(result, oldData); // STABLE(oldEpoch, nodeA)

    // Phase 2: new leader data with epoch=0 (different node, different epoch) → BUFFERING
    result = processor.process(Collections.singletonList(earlyNewData));
    Assert.assertEquals(0, result.size());
    Assert.assertEquals(1, processor.getBufferedCount());

    // Phase 3: more new leader data with epoch=newEpoch → still buffered
    result = processor.process(Collections.singletonList(lateNewData));
    Assert.assertEquals(0, result.size());
    Assert.assertEquals(2, processor.getBufferedCount());

    // Phase 4: sentinel for old epoch → releases buffer
    result = processor.process(Collections.singletonList(sentOld));
    Assert.assertEquals(3, result.size());
    Assert.assertSame(earlyNewData, result.get(0)); // released from buffer
    Assert.assertSame(lateNewData, result.get(1)); // released from buffer
    Assert.assertSame(sentOld, result.get(2));
    Assert.assertEquals(0, processor.getBufferedCount());

    // Phase 5: next message from NodeB → INITIAL → STABLE
    // After buffer release, the mixed-epoch data (0, newEpoch) was already delivered.
    // New data from NodeB with newEpoch enters normally.
    final SubscriptionMessage nextData = dataMsg(REGION_A, newEpoch, 2);
    result = processor.process(Collections.singletonList(nextData));
    assertOutput(result, nextData); // INITIAL → STABLE(newEpoch, nodeB)
  }
}
