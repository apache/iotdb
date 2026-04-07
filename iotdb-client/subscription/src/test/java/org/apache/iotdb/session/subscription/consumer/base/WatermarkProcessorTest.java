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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class WatermarkProcessorTest {

  private static final String TOPIC = "topic1";
  private static final String GROUP = "group1";
  private static final String REGION_R1 = "R1";
  private static final String REGION_R2 = "R2";

  private static SubscriptionMessage dataMsg(final String regionId, final int dataNodeId) {
    final SubscriptionCommitContext ctx =
        new SubscriptionCommitContext(dataNodeId, 0, TOPIC, GROUP, 0L, 0L, regionId, 0L);
    return new SubscriptionMessage(ctx, Collections.emptyMap());
  }

  private static SubscriptionMessage watermarkMsg(
      final String regionId, final int dataNodeId, final long watermarkTs) {
    final SubscriptionCommitContext ctx =
        new SubscriptionCommitContext(dataNodeId, 0, TOPIC, GROUP, 0L, 0L, regionId, 0L);
    return new SubscriptionMessage(ctx, watermarkTs);
  }

  @Test
  public void testSingleRegionRelease() {
    final WatermarkProcessor proc = new WatermarkProcessor(5, 60_000);

    final List<SubscriptionMessage> result =
        proc.process(Collections.singletonList(watermarkMsg(REGION_R1, 1, 1000)));

    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(995, proc.getWatermark());
  }

  @Test
  public void testTwoRegionsMinWatermark() {
    final WatermarkProcessor proc = new WatermarkProcessor(10, 60_000);

    proc.process(Arrays.asList(watermarkMsg(REGION_R1, 1, 2000), watermarkMsg(REGION_R2, 1, 500)));

    Assert.assertEquals(490, proc.getWatermark());
  }

  @Test
  public void testWatermarkAdvancesIdleRegion() {
    final WatermarkProcessor proc = new WatermarkProcessor(5, 60_000);

    proc.process(Arrays.asList(watermarkMsg(REGION_R1, 1, 2000), watermarkMsg(REGION_R2, 1, 500)));
    Assert.assertEquals(495, proc.getWatermark());

    proc.process(Collections.singletonList(watermarkMsg(REGION_R2, 1, 1500)));
    Assert.assertEquals(1495, proc.getWatermark());

    proc.process(Collections.singletonList(watermarkMsg(REGION_R2, 1, 3000)));
    Assert.assertEquals(1995, proc.getWatermark());
  }

  @Test
  public void testWatermarkEventsNotBuffered() {
    final WatermarkProcessor proc = new WatermarkProcessor(5, 60_000);

    proc.process(Collections.singletonList(watermarkMsg(REGION_R1, 1, 1000)));

    Assert.assertEquals(0, proc.getBufferedCount());
  }

  @Test
  public void testFlushReleasesAll() {
    final WatermarkProcessor proc = new WatermarkProcessor(5, 60_000);

    proc.process(Arrays.asList(dataMsg(REGION_R1, 1), dataMsg(REGION_R1, 1)));

    proc.flush();
    Assert.assertEquals(0, proc.getBufferedCount());
  }

  @Test
  public void testWatermarkNoRegression() {
    final WatermarkProcessor proc = new WatermarkProcessor(10, 60_000);

    proc.process(Collections.singletonList(watermarkMsg(REGION_R1, 1, 2000)));
    Assert.assertEquals(1990, proc.getWatermark());

    proc.process(Collections.singletonList(watermarkMsg(REGION_R1, 1, 1500)));
    Assert.assertEquals(1990, proc.getWatermark());
  }

  @Test
  public void testMultipleWatermarksInSingleBatch() {
    final WatermarkProcessor proc = new WatermarkProcessor(0, 60_000);

    proc.process(
        Arrays.asList(
            watermarkMsg(REGION_R1, 1, 100),
            watermarkMsg(REGION_R2, 1, 200),
            watermarkMsg(REGION_R1, 1, 300)));

    Assert.assertEquals(200, proc.getWatermark());
  }

  @Test
  public void testEmptyInput() {
    final WatermarkProcessor proc = new WatermarkProcessor(5, 60_000);

    final List<SubscriptionMessage> result = proc.process(Collections.emptyList());
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(Long.MIN_VALUE, proc.getWatermark());
  }

  @Test
  public void testThreeRegionsSlowestDeterminesWatermark() {
    final WatermarkProcessor proc = new WatermarkProcessor(10, 60_000);

    proc.process(
        Arrays.asList(
            watermarkMsg(REGION_R1, 1, 5000),
            watermarkMsg(REGION_R2, 1, 3000),
            watermarkMsg("R3", 2, 4000)));

    Assert.assertEquals(2990, proc.getWatermark());

    proc.process(Collections.singletonList(watermarkMsg(REGION_R2, 1, 6000)));
    Assert.assertEquals(3990, proc.getWatermark());
  }

  @Test
  public void testZeroOutOfOrderness() {
    final WatermarkProcessor proc = new WatermarkProcessor(0, 60_000);

    proc.process(Collections.singletonList(watermarkMsg(REGION_R1, 1, 1000)));
    Assert.assertEquals(1000, proc.getWatermark());
  }
}
