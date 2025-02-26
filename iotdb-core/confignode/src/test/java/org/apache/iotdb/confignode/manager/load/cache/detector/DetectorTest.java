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

package org.apache.iotdb.confignode.manager.load.cache.detector;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DetectorTest {

  final long sec = 1_000_000_000L;
  final PhiAccrualDetector phiAccrualDetector =
      new PhiAccrualDetector(35, 10 * sec, sec, (long) (0.2 * sec));
  final FixedDetector fixedDetector = new FixedDetector(20 * sec);

  private double getPhi(long elapsed, double[] intervals, long minStd, long pause) {
    final PhiAccrualDetector.PhiAccrual p =
        new PhiAccrualDetector.PhiAccrual(intervals, elapsed, minStd, pause);
    return p.phi();
  }

  private void assertInRange(double value, double start, double end) {
    Assert.assertTrue(value > start);
    Assert.assertTrue(value < end);
  }

  @Test
  public void testFixedDetector() {
    final long lastHeartbeatTs = System.nanoTime() - 21 * sec;
    final List<AbstractHeartbeatSample> history =
        Collections.singletonList(new NodeHeartbeatSample(lastHeartbeatTs, NodeStatus.Unknown));
    Assert.assertFalse(fixedDetector.isAvailable(history));
  }

  @Test
  public void testPhiCalculation1() {
    /* (min, std, acceptable_pause) = (1000, 200, 0) */
    final double[] heartbeatIntervals = {1000, 1000, 1000, 1000, 1000};
    final long minStd = 200;
    final long pause = 0;

    assertInRange(getPhi(1000, heartbeatIntervals, minStd, pause), 0, 1);
    assertInRange(getPhi(2000, heartbeatIntervals, minStd, pause), 5, 10);
    assertInRange(getPhi(3000, heartbeatIntervals, minStd, pause), 35, 50);
  }

  @Test
  public void testPhiCalculation2() {
    /* (min, std, acceptable_pause) = (1000, 300, 0) */
    /* (min, std, acceptable_pause) = (1000, 300, 5000) */
    final double[] heartbeatIntervals = {1000, 1000, 1000, 1000, 1000};
    final long minStd = 300;
    final long pause = 0;

    assertInRange(getPhi(1000, heartbeatIntervals, minStd, pause), 0, 1);
    assertInRange(getPhi(2000, heartbeatIntervals, minStd, pause), 1, 5);
    assertInRange(getPhi(3000, heartbeatIntervals, minStd, pause), 10, 15);
  }

  @Test
  public void testPhiCalculation3() {
    /* (min, std, acceptable_pause) = (1000, 200, 3000) */
    final double[] heartbeatIntervals = {1000, 1000, 1000, 1000, 1000};
    final long minStd = 200;
    final long pause = 3000;

    assertInRange(getPhi(1000 + pause, heartbeatIntervals, minStd, pause), 0, 1);
    assertInRange(getPhi(2000 + pause, heartbeatIntervals, minStd, pause), 5, 10);
    assertInRange(getPhi(3000 + pause, heartbeatIntervals, minStd, pause), 35, 50);
  }

  @Test
  public void testComparisonQuickFailureDetection() {
    // When a node hasn't responded with interval longer than accepted GC pause
    // Phi Accrual can detect the problem quicker than Fix
    // In this case, the accepted pause is 10s, but we haven't received heartbeats for 13s
    long[] interval = new long[] {sec, sec, sec};
    List<AbstractHeartbeatSample> history = fromInterval(interval, 13 * sec);
    Assert.assertTrue(fixedDetector.isAvailable(history));
    Assert.assertFalse(phiAccrualDetector.isAvailable(history));
  }

  @Test
  public void testFalsePositiveOnExceptionallyLongGCPause() {
    // When the system load is high, we may observe exceptionally long GC pause
    // The first exceptionally long GC pause will be a false positive to Phi
    // the GC pause is 15 (longer than the expected 10s)
    // Phi will report false positive
    long[] interval = new long[] {sec, sec, sec};
    long gcPause = 15 * sec;
    List<AbstractHeartbeatSample> history = fromInterval(interval, gcPause + 2 * sec);
    Assert.assertTrue(fixedDetector.isAvailable(history));
    Assert.assertFalse(phiAccrualDetector.isAvailable(history));
  }

  @Test
  public void testPhiAdaptionToFrequentGCPause() {
    // When the system load is high, we may observe exceptionally long GC pause
    // If the GC pause is very often, Phi can be adaptive to the new environment
    // in this case, there are 3 long GC pause in history
    // when a new GC with 21s pause occurs, Phi takes it normal while Fixed will fail.
    long[] interval =
        new long[] {
          sec,
          sec,
          sec,
          15 * sec,
          (long) (0.1 * sec),
          sec,
          sec,
          sec,
          15 * sec,
          (long) (0.1 * sec),
          sec,
          sec
        };
    List<AbstractHeartbeatSample> history = fromInterval(interval, 21 * sec);
    Assert.assertFalse(fixedDetector.isAvailable(history));
    Assert.assertTrue(phiAccrualDetector.isAvailable(history));
  }

  private List<AbstractHeartbeatSample> fromInterval(long[] interval, long timeElapsed) {
    long now = System.nanoTime();
    long begin = now - timeElapsed;
    List<AbstractHeartbeatSample> sample = new LinkedList<>();
    sample.add(new NodeHeartbeatSample(begin, NodeStatus.Running));
    for (int i = interval.length - 1; i >= 0; i--) {
      begin -= interval[i];
      sample.add(0, new NodeHeartbeatSample(begin, NodeStatus.Running));
    }
    return sample;
  }
}
