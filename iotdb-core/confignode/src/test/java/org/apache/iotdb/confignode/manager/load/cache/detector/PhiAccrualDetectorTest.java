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

import org.junit.Assert;
import org.junit.Test;

public class PhiAccrualDetectorTest {

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
}
