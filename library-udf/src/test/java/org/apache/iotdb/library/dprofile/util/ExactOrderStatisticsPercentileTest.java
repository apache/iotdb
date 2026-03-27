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

package org.apache.iotdb.library.dprofile.util;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Discrete nearest-rank percentile: index = ceil(n * phi) - 1 clamped to [0, n - 1]. See class
 * Javadoc of {@link ExactOrderStatistics}.
 */
public class ExactOrderStatisticsPercentileTest {

  private static DoubleArrayList doubles(double... values) {
    DoubleArrayList d = new DoubleArrayList();
    for (double v : values) {
      d.add(v);
    }
    return d;
  }

  @Test
  public void oneToFiveRankHalfIsThree() {
    DoubleArrayList d = doubles(1, 2, 3, 4, 5);
    Assert.assertEquals(3.0, ExactOrderStatistics.getPercentile(d, 0.5), 0.0);
  }

  @Test
  public void oneToFiveUnsortedRankHalfIsThree() {
    DoubleArrayList d = doubles(5, 1, 4, 2, 3);
    Assert.assertEquals(3.0, ExactOrderStatistics.getPercentile(d, 0.5), 0.0);
  }

  @Test
  public void oneToFiveRankOneIsMax() {
    DoubleArrayList d = doubles(1, 2, 3, 4, 5);
    Assert.assertEquals(5.0, ExactOrderStatistics.getPercentile(d, 1.0), 0.0);
  }

  @Test
  public void oneToFiveRankSmallPhiTakesSmallest() {
    DoubleArrayList d = doubles(1, 2, 3, 4, 5);
    // ceil(5 * 0.01) - 1 = 0 -> 1
    Assert.assertEquals(1.0, ExactOrderStatistics.getPercentile(d, 0.01), 0.0);
  }

  @Test
  public void oneToFiveRankPointTwoIsFirstOrderStat() {
    DoubleArrayList d = doubles(1, 2, 3, 4, 5);
    // ceil(5 * 0.2) - 1 = ceil(1.0) - 1 = 0 -> 1
    Assert.assertEquals(1.0, ExactOrderStatistics.getPercentile(d, 0.2), 0.0);
  }

  @Test
  public void oneToFiveRankPointFourIsSecondOrderStat() {
    DoubleArrayList d = doubles(1, 2, 3, 4, 5);
    // ceil(5 * 0.4) - 1 = 1 -> 2
    Assert.assertEquals(2.0, ExactOrderStatistics.getPercentile(d, 0.4), 0.0);
  }

  @Test
  public void fourElementsRankHalfIsSecond() {
    DoubleArrayList d = doubles(1, 2, 3, 4);
    // ceil(4 * 0.5) - 1 = 1 -> 2 (discrete; not the arithmetic mean 2.5)
    Assert.assertEquals(2.0, ExactOrderStatistics.getPercentile(d, 0.5), 0.0);
  }
}
