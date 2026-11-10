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
import org.eclipse.collections.impl.list.mutable.primitive.FloatArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.Assert;
import org.junit.Test;

public class ExactOrderStatisticsTest {

  private static final double[] VALUES = {
    2403.71, 2404.36, 2402.88, 2403.95, 2404.12, 2401.63, 2403.52, 2404.77, 2402.34, 2403.18,
    2405.01, 2397.77, 2404.63, 2402.07, 2403.85, 2404.44, 2402.61, 2403.29, 2404.90, 2402.15,
    2403.63, 2404.05, 2402.72, 2403.97
  };
  private static final double EXPECTED_MAD = 0.78;

  @Test
  public void testGetMadDouble() {
    DoubleArrayList nums = new DoubleArrayList(VALUES.length);
    for (double value : VALUES) {
      nums.add(value);
    }
    Assert.assertEquals(EXPECTED_MAD, ExactOrderStatistics.getMad(nums), 1e-9);
  }

  @Test
  public void testGetMadFloat() {
    FloatArrayList nums = new FloatArrayList(VALUES.length);
    for (double value : VALUES) {
      nums.add((float) value);
    }
    Assert.assertEquals(EXPECTED_MAD, ExactOrderStatistics.getMad(nums), 1e-3);
  }

  @Test
  public void testGetMadInt() {
    IntArrayList nums = new IntArrayList(VALUES.length);
    for (int i = 1; i <= VALUES.length; i++) {
      nums.add(i);
    }
    Assert.assertEquals(6.0, ExactOrderStatistics.getMad(nums), 0);
  }

  @Test
  public void testGetMadLong() {
    LongArrayList nums = new LongArrayList(VALUES.length);
    for (long i = 1; i <= VALUES.length; i++) {
      nums.add(i);
    }
    Assert.assertEquals(6.0, ExactOrderStatistics.getMad(nums), 0);
  }
}
