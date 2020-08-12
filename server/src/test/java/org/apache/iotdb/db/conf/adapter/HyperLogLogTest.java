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
package org.apache.iotdb.db.conf.adapter;

import static org.junit.Assert.assertEquals;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;

public class HyperLogLogTest {

  @Test
  public void testStreamLibHll() {
    final int seed = 12345;
    // data on which to calculate distinct count
    Random random = new Random(seed);
    int sampleSize = 10000;
    double[] floor = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0};
    String[] avgErrors = new String[floor.length];
    int testNum = 1000;

    for (double v : floor) {
      double errorSum = 0;
      for (int testIndex = 0; testIndex < testNum; testIndex++) {
        final ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < sampleSize; i++) {
          list.add(random.nextInt((int) (sampleSize * v)));
        }
        Set<Integer> set = new HashSet<>();
        ICardinality card = new HyperLogLog(ActiveTimeSeriesCounter.LOG2M);
        for (int a : list) {
          set.add(a);
          card.offer(a);
        }
        double p = (card.cardinality() - set.size()) / (double) set.size();
        errorSum += Math.abs(p);
      }
      // allow average error rate less than 1%
      assertEquals(0, errorSum / testNum, 0.01);
    }
  }
}
