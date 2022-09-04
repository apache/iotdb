/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.micrometer;

public class MicrometerMetricTestPlan {
  private static final Integer[] TAG_NUMBERS = {2, 4, 6, 8, 10};
  private static final Integer[] METRIC_NUMBERS = {1000, 10000, 50000, 100000, 500000, 1000000};
  private static final Integer LOOP = 10;
  private static final Integer tagTotalNumber = 1000;
  private static final Integer searchNumber = 100000;

  private static void test(Integer metric, Integer tag) {
    Long[] times = {0L, 0L, 0L};
    MicrometerMetricTest test = new MicrometerMetricTest(metric, tagTotalNumber, tag, searchNumber);
    times[0] += test.createMetricInorder();
    for (int i = 0; i < LOOP; i++) {
      times[1] += test.searchMetricInorder();
      times[2] += test.searchMetricDisorder();
    }
    test.stop();
    System.out.println(
        metric
            + ","
            + tagTotalNumber
            + ","
            + tag
            + ","
            + searchNumber
            + ","
            + (times[0])
            + ","
            + (times[1] * 1.0 / LOOP)
            + ","
            + (times[2] * 1.0 / LOOP));
  }

  public static void main(String[] args) {
    System.setProperty("IOTDB_CONF", "metrics/micrometer-metrics/src/test/resources");
    for (Integer metric : METRIC_NUMBERS) {
      for (Integer tag : TAG_NUMBERS) {
        test(metric, tag);
      }
    }
  }
}
