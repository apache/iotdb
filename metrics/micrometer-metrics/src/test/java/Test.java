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

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;

/** @Author stormbroken Create by 2021/07/13 @Version 1.0 */
public class Test {
  public MetricManager metricManager = MetricService.getMetricManager();
  private static final String[] TAGS = {
    "tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8", "tag9", "tag10"
  };

  private long createMeter(Integer meterNumber, String[] tags) {
    long start = System.currentTimeMillis();
    for (int i = 0; i < meterNumber; i++) {
      metricManager.getOrCreateCounter("counter" + i, tags);
    }
    long stop = System.currentTimeMillis();
    return stop - start;
  }

  public static void main(String[] args) throws InterruptedException {
    System.setProperty("METRIC_CONF", "path of yml");
    Test test = new Test();
    Integer number = 1000000;
    Integer tagNumber = 10;
    String[] tags = new String[tagNumber];
    for (int i = 0; i < tags.length; i++) {
      tags[i] = TAGS[i];
    }

    long create = test.createMeter(number, tags);
    long find = test.createMeter(number, tags);

    StringBuilder stringBuilder = new StringBuilder();
    for (String tag : tags) {
      stringBuilder.append(tag + "|");
    }
    System.out.println(
        "In number="
            + number
            + " and tags="
            + stringBuilder.toString()
            + ", create uses "
            + create
            + " ms, find uses "
            + find
            + " ms.");
    //        while (true){
    //            TimeUnit.SECONDS.sleep(1);
    //            Counter count = test.metricManager.getOrCreateCounter("count0", tags);
    //            count.inc();
    //            System.out.println(count.count());
    //        }
  }
}
