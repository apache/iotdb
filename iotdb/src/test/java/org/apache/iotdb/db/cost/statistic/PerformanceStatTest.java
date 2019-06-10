/**
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
package org.apache.iotdb.db.cost.statistic;

import org.junit.Test;

public class PerformanceStatTest {

  @Test
  public void test() {
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_BATCH, System.currentTimeMillis());
    Measurement.INSTANCE
        .addOperationLatency(Operation.EXECUTE_BATCH, System.currentTimeMillis() - 8000000);

    try {
      Measurement.INSTANCE.start();
      Measurement.INSTANCE.startContinuousStatistics();
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_BATCH, System.currentTimeMillis());
      Measurement.INSTANCE
          .addOperationLatency(Operation.EXECUTE_BATCH, System.currentTimeMillis() - 8000000);
      Thread.currentThread().sleep(2000);
      Measurement.INSTANCE.stopStatistic();
      Measurement.INSTANCE.stopStatistic();
      Measurement.INSTANCE.stopStatistic();
      System.out.println("After stopStatistic!");
      Thread.currentThread().sleep(1000);
      Measurement.INSTANCE.startContinuousStatistics();
      System.out.println("ReStart!");
      Thread.currentThread().sleep(2000);
      Measurement.INSTANCE.startContinuousStatistics();
      System.out.println("ReStart2!");
      Thread.currentThread().sleep(2000);
      Measurement.INSTANCE.stopStatistic();
      System.out.println("After stopStatistic2!");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      Measurement.INSTANCE.stop();
    }
  }
}
