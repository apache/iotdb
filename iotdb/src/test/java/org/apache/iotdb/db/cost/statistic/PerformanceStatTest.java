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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceStatTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceStatTest.class);

  @Test
  public void test() {
    Measurement measurement = Measurement.INSTANCE;
    Operation operation = Operation.EXECUTE_BATCH;
    measurement.addOperationLatency(operation, System.currentTimeMillis());
    measurement.addOperationLatency(operation,
        System.currentTimeMillis() - 8000000);

    long batchOpCnt = measurement.getOperationCnt()[operation.ordinal()];
    Assert.assertEquals(0L, batchOpCnt);
    try {
      measurement.start();
      measurement.startContinuousStatistics();
      measurement.addOperationLatency(operation, System.currentTimeMillis());
      measurement
          .addOperationLatency(operation, System.currentTimeMillis() - 8000000);
      Thread.currentThread().sleep(1000);
      batchOpCnt = measurement.getOperationCnt()[operation.ordinal()];
      Assert.assertEquals(2L, batchOpCnt);
      measurement.stopStatistic();
      measurement.stopStatistic();
      measurement.stopStatistic();
      LOGGER.info("After stopStatistic!");
      Thread.currentThread().sleep(1000);
      measurement.clearStatisticalState();
      batchOpCnt = measurement.getOperationCnt()[operation.ordinal()];
      Assert.assertEquals(0L, batchOpCnt);
      measurement.startContinuousStatistics();
      LOGGER.info("ReStart!");
      Thread.currentThread().sleep(1000);
      measurement.startContinuousStatistics();
      LOGGER.info("ReStart2!");
      Thread.currentThread().sleep(1000);
      measurement.stopStatistic();
      LOGGER.info("After stopStatistic2!");
    } catch (Exception e) {
      LOGGER.error("find error in stat performance, the message is {}", e.getMessage());
    } finally {
      measurement.stop();
    }
  }
}
