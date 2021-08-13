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
package org.apache.iotdb.db.cost.statistic;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceStatTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceStatTest.class);

  @Before
  public void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePerformanceStat(true);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePerformanceStat(false);
  }

  @Test
  public void test() {
    Measurement measurement = Measurement.INSTANCE;
    Operation operation = Operation.EXECUTE_JDBC_BATCH;
    measurement.addOperationLatency(operation, System.currentTimeMillis());
    measurement.addOperationLatency(operation, System.currentTimeMillis() - 8000000);

    long batchOpCnt = measurement.getOperationCnt()[operation.ordinal()];
    Assert.assertEquals(0L, batchOpCnt);
    try {
      measurement.start();
      measurement.startContinuousPrintStatistics();
      measurement.addOperationLatency(operation, System.currentTimeMillis());
      measurement.addOperationLatency(operation, System.currentTimeMillis() - 8000000);
      Thread.currentThread().sleep(1000);
      batchOpCnt = measurement.getOperationCnt()[operation.ordinal()];
      Assert.assertEquals(2L, batchOpCnt);
      measurement.stopPrintStatistic();
      measurement.stopPrintStatistic();
      measurement.stopPrintStatistic();
      LOGGER.info("After stopPrintStatistic!");
      Thread.currentThread().sleep(1000);
      measurement.clearStatisticalState();
      batchOpCnt = measurement.getOperationCnt()[operation.ordinal()];
      Assert.assertEquals(0L, batchOpCnt);
      measurement.startContinuousPrintStatistics();
      LOGGER.info("ReStart!");
      Thread.currentThread().sleep(1000);
      measurement.startContinuousPrintStatistics();
      LOGGER.info("ReStart2!");
      Thread.currentThread().sleep(1000);
      measurement.stopPrintStatistic();
      LOGGER.info("After stopStatistic2!");
    } catch (Exception e) {
      LOGGER.error("find error in stat performance, the message is {}", e.getMessage());
    } finally {
      measurement.stop();
    }
  }

  @Test
  public void testSwitch() {
    Measurement measurement = Measurement.INSTANCE;
    try {
      measurement.start();
      measurement.startStatistics();
      measurement.startStatistics();
      measurement.startContinuousPrintStatistics();
      measurement.stopPrintStatistic();
      measurement.stopStatistic();
      measurement.clearStatisticalState();
      measurement.startPrintStatisticsOnce();
      measurement.startContinuousPrintStatistics();
      measurement.startStatistics();
    } catch (StartupException e) {
      e.printStackTrace();
    } finally {
      measurement.stop();
    }
  }
}
