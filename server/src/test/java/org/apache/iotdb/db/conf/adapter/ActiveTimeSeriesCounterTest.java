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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ActiveTimeSeriesCounter Tester.
 */
public class ActiveTimeSeriesCounterTest {

  private static final String TEST_SG_PREFIX = "root.sg_";
  private static int testStorageGroupNum = 10;
  private static String[] storageGroups = new String[testStorageGroupNum];
  private static int[] measurementNum = new int[testStorageGroupNum];
  private static double totalSeriesNum = 0;

  static {
    for (int i = 0; i < testStorageGroupNum; i++) {
      storageGroups[i] = TEST_SG_PREFIX + i;
      measurementNum[i] = i + 1;
      totalSeriesNum += measurementNum[i];
    }
  }

  @Before
  public void before() throws Exception {
    ActiveTimeSeriesCounter.clear();
    for (String storageGroup : storageGroups) {
      ActiveTimeSeriesCounter.getInstance().init(storageGroup);
    }
  }

  @After
  public void after() throws Exception {
    for (String storageGroup : storageGroups) {
      ActiveTimeSeriesCounter.getInstance().delete(storageGroup);
    }
  }

  /**
   * Method: init(String storageGroup)
   */
  @Test
  public void testInit() throws Exception {
    for (int i = 0; i < testStorageGroupNum; i++) {
      assertEquals(0D, ActiveTimeSeriesCounter.getInstance().getActiveRatio(storageGroups[i]), 0.0);
    }
  }

  /**
   * Method: updateActiveRatio(String storageGroup)
   */
  @Test
  public void testUpdateActiveRatio() throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(storageGroups.length);
    CountDownLatch finished = new CountDownLatch(storageGroups.length);
    for (int i = 0; i < storageGroups.length; i++) {
      service.submit(new OfferThreads(storageGroups[i], measurementNum[i], finished));
    }
    finished.await();
    for (String storageGroup : storageGroups) {
      ActiveTimeSeriesCounter.getInstance().updateActiveRatio(storageGroup);
      double sum = 0;
      for (String s : storageGroups) {
        sum += ActiveTimeSeriesCounter.getInstance().getActiveRatio(s);
      }
      assertEquals(1.0, sum, 0.001);
    }
    for (int i = 0; i < storageGroups.length; i++) {
      double r = ActiveTimeSeriesCounter.getInstance().getActiveRatio(storageGroups[i]);
      assertEquals(measurementNum[i] / totalSeriesNum, r, 0.001);
    }
  }

  private static class OfferThreads extends WrappedRunnable {
    private int sensorNum;
    private String storageGroup;
    private CountDownLatch finished;

    private OfferThreads(String storageGroup, int sensorNum, CountDownLatch finished) {
      this.sensorNum = sensorNum;
      this.storageGroup = storageGroup;
      this.finished = finished;
    }

    @Override
    public void runMayThrow() {
      try {
        for (int j = 0; j < sensorNum; j++) {
          ActiveTimeSeriesCounter.getInstance().offer(storageGroup, "device_0", "sensor_" + j);
        }
      }finally {
        finished.countDown();
      }
    }
  }


} 
