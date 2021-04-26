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
package org.apache.iotdb.db.utils.windowing;

import org.apache.iotdb.db.utils.windowing.configuration.SlidingTimeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.db.utils.windowing.handler.SlidingTimeWindowEvaluationHandler;
import org.apache.iotdb.db.utils.windowing.window.EvictableBatchList;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class SlidingTimeWindowEvaluationHandlerTest {

  @Before
  public void setUp() throws Exception {
    EvictableBatchList.setInternalBatchSize(2);
  }

  @After
  public void tearDown() throws Exception {
    EvictableBatchList.setInternalBatchSize(
        TSFileConfig.ARRAY_CAPACITY_THRESHOLD * TSFileConfig.ARRAY_CAPACITY_THRESHOLD);
  }

  @Test
  public void test00() throws WindowingException {
    doTest(1, 1, 0);
  }

  @Test
  public void test01() throws WindowingException {
    doTest(1, 1, 1);
  }

  @Test
  public void test02() throws WindowingException {
    doTest(1, 1, 2);
  }

  @Test
  public void test03() throws WindowingException {
    doTest(1, 1, 5);
  }

  @Test
  public void test04() throws WindowingException {
    doTest(1, 2, 0);
  }

  @Test
  public void test05() throws WindowingException {
    doTest(1, 2, 1);
  }

  @Test
  public void test06() throws WindowingException {
    doTest(1, 2, 2);
  }

  @Test
  public void test07() throws WindowingException {
    doTest(1, 2, 5);
  }

  @Test
  public void test08() throws WindowingException {
    doTest(7, 2, 5);
  }

  @Test
  public void test09() throws WindowingException {
    doTest(7, 3, 7);
  }

  @Test
  public void test10() throws WindowingException {
    doTest(7, 3, 24);
  }

  @Test
  public void test11() throws WindowingException {
    doTest(7, 10, 75);
  }

  @Test
  public void test12() throws WindowingException {
    doTest(7, 10, 76);
  }

  @Test
  public void test13() throws WindowingException {
    doTest(7, 10, 77);
  }

  @Test
  public void test14() throws WindowingException {
    doTest(7, 7, 75);
  }

  @Test
  public void test15() throws WindowingException {
    doTest(7, 7, 76);
  }

  @Test
  public void test16() throws WindowingException {
    doTest(7, 7, 77);
  }

  @Test
  public void test17() throws WindowingException {
    doTest(7, 33, 77);
  }

  @Test
  public void test18() throws WindowingException {
    doTest(4, 16, 128);
  }

  @Test
  public void test19() throws WindowingException {
    doTest(1, 100, 101);
  }

  private void doTest(long timeInterval, long slidingStep, long totalTime)
      throws WindowingException {
    final AtomicInteger count = new AtomicInteger(0);
    final ConcurrentHashMap<Integer, Integer> actualTVMap = new ConcurrentHashMap<>();

    SlidingTimeWindowEvaluationHandler handler =
        new SlidingTimeWindowEvaluationHandler(
            new SlidingTimeWindowConfiguration(TSDataType.INT32, timeInterval, slidingStep),
            window -> {
              for (int i = 0; i < window.size(); ++i) {
                actualTVMap.put((int) window.getTime(i), window.getInt(i));
              }

              count.incrementAndGet();
            });

    for (int i = 0; i < totalTime; ++i) {
      handler.collect(i, i);

      // the following data points will be ignored
      handler.collect(i, i);
      handler.collect(i - 1, i);
    }

    await()
        .atMost(30, SECONDS)
        .until(
            () ->
                (totalTime < timeInterval
                        ? 0
                        : 1
                            + (totalTime - timeInterval) / slidingStep
                            - ((totalTime - timeInterval) % slidingStep == 0 ? 1 : 0))
                    == count.get());

    final ConcurrentHashMap<Integer, Integer> expectedTVMap = new ConcurrentHashMap<>();
    final int windowCount = count.get();
    collection:
    for (int i = 0; i < windowCount; ++i) {
      for (int j = 0; j < timeInterval; ++j) {
        final int tv = (int) (i * slidingStep + j);
        if (totalTime <= tv) {
          break collection;
        }
        expectedTVMap.put(tv, tv);
      }
    }
    Assert.assertEquals(expectedTVMap, actualTVMap);
  }

  @Test
  public void testWithEmptyWindows() throws WindowingException {
    final AtomicInteger countTotal = new AtomicInteger(0);
    final AtomicInteger countEmpty = new AtomicInteger(0);

    SlidingTimeWindowEvaluationHandler handler =
        new SlidingTimeWindowEvaluationHandler(
            new SlidingTimeWindowConfiguration(TSDataType.INT32, 3, 7),
            window -> {
              Assert.assertTrue(window.size() == 0 || window.size() == 1);

              countTotal.incrementAndGet();

              if (window.size() == 0) {
                countEmpty.incrementAndGet();
              }
            });

    for (int i = 0; i < 10; ++i) {
      handler.collect(21 * i, 21 * i);
    }

    await().atMost(30, SECONDS).until(() -> countTotal.get() == 27 && countEmpty.get() == 18);
  }
}
