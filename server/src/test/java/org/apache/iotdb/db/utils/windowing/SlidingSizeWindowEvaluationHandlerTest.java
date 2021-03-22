package org.apache.iotdb.db.utils.windowing;

import org.apache.iotdb.db.utils.windowing.configuration.SlidingSizeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.db.utils.windowing.handler.SlidingSizeWindowEvaluationHandler;
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

public class SlidingSizeWindowEvaluationHandlerTest {

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

  private void doTest(int windowSize, int slidingStep, int totalPointNumber)
      throws WindowingException {
    final AtomicInteger count = new AtomicInteger(0);
    final ConcurrentHashMap<Integer, Integer> actualTVMap = new ConcurrentHashMap<>();

    SlidingSizeWindowEvaluationHandler handler =
        new SlidingSizeWindowEvaluationHandler(
            new SlidingSizeWindowConfiguration(TSDataType.INT32, windowSize, slidingStep),
            window -> {
              count.incrementAndGet();

              for (int i = 0; i < window.size(); ++i) {
                actualTVMap.put((int) window.getTime(i), window.getInt(i));
              }
            });

    for (int i = 0; i < totalPointNumber; ++i) {
      handler.accept(i, i);
    }

    await()
        .atMost(60, SECONDS)
        .until(
            () ->
                (totalPointNumber < windowSize
                        ? 0
                        : 1 + (totalPointNumber - windowSize) / slidingStep)
                    == count.get());

    final ConcurrentHashMap<Integer, Integer> expectedTVMap = new ConcurrentHashMap<>();
    final int windowCount = count.get();
    for (int i = 0; i < windowCount; ++i) {
      for (int j = 0; j < windowSize; ++j) {
        final int tv = i * slidingStep + j;
        if (totalPointNumber <= tv) {
          break;
        }
        expectedTVMap.put(tv, tv);
      }
    }
    Assert.assertEquals(expectedTVMap, actualTVMap);
  }
}
