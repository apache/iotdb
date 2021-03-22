package org.apache.iotdb.db.utils.windowing;

import org.apache.iotdb.db.utils.windowing.configuration.SlidingSizeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.db.utils.windowing.handler.SlidingSizeWindowEvaluationHandler;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class SlidingSizeWindowEvaluationHandlerTest {

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

  //

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

    SlidingSizeWindowEvaluationHandler handler =
        new SlidingSizeWindowEvaluationHandler(
            new SlidingSizeWindowConfiguration(TSDataType.INT32, windowSize, slidingStep),
            window -> count.incrementAndGet());

    for (int i = 0; i < totalPointNumber; ++i) {
      handler.accept(i, i);
    }

    await()
        .atMost(10, SECONDS)
        .until(
            () ->
                (totalPointNumber < windowSize
                        ? 0
                        : 1 + (totalPointNumber - windowSize) / slidingStep)
                    == count.get());
  }
}
