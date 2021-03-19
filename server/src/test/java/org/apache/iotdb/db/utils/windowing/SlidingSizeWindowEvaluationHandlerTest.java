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
    doTest(1, 1, 10);
  }

  @Test
  public void test05() throws WindowingException {
    doTest(4, 2, 10);
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
        .atMost(5, SECONDS)
        .until(
            () ->
                (totalPointNumber < windowSize
                        ? 0
                        : 1 + (totalPointNumber - windowSize) / slidingStep)
                    == count.get());
  }
}
