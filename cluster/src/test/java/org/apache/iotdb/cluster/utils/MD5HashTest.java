package org.apache.iotdb.cluster.utils;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MD5HashTest {

  private MD5Hash function = new MD5Hash();
  private final String str = "root.device.sensor";
  private final int result = 1936903121;
  private CountDownLatch latch;

  @Before
  public void setUp() throws Exception {
    latch = new CountDownLatch(2);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testHash() throws InterruptedException {
    Thread t1 = new TestThread();
    Thread t2 = new TestThread();
    t1.start();
    t2.start();
    latch.await();
  }

  private class TestThread extends Thread {

    @Override
    public void run() {
      for (int i = 0; i < 100000; i++) {
        assertEquals(result, function.hash(str));
      }
      System.out.println("Thread exit");
      latch.countDown();
    }
  }

}
