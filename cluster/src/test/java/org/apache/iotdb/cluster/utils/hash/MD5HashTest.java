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
package org.apache.iotdb.cluster.utils.hash;

import static org.junit.Assert.assertEquals;

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
  public void setUp() {
    latch = new CountDownLatch(2);
  }

  @After
  public void tearDown() {
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
