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
package org.apache.iotdb.db.concurrent;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class IoTDBDefaultThreadExceptionHandlerTest {

  private final String message = "Expected!";
  private Thread.UncaughtExceptionHandler handler;
  private AtomicInteger count;

  @Before
  public void setUp() {
    handler = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(new TestExceptionHandler(message));
    count = new AtomicInteger(0);
  }

  @After
  public void tearDown() {
    Thread.setDefaultUncaughtExceptionHandler(handler);
  }

  @Test
  public void test() throws InterruptedException {
    int num = 10;
    for (int i = 0; i < num; i++) {
      TestThread thread = new TestThread();
      thread.start();
    }
    Thread.sleep(500);
    assertEquals(num, count.get());
  }

  class TestThread extends Thread {

    @Override
    public void run() {
      throw new RuntimeException(message);
    }
  }

  class TestExceptionHandler implements Thread.UncaughtExceptionHandler {

    private String name;

    public TestExceptionHandler(String name) {
      this.name = name;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      assertEquals(name, e.getMessage());
      count.addAndGet(1);
    }
  }
}
