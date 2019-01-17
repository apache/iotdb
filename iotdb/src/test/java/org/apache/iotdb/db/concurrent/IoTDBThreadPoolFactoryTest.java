/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBThreadPoolFactoryTest {

  private final String POOL_NAME = "test";
  private AtomicInteger count;
  private CountDownLatch latch;

  @Before
  public void setUp() throws Exception {
    count = new AtomicInteger(0);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testNewFixedThreadPool() throws InterruptedException, ExecutionException {
    String reason = "NewFixedThreadPool";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 5;
    latch = new CountDownLatch(threadCount);
    ExecutorService exec = IoTDBThreadPoolFactory
        .newFixedThreadPool(threadCount, POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      exec.execute(task);
    }
    try {
      latch.await();
      assertEquals(count.get(), threadCount);
    } catch (InterruptedException E) {
      fail();
    }
  }

  @Test
  public void testNewSingleThreadExecutor() throws InterruptedException {
    String reason = "NewSingleThreadExecutor";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 1;
    latch = new CountDownLatch(threadCount);
    ExecutorService exec = IoTDBThreadPoolFactory.newSingleThreadExecutor(POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      exec.execute(task);
    }
    try {
      latch.await();
      assertEquals(count.get(), threadCount);
    } catch (InterruptedException E) {
      fail();
    }
  }

  @Test
  public void testNewCachedThreadPool() throws InterruptedException {
    String reason = "NewCachedThreadPool";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 10;
    latch = new CountDownLatch(threadCount);
    ExecutorService exec = IoTDBThreadPoolFactory.newCachedThreadPool(POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      exec.execute(task);
    }
    try {
      latch.await();
      assertEquals(count.get(), threadCount);
    } catch (InterruptedException E) {
      fail();
    }
  }

  @Test
  public void testNewSingleThreadScheduledExecutor() throws InterruptedException {
    String reason = "NewSingleThreadScheduledExecutor";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 1;
    latch = new CountDownLatch(threadCount);
    ScheduledExecutorService exec = IoTDBThreadPoolFactory
        .newSingleThreadScheduledExecutor(POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      ScheduledFuture<?> future = exec.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
      try {
        future.get();
      } catch (ExecutionException e) {
        assertEquals(reason, e.getCause().getMessage());
        count.addAndGet(1);
        latch.countDown();
      }
    }
    try {
      latch.await();
      assertEquals(count.get(), threadCount);
    } catch (InterruptedException E) {
      fail();
    }
  }

  @Test
  public void testNewScheduledThreadPool() throws InterruptedException {
    String reason = "NewScheduledThreadPool";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 10;
    latch = new CountDownLatch(threadCount);
    ScheduledExecutorService exec = IoTDBThreadPoolFactory
        .newScheduledThreadPool(threadCount, POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      ScheduledFuture<?> future = exec.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
      try {
        future.get();
      } catch (ExecutionException e) {
        assertEquals(reason, e.getCause().getMessage());
        count.addAndGet(1);
        latch.countDown();
      }
    }
    try {
      latch.await();
      assertEquals(count.get(), threadCount);
    } catch (InterruptedException E) {
      fail();
    }
  }

  @Test
  public void testCreateJDBCClientThreadPool() throws InterruptedException {
    String reason = "CreateJDBCClientThreadPool";
    TThreadPoolServer.Args args = new Args(null);
    args.maxWorkerThreads = 100;
    args.minWorkerThreads = 10;
    args.stopTimeoutVal = 10;
    args.stopTimeoutUnit = TimeUnit.SECONDS;
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 50;
    latch = new CountDownLatch(threadCount);
    ExecutorService exec = IoTDBThreadPoolFactory
        .createJDBCClientThreadPool(args, POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      exec.execute(task);
    }
    try {
      latch.await();
      assertEquals(count.get(), threadCount);
    } catch (InterruptedException E) {
      fail();
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
      latch.countDown();
    }
  }

  class TestThread implements Runnable {

    private String name;

    public TestThread(String name) {
      this.name = name;
    }

    @Override
    public void run() {
      throw new RuntimeException(name);
    }

  }
}
