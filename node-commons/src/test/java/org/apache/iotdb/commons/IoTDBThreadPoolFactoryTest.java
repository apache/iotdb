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
package org.apache.iotdb.commons;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.WrappedRunnable;

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBThreadPoolFactoryTest {

  private final String POOL_NAME = "test";
  private AtomicInteger count;
  private CountDownLatch latch;

  @Before
  public void setUp() throws Exception {
    count = new AtomicInteger(0);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testNewFixedThreadPool() {
    String reason = "(can be ignored in Tests) NewFixedThreadPool";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 4;
    latch = new CountDownLatch(threadCount);
    ExecutorService exec =
        IoTDBThreadPoolFactory.newFixedThreadPool(threadCount / 2, POOL_NAME, handler);
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
  public void testNewSingleThreadExecutor() {
    String reason = "(can be ignored in Tests)NewSingleThreadExecutor";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 2;
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
  public void testNewCachedThreadPool() {
    String reason = "(can be ignored in Tests) NewCachedThreadPool";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 4;
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
    String reason = "(can be ignored in Tests) NewSingleThreadScheduledExecutor";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 2;
    latch = new CountDownLatch(threadCount);
    ScheduledExecutorService exec =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      ScheduledFuture<?> future = exec.schedule(task, 0, TimeUnit.SECONDS);
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
    String reason = "(can be ignored in Tests) NewScheduledThreadPool";
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 4;
    latch = new CountDownLatch(threadCount);
    ScheduledExecutorService exec =
        IoTDBThreadPoolFactory.newScheduledThreadPool(threadCount / 2, POOL_NAME, handler);
    for (int i = 0; i < threadCount; i++) {
      Runnable task = new TestThread(reason);
      ScheduledFuture<?> future = exec.schedule(task, 0, TimeUnit.SECONDS);
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
  public void testCreateJDBCClientThreadPool() {
    String reason = "(can be ignored in Tests) CreateJDBCClientThreadPool";
    TThreadPoolServer.Args args = new Args(null);
    args.maxWorkerThreads = 4;
    args.minWorkerThreads = 2;
    args.stopTimeoutVal = 10;
    args.stopTimeoutUnit = TimeUnit.SECONDS;
    Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
    int threadCount = 4;
    latch = new CountDownLatch(threadCount);
    ExecutorService exec =
        IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(args, POOL_NAME, handler);
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

  class TestThread extends WrappedRunnable {

    private String name;

    public TestThread(String name) {
      this.name = name;
    }

    @Override
    public void runMayThrow() {
      throw new RuntimeException(name);
    }
  }
}
