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

package org.apache.iotdb.commons.concurrent.threadpool;

import org.apache.iotdb.commons.concurrent.ThreadPoolMetrics;
import org.apache.iotdb.commons.concurrent.WrappedCallable;
import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.JMXService;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WrappedSingleThreadExecutorService
    implements ExecutorService, WrappedSingleThreadExecutorServiceMBean {
  private final String mbeanName;

  ExecutorService service;
  private final AtomicInteger taskCount = new AtomicInteger(0);
  private final AtomicInteger runCount = new AtomicInteger(0);

  public WrappedSingleThreadExecutorService(ExecutorService service, String mbeanName) {
    this.service = service;
    this.mbeanName =
        String.format(
            "%s:%s=%s", IoTDBConstant.IOTDB_THREADPOOL_PACKAGE, IoTDBConstant.JMX_TYPE, mbeanName);
    JMXService.registerMBean(this, this.mbeanName);
    ThreadPoolMetrics.getInstance().registerThreadPool(this, this.mbeanName);
  }

  @Override
  public void shutdown() {
    service.shutdown();
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public List<Runnable> shutdownNow() {
    JMXService.deregisterMBean(mbeanName);
    return service.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return service.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return service.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return service.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    taskCount.incrementAndGet();
    return service.submit(WrappedCallable.wrapWithCount(task, runCount));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    taskCount.incrementAndGet();
    return service.submit(WrappedRunnable.wrapWithCount(task, runCount), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    taskCount.incrementAndGet();
    return service.submit(WrappedRunnable.wrapWithCount(task, runCount));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    taskCount.addAndGet(tasks.size());
    return service.invokeAll(
        tasks.stream()
            .map(x -> WrappedCallable.wrapWithCount(x, runCount))
            .collect(Collectors.toList()));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    taskCount.addAndGet(tasks.size());
    return service.invokeAll(
        tasks.stream()
            .map(x -> WrappedCallable.wrapWithCount(x, runCount))
            .collect(Collectors.toList()),
        timeout,
        unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    taskCount.addAndGet(tasks.size());
    return service.invokeAny(
        tasks.stream()
            .map(x -> WrappedCallable.wrapWithCount(x, runCount))
            .collect(Collectors.toList()));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    taskCount.addAndGet(tasks.size());
    return service.invokeAny(
        tasks.stream()
            .map(x -> WrappedCallable.wrapWithCount(x, runCount))
            .collect(Collectors.toList()),
        timeout,
        unit);
  }

  @Override
  public void execute(Runnable command) {
    taskCount.incrementAndGet();
    service.execute(WrappedRunnable.wrapWithCount(command, runCount));
  }

  @Override
  public int getCorePoolSize() {
    return 1;
  }

  @Override
  public boolean prestartCoreThread() {
    return false;
  }

  @Override
  public int getMaximumPoolSize() {
    return 1;
  }

  @Override
  public Queue<Runnable> getQueue() {
    return new LinkedList<>();
  }

  @Override
  public int getQueueLength() {
    return taskCount.get() - runCount.get();
  }

  @Override
  public int getPoolSize() {
    return 1;
  }

  @Override
  public int getActiveCount() {
    return taskCount.get() - runCount.get() > 0 ? 1 : 0;
  }

  @Override
  public int getLargestPoolSize() {
    return 1;
  }

  @Override
  public long getTaskCount() {
    return taskCount.get();
  }

  @Override
  public long getCompletedTaskCount() {
    return runCount.get();
  }
}
