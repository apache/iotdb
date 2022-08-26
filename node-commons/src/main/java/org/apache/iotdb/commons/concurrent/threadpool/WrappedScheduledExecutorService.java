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

import org.apache.iotdb.commons.concurrent.WrappedCallable;
import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.JMXService;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class WrappedScheduledExecutorService
    implements ScheduledExecutorService, WrappedScheduledExecutorServiceMBean {
  private final String mbeanName;
  ScheduledExecutorService service;

  public WrappedScheduledExecutorService(ScheduledExecutorService service, String mbeanName) {
    this.mbeanName =
        String.format(
            "%s:%s=%s", IoTDBConstant.IOTDB_THREADPOOL_PACKAGE, IoTDBConstant.JMX_TYPE, mbeanName);
    this.service = service;
    JMXService.registerMBean(this, this.mbeanName);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return service.schedule(WrappedRunnable.wrap(command), delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return service.schedule(WrappedCallable.wrap(callable), delay, unit);
  }

  @Override
  @SuppressWarnings("unsafeThreadSchedule")
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    return service.scheduleAtFixedRate(WrappedRunnable.wrap(command), initialDelay, period, unit);
  }

  @Override
  @SuppressWarnings("unsafeThreadSchedule")
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return service.scheduleWithFixedDelay(WrappedRunnable.wrap(command), initialDelay, delay, unit);
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
    return service.submit(WrappedCallable.wrap(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return service.submit(WrappedRunnable.wrap(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return service.submit(WrappedRunnable.wrap(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return service.invokeAll(
        tasks.stream().map(WrappedCallable::wrap).collect(Collectors.toList()));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return service.invokeAll(
        tasks.stream().map(WrappedCallable::wrap).collect(Collectors.toList()), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return service.invokeAny(
        tasks.stream().map(WrappedCallable::wrap).collect(Collectors.toList()));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return service.invokeAny(
        tasks.stream().map(WrappedCallable::wrap).collect(Collectors.toList()), timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    service.execute(WrappedRunnable.wrap(command));
  }

  @Override
  public int getCorePoolSize() {
    return ((ThreadPoolExecutor) service).getCorePoolSize();
  }

  @Override
  public boolean prestartCoreThread() {
    return ((ThreadPoolExecutor) service).prestartCoreThread();
  }

  @Override
  public int getMaximumPoolSize() {
    return ((ThreadPoolExecutor) service).getMaximumPoolSize();
  }

  @Override
  public Queue<Runnable> getQueue() {
    return ((ThreadPoolExecutor) service).getQueue();
  }

  @Override
  public int getPoolSize() {
    return ((ThreadPoolExecutor) service).getPoolSize();
  }

  @Override
  public int getActiveCount() {
    return ((ThreadPoolExecutor) service).getActiveCount();
  }

  @Override
  public int getLargestPoolSize() {
    return ((ThreadPoolExecutor) service).getLargestPoolSize();
  }

  @Override
  public long getTaskCount() {
    return ((ThreadPoolExecutor) service).getTaskCount();
  }

  @Override
  public long getCompletedTaskCount() {
    return ((ThreadPoolExecutor) service).getCompletedTaskCount();
  }

  @Override
  public int getQueueLength() {
    return getQueue().size();
  }
}
