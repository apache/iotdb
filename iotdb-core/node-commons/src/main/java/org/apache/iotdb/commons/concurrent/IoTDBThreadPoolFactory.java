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
package org.apache.iotdb.commons.concurrent;

import org.apache.iotdb.commons.concurrent.threadpool.WrappedScheduledExecutorService;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedSingleThreadExecutorService;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedSingleThreadScheduledExecutor;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;

import org.apache.thrift.server.TThreadPoolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to create thread pool which must contain the pool name. Notice that IoTDB
 * project does not allow creating ThreadPool using Executors.newXXX() function to get a threadpool
 * because it is hard to be traced.
 */
public class IoTDBThreadPoolFactory {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBThreadPoolFactory.class);

  private static final String NEW_FIXED_THREAD_POOL_LOGGER_FORMAT =
      "new fixed thread pool: {}, thread number: {}";
  private static final String NEW_SINGLE_THREAD_POOL_LOGGER_FORMAT = "new single thread pool: {}";
  private static final String NEW_CACHED_THREAD_POOL_LOGGER_FORMAT = "new cached thread pool: {}";
  private static final String NEW_SINGLE_SCHEDULED_THREAD_POOL_LOGGER_FORMAT =
      "new single scheduled thread pool: {}";
  private static final String NEW_SCHEDULED_THREAD_POOL_LOGGER_FORMAT =
      "new scheduled thread pool: {}";
  private static final String NEW_SYNCHRONOUS_QUEUE_THREAD_POOL_LOGGER_FORMAT =
      "new SynchronousQueue thread pool: {}";
  private static final String NEW_THREAD_POOL_LOGGER_FORMAT = "new thread pool: {}";

  private IoTDBThreadPoolFactory() {}

  /**
   * see {@link Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName - the name of thread pool
   * @return fixed size thread pool
   */
  public static ExecutorService newFixedThreadPool(int nThreads, String poolName) {
    logger.info(NEW_FIXED_THREAD_POOL_LOGGER_FORMAT, poolName, nThreads);

    return new WrappedThreadPoolExecutor(
        nThreads,
        nThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new IoTThreadFactory(poolName),
        poolName);
  }

  /**
   * see {@link Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName - the name of thread pool
   * @return fixed size thread pool
   */
  public static ExecutorService newFixedThreadPool(
      int nThreads, String poolName, RejectedExecutionHandler handler) {
    logger.info(NEW_FIXED_THREAD_POOL_LOGGER_FORMAT, poolName, nThreads);

    return new WrappedThreadPoolExecutor(
        nThreads,
        nThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new IoTThreadFactory(poolName),
        poolName,
        handler);
  }

  public static ExecutorService newFixedThreadPoolWithDaemonThread(int nThreads, String poolName) {
    logger.info(NEW_FIXED_THREAD_POOL_LOGGER_FORMAT, poolName, nThreads);
    return new WrappedSingleThreadExecutorService(
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new IoTThreadFactory(poolName)),
        poolName);
  }

  public static ExecutorService newFixedThreadPool(
      int nThreads, String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info(NEW_FIXED_THREAD_POOL_LOGGER_FORMAT, poolName, nThreads);
    return new WrappedThreadPoolExecutor(
        nThreads,
        nThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new IoTThreadFactory(poolName, handler),
        poolName);
  }

  /**
   * see {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName the name of thread pool.
   * @return thread pool.
   */
  public static ExecutorService newSingleThreadExecutor(String poolName) {
    logger.info(NEW_SINGLE_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedSingleThreadExecutorService(
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new IoTThreadFactory(poolName)),
        poolName);
  }

  public static ExecutorService newSingleThreadExecutor(
      String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info(NEW_SINGLE_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedSingleThreadExecutorService(
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new IoTThreadFactory(poolName, handler)),
        poolName);
  }

  public static ExecutorService newSingleThreadExecutor(
      String poolName, RejectedExecutionHandler handler) {
    logger.info(NEW_SINGLE_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedSingleThreadExecutorService(
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new IoTThreadFactory(poolName),
            handler),
        poolName);
  }

  /**
   * see {@link Executors#newCachedThreadPool(java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName the name of thread pool.
   * @param corePoolSize the corePoolSize of thread pool
   * @param maximumPoolSize the maximumPoolSize of thread pool
   * @param rejectedExecutionHandler the reject handler
   * @return thread pool.
   */
  public static ExecutorService newCachedThreadPool(
      String poolName,
      int corePoolSize,
      int maximumPoolSize,
      RejectedExecutionHandler rejectedExecutionHandler) {
    logger.info(NEW_CACHED_THREAD_POOL_LOGGER_FORMAT, poolName);
    WrappedThreadPoolExecutor executor =
        new WrappedThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new IoTThreadFactory(poolName),
            poolName);
    executor.setRejectedExecutionHandler(rejectedExecutionHandler);
    return executor;
  }

  /**
   * see {@link Executors#newCachedThreadPool(java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName the name of thread pool.
   * @return thread pool.
   */
  public static ExecutorService newCachedThreadPool(String poolName) {
    logger.info(NEW_CACHED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        60L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new IoTThreadFactory(poolName),
        poolName);
  }

  public static ExecutorService newCachedThreadPool(
      String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info(NEW_CACHED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        60L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new IoTThreadFactory(poolName, handler),
        poolName);
  }

  public static ExecutorService newCachedThreadPoolWithDaemon(String poolName) {
    logger.info(NEW_CACHED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        60L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new IoTDBDaemonThreadFactory(poolName),
        poolName);
  }

  /**
   * see {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName the name of thread pool.
   * @return scheduled thread pool.
   */
  public static ScheduledExecutorService newSingleThreadScheduledExecutor(String poolName) {
    logger.info(NEW_SINGLE_SCHEDULED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedSingleThreadScheduledExecutor(
        new ScheduledThreadPoolExecutor(1, new IoTThreadFactory(poolName)), poolName);
  }

  public static ScheduledExecutorService newSingleThreadScheduledExecutor(
      String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info(NEW_SINGLE_SCHEDULED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedSingleThreadScheduledExecutor(
        new ScheduledThreadPoolExecutor(1, new IoTThreadFactory(poolName, handler)), poolName);
  }

  /**
   * see {@link Executors#newScheduledThreadPool(int, java.util.concurrent.ThreadFactory)}.
   *
   * @param corePoolSize the number of threads to keep in the pool.
   * @param poolName the name of thread pool.
   * @return thread pool.
   */
  public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, String poolName) {
    logger.info(NEW_SCHEDULED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedScheduledExecutorService(
        Executors.newScheduledThreadPool(corePoolSize, new IoTThreadFactory(poolName)), poolName);
  }

  public static ScheduledExecutorService newScheduledThreadPoolWithDaemon(
      int corePoolSize, String poolName) {
    logger.info(NEW_SCHEDULED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedScheduledExecutorService(
        Executors.newScheduledThreadPool(corePoolSize, new IoTDBDaemonThreadFactory(poolName)),
        poolName);
  }

  public static ScheduledExecutorService newScheduledThreadPool(
      int corePoolSize, String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info(NEW_SCHEDULED_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedScheduledExecutorService(
        Executors.newScheduledThreadPool(corePoolSize, new IoTThreadFactory(poolName, handler)),
        poolName);
  }

  public static ExecutorService newThreadPool(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      IoTThreadFactory ioTThreadFactory,
      String poolName) {
    logger.info(NEW_THREAD_POOL_LOGGER_FORMAT, poolName);
    return new WrappedThreadPoolExecutor(
        corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, ioTThreadFactory, poolName);
  }

  /** function for creating thrift rpc client thread pool. */
  public static ExecutorService createThriftRpcClientThreadPool(
      TThreadPoolServer.Args args, String poolName) {
    logger.info(NEW_SYNCHRONOUS_QUEUE_THREAD_POOL_LOGGER_FORMAT, poolName);
    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<>();
    return new WrappedThreadPoolExecutor(
        args.minWorkerThreads,
        args.maxWorkerThreads,
        args.stopTimeoutVal,
        args.stopTimeoutUnit,
        executorQueue,
        new IoTThreadFactory(poolName),
        poolName);
  }

  /** function for creating thrift rpc client thread pool. */
  public static ExecutorService createThriftRpcClientThreadPool(
      int minWorkerThreads,
      int maxWorkerThreads,
      int stopTimeoutVal,
      TimeUnit stopTimeoutUnit,
      String poolName) {
    logger.info(NEW_SYNCHRONOUS_QUEUE_THREAD_POOL_LOGGER_FORMAT, poolName);
    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<>();
    return new WrappedThreadPoolExecutor(
        minWorkerThreads,
        maxWorkerThreads,
        stopTimeoutVal,
        stopTimeoutUnit,
        executorQueue,
        new IoTThreadFactory(poolName),
        poolName);
  }

  /** function for creating thrift rpc client thread pool. */
  public static ExecutorService createThriftRpcClientThreadPool(
      TThreadPoolServer.Args args, String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info(NEW_SYNCHRONOUS_QUEUE_THREAD_POOL_LOGGER_FORMAT, poolName);
    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<>();
    return new WrappedThreadPoolExecutor(
        args.minWorkerThreads,
        args.maxWorkerThreads,
        args.stopTimeoutVal,
        args.stopTimeoutUnit,
        executorQueue,
        new IoTThreadFactory(poolName, handler),
        poolName);
  }
}
