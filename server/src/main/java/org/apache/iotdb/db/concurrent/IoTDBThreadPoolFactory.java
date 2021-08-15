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

import org.apache.thrift.server.TThreadPoolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

/** This class is used to create thread pool which must contain the pool name. */
public class IoTDBThreadPoolFactory {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBThreadPoolFactory.class);

  private IoTDBThreadPoolFactory() {}

  /**
   * see {@link Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName - the name of thread pool
   * @return fixed size thread pool
   */
  public static ExecutorService newFixedThreadPool(int nthreads, String poolName) {
    logger.info("new fixed thread pool: {}, thread number: {}", poolName, nthreads);
    return Executors.newFixedThreadPool(nthreads, new IoTThreadFactory(poolName));
  }

  public static ExecutorService newFixedThreadPool(
      int nthreads, String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info("new fixed thread pool: {}, thread number: {}", poolName, nthreads);
    return Executors.newFixedThreadPool(nthreads, new IoTThreadFactory(poolName, handler));
  }

  /**
   * see {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName the name of thread pool.
   * @return thread pool.
   */
  public static ExecutorService newSingleThreadExecutor(String poolName) {
    logger.info("new single thread pool: {}", poolName);
    return Executors.newSingleThreadExecutor(new IoTThreadFactory(poolName));
  }

  public static ExecutorService newSingleThreadExecutor(
      String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info("new single thread pool: {}", poolName);
    return Executors.newSingleThreadExecutor(new IoTThreadFactory(poolName, handler));
  }

  /**
   * see {@link Executors#newCachedThreadPool(java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName the name of thread pool.
   * @return thread pool.
   */
  public static ExecutorService newCachedThreadPool(String poolName) {
    logger.info("new cached thread pool: {}", poolName);
    return Executors.newCachedThreadPool(new IoTThreadFactory(poolName));
  }

  public static ExecutorService newCachedThreadPool(
      String poolName, Thread.UncaughtExceptionHandler handler) {
    logger.info("new cached thread pool: {}", poolName);
    return Executors.newCachedThreadPool(new IoTThreadFactory(poolName, handler));
  }

  /**
   * see {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}.
   *
   * @param poolName the name of thread pool.
   * @return scheduled thread pool.
   */
  public static ScheduledExecutorService newSingleThreadScheduledExecutor(String poolName) {
    return Executors.newSingleThreadScheduledExecutor(new IoTThreadFactory(poolName));
  }

  public static ScheduledExecutorService newSingleThreadScheduledExecutor(
      String poolName, Thread.UncaughtExceptionHandler handler) {
    return Executors.newSingleThreadScheduledExecutor(new IoTThreadFactory(poolName, handler));
  }

  /**
   * see {@link Executors#newScheduledThreadPool(int, java.util.concurrent.ThreadFactory)}.
   *
   * @param corePoolSize the number of threads to keep in the pool.
   * @param poolName the name of thread pool.
   * @return thread pool.
   */
  public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, String poolName) {
    return Executors.newScheduledThreadPool(corePoolSize, new IoTThreadFactory(poolName));
  }

  public static ScheduledExecutorService newScheduledThreadPool(
      int corePoolSize, String poolName, Thread.UncaughtExceptionHandler handler) {
    return Executors.newScheduledThreadPool(corePoolSize, new IoTThreadFactory(poolName, handler));
  }

  /** function for creating thrift rpc client thread pool. */
  public static ExecutorService createThriftRpcClientThreadPool(
      TThreadPoolServer.Args args, String poolName) {
    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<>();
    return new ThreadPoolExecutor(
        args.minWorkerThreads,
        args.maxWorkerThreads,
        args.stopTimeoutVal,
        args.stopTimeoutUnit,
        executorQueue,
        new IoTThreadFactory(poolName));
  }

  /** function for creating thrift rpc client thread pool. */
  public static ExecutorService createThriftRpcClientThreadPool(
      TThreadPoolServer.Args args, String poolName, Thread.UncaughtExceptionHandler handler) {
    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<>();
    return new ThreadPoolExecutor(
        args.minWorkerThreads,
        args.maxWorkerThreads,
        args.stopTimeoutVal,
        args.stopTimeoutUnit,
        executorQueue,
        new IoTThreadFactory(poolName, handler));
  }
}
