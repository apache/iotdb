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
package org.apache.iotdb.commons.concurrent.dynamic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class DynamicThreadGroup {

  private static final Logger logger = LoggerFactory.getLogger(DynamicThreadGroup.class);
  private Function<Runnable, Future<?>> poolSubmitter;
  private String name;
  private Supplier<DynamicThread> threadFactory;
  private AtomicInteger threadCnt = new AtomicInteger();
  private int minThreadCnt;
  private int maxThreadCnt;
  private Map<DynamicThread, Future<?>> threadFutureMap = new ConcurrentHashMap<>();

  public DynamicThreadGroup(
      String name,
      Function<Runnable, Future<?>> poolSubmitter,
      Supplier<DynamicThread> threadFactory,
      int minThreadCnt,
      int maxThreadCnt) {
    this.name = name;
    this.poolSubmitter = poolSubmitter;
    this.threadFactory = threadFactory;
    this.minThreadCnt = Math.max(1, minThreadCnt);
    this.maxThreadCnt = Math.max(this.minThreadCnt, maxThreadCnt);
  }

  public void init() {
    int initialThreadNum = (this.minThreadCnt + this.maxThreadCnt) / 2;
    for (int i = 0; i < initialThreadNum; i++) {
      addThread();
    }
    logger.info(
        "A new dynamic thread group: {}, thread number range:[{}, {}]",
        name,
        this.minThreadCnt,
        this.maxThreadCnt);
  }

  /** Add a thread to this group if the number of threads does not reach the maximum. */
  public void addThread() {
    int afterCnt = threadCnt.incrementAndGet();
    if (afterCnt <= maxThreadCnt) {
      DynamicThread dynamicThread = threadFactory.get();
      Future<?> submit = poolSubmitter.apply(dynamicThread);
      threadFutureMap.put(dynamicThread, submit);
    } else {
      threadCnt.decrementAndGet();
    }
  }

  public AtomicInteger getThreadCnt() {
    return threadCnt;
  }

  public int getMinThreadCnt() {
    return minThreadCnt;
  }

  /**
   * Remove a thread from the group when it exits.
   *
   * @param dynamicThread exiting thread
   */
  public void onThreadExit(DynamicThread dynamicThread) {
    threadCnt.decrementAndGet();
    threadFutureMap.remove(dynamicThread);
    logger.info("A dynamic thread exits: {}", dynamicThread);
  }

  public void cancelAll() {
    threadFutureMap.forEach((t, f) -> f.cancel(true));
    threadFutureMap.clear();
    threadCnt.set(0);
  }

  public void join() throws ExecutionException, InterruptedException {
    for (Future<?> future : threadFutureMap.values()) {
      future.get();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
