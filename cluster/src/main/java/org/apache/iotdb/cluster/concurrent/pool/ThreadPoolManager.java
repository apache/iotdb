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
package org.apache.iotdb.cluster.concurrent.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.ProcessorException;

public abstract class ThreadPoolManager {

  ExecutorService pool;

  private void checkInit() {
    if (pool == null) {
      init();
    }
  }

  /**
   * Init pool manager
   */
  public void init(){
    pool = IoTDBThreadPoolFactory.newFixedThreadPool(getThreadPoolSize(), getThreadName());
  }

  /**
   * Block new submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end.
   *
   * @param block if set to true, this method will wait for timeOut milliseconds. false, return
   * directly.
   * @param timeout block time out in milliseconds.
   * @throws ProcessorException if timeOut is reached or being interrupted while waiting to exit.
   */
  public void close(boolean block, long timeout) throws ProcessorException {
    if (pool != null) {
      try {
        pool.shutdown();
        if (block) {
          try {
            if (!pool.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
              throw new ProcessorException(
                  String
                      .format("%s thread pool doesn't exit after %d ms", getManagerName(),
                          timeout));
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessorException(
                String
                    .format("Interrupted while waiting %s thread pool to exit.", getManagerName()),
                e);
          }
        }
      } finally {
        pool = null;
      }
    }
  }

  /**
   * Name of Pool Manager
   */
  public abstract String getManagerName();

  public abstract String getThreadName();

  public abstract int getThreadPoolSize();

  public void execute(Runnable task) {
    checkInit();
    pool.execute(task);
  }

  public Future<?> submit(Runnable task) {
    checkInit();
    return pool.submit(task);
  }

  public int getActiveCnt() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

}
