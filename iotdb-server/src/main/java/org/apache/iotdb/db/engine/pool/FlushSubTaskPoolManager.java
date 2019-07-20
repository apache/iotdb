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
package org.apache.iotdb.db.engine.pool;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.exception.ProcessorException;

public class FlushSubTaskPoolManager {

  private static final int EXIT_WAIT_TIME = 60 * 1000;

  private ExecutorService pool;

  private FlushSubTaskPoolManager() {
    this.pool = IoTDBThreadPoolFactory
        .newCachedThreadPool(ThreadName.FLUSH_SUB_TASK_SERVICE.getName());
  }

  public static FlushSubTaskPoolManager getInstance() {
    return FlushSubTaskPoolManager.InstanceHolder.instance;
  }

  /**
   * Block new flush submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end.
   *
   * @param block if set to true, this method will wait for timeOut milliseconds.
   * @param timeout block time out in milliseconds.
   * @throws ProcessorException if timeOut is reached or being interrupted while waiting to exit.
   */
  public void close(boolean block, long timeout) throws ProcessorException {
    pool.shutdown();
    if (block) {
      try {
        if (!pool.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
          throw new ProcessorException("Flush thread pool doesn't exit after "
              + EXIT_WAIT_TIME + " ms");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ProcessorException("Interrupted while waiting flush thread pool to exit. ", e);
      }
    }
  }

  public synchronized Future<?> submit(Runnable task) {
    return pool.submit(task);
  }

  public synchronized <T> Future<T> submit(Callable<T> task) {
    return pool.submit(task);
  }

  public int getActiveCnt() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  private static class InstanceHolder {

    private InstanceHolder() {
      //allowed to do nothing
    }

    private static FlushSubTaskPoolManager instance = new FlushSubTaskPoolManager();
  }

  public int getWaitingTasksNumber() {
    return ((ThreadPoolExecutor) pool).getQueue().size();
  }

  public int getCorePoolSize() {
    return ((ThreadPoolExecutor) pool).getCorePoolSize();
  }
}
