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
package org.apache.iotdb.db.engine.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ProcessorException;

public class MergeManager {

  private ExecutorService pool;
  private int threadCnt;

  private MergeManager() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    this.threadCnt = config.mergeConcurrentThreads;
    pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.MERGE_SERVICE.getName());
  }

  public static MergeManager getInstance() {
    return InstanceHolder.instance;
  }

  /**
   * reopen function.
   *
   * @throws ProcessorException if the pool is not terminated.
   */
  public void reopen() throws ProcessorException {
    if (!pool.isTerminated()) {
      throw new ProcessorException("Merge pool is not terminated!");
    }
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    pool = Executors.newFixedThreadPool(config.mergeConcurrentThreads);
  }

  /**
   * Refuse new merge submits and exit when all RUNNING THREAD in the pool end.
   *
   * @param block if set block to true, this method will wait for timeOut milliseconds to close the
   * merge pool. false, return directly.
   * @param timeOut block time out in milliseconds.
   * @throws ProcessorException if timeOut reach or interrupted while waiting to exit.
   */
  public void forceClose(boolean block, long timeOut) throws ProcessorException {
    pool.shutdownNow();
    if (block) {
      try {
        if (!pool.awaitTermination(timeOut, TimeUnit.MILLISECONDS)) {
          throw new ProcessorException(
              "Merge thread pool doesn't exit after " + timeOut + " ms");
        }
      } catch (InterruptedException e) {
        throw new ProcessorException(
            "Interrupted while waiting merge thread pool to exit. Because " + e.getMessage());
      }
    }
  }

  /**
   * Block new merge submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end.
   *
   * @param block if set to true, this method will wait for timeOut milliseconds. false, return
   * directly. False, return directly.
   * @param timeOut block time out in milliseconds.
   * @throws ProcessorException if timeOut is reached or being interrupted while waiting to exit.
   */
  public void close(boolean block, long timeOut) throws ProcessorException {
    pool.shutdown();
    if (block) {
      try {
        if (!pool.awaitTermination(timeOut, TimeUnit.MILLISECONDS)) {
          throw new ProcessorException(
              "Merge thread pool doesn't exit after " + timeOut + " ms");
        }
      } catch (InterruptedException e) {
        throw new ProcessorException(
            "Interrupted while waiting merge thread pool to exit. Because" + e.getMessage());
      }
    }
  }

  public Future<?> submit(Runnable task) {
    return pool.submit(task);
  }

  public int getActiveCnt() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  public int getThreadCnt() {
    return threadCnt;
  }

  private static class InstanceHolder {

    private static MergeManager instance = new MergeManager();
  }
}
