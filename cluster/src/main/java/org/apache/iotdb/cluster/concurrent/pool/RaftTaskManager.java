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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.cluster.concurrent.ThreadName;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.ProcessorException;

/**
 * Manage all raft tasks which are applied to data state machine in thread.
 */
public class RaftTaskManager {


  private ExecutorService pool;
  private int threadCnt;

  private RaftTaskManager() {
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    this.threadCnt = config.getConcurrentRaftTaskThread();
    pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.RAFT_TASK.getName());
  }

  public static RaftTaskManager getInstance() {
    return RaftTaskManager.InstanceHolder.instance;
  }

  /**
   * @throws ProcessorException if the pool is not terminated.
   */
  public void reopen() throws ProcessorException {
    if (!pool.isTerminated()) {
      throw new ProcessorException("Raft task Pool is not terminated!");
    }
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    this.threadCnt = config.getConcurrentRaftTaskThread();
    pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.RAFT_TASK.getName());
  }

  /**
   * Block new raft submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end.
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
              "Raft task thread pool doesn't exit after " + timeOut + " ms");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ProcessorException(
            "Interrupted while waiting raft task thread pool to exit.", e);
      }
    }
  }

  public void execute(Runnable task) {
    pool.execute(task);
  }

  public int getActiveCnt() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  public int getThreadCnt() {
    return threadCnt;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static RaftTaskManager instance = new RaftTaskManager();
  }
}
