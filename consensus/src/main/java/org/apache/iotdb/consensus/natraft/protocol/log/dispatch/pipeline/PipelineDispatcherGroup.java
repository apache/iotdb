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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch.pipeline;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.dynamic.DynamicThreadGroup;
import org.apache.iotdb.consensus.common.Peer;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class PipelineDispatcherGroup implements Comparable<PipelineDispatcherGroup> {

  private static final Logger logger = LoggerFactory.getLogger(PipelineDispatcherGroup.class);
  protected boolean nodeEnabled;
  protected volatile RateLimiter rateLimiter;
  protected final ExecutorService dispatcherThreadPool;
  protected final PipelinedLogDispatcher logDispatcher;
  protected volatile boolean delayed;
  protected DynamicThreadGroup dynamicThreadGroup;
  protected String name;
  private final BlockingQueue<DispatchTask> taskQueue;

  public PipelineDispatcherGroup(
      Peer peer,
      PipelinedLogDispatcher logDispatcher,
      int maxBindingThreadNum,
      int minBindingThreadNum) {
    this.logDispatcher = logDispatcher;
    this.nodeEnabled = true;
    this.rateLimiter = RateLimiter.create(Double.MAX_VALUE);
    this.dispatcherThreadPool = createPool(peer, logDispatcher.getMember().getName());
    this.name = logDispatcher.getMember().getName() + "-" + peer;
    this.dynamicThreadGroup =
        new DynamicThreadGroup(
            name,
            dispatcherThreadPool::submit,
            () -> newDispatcherThread(peer),
            minBindingThreadNum,
            maxBindingThreadNum);
    this.taskQueue = new ArrayBlockingQueue<>(logDispatcher.getConfig().getMaxNumOfLogsInMem());
    init();
  }

  public void close() {
    try {
      dynamicThreadGroup.cancelAll();
      dynamicThreadGroup.join();
    } catch (ExecutionException | InterruptedException e) {
      logger.error("Failed to stop threads in {}", dynamicThreadGroup);
    }
  }

  public void updateRate(double rate) {
    rateLimiter = RateLimiter.create(rate);
    delayed = rate != Double.MAX_VALUE;
    logger.info("{} is delayed: {}", name, delayed);
  }

  ExecutorService createPool(Peer node, String name) {
    return IoTDBThreadPoolFactory.newCachedThreadPool(
        "LogDispatcher-"
            + name
            + "-"
            + node.getEndpoint().getIp()
            + "-"
            + node.getEndpoint().getPort()
            + "-"
            + node.getNodeId());
  }

  protected void init() {
    this.dynamicThreadGroup.init();
  }

  protected PipelineDispatcherThread newDispatcherThread(Peer node) {
    return new PipelineDispatcherThread(logDispatcher, node, taskQueue, this);
  }

  @Override
  public int compareTo(PipelineDispatcherGroup o) {
    return Long.compare(this.getQueueSize(), o.getQueueSize());
  }

  public int getQueueSize() {
    return taskQueue.size();
  }

  public void wakeUp() {
    synchronized (taskQueue) {
      taskQueue.notifyAll();
    }
  }

  public DynamicThreadGroup getDynamicThreadGroup() {
    return dynamicThreadGroup;
  }

  public RateLimiter getRateLimiter() {
    return rateLimiter;
  }

  @Override
  public String toString() {
    return "{"
        + "rate="
        + rateLimiter.getRate()
        + ", delayed="
        + delayed
        + ", queueSize="
        + taskQueue.size()
        + ", dispatcherNum="
        + dynamicThreadGroup.getThreadCnt().get()
        + "}";
  }

  public void add(DispatchTask dispatchTask) {
    try {
      taskQueue.put(dispatchTask);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
