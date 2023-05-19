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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.dynamic.DynamicThreadGroup;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;

import org.apache.ratis.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class DispatcherGroup {

  private static final Logger logger = LoggerFactory.getLogger(DispatcherGroup.class);
  private final Peer peer;
  private final BlockingQueue<VotingEntry> entryQueue;
  private boolean nodeEnabled;
  private RateLimiter rateLimiter;
  private final ExecutorService dispatcherThreadPool;
  private final LogDispatcher logDispatcher;
  private boolean delayed;
  private DynamicThreadGroup dynamicThreadGroup;
  private String name;

  public DispatcherGroup(Peer peer, LogDispatcher logDispatcher, int maxBindingThreadNum) {
    this.logDispatcher = logDispatcher;
    this.peer = peer;
    this.entryQueue = new ArrayBlockingQueue<>(logDispatcher.getConfig().getMaxNumOfLogsInMem());
    this.nodeEnabled = true;
    this.rateLimiter = RateLimiter.create(Double.MAX_VALUE);
    this.dispatcherThreadPool = createPool(peer, logDispatcher.getMember().getName());
    this.name = logDispatcher.member.getName() + "-" + peer;
    this.dynamicThreadGroup =
        new DynamicThreadGroup(
            name,
            dispatcherThreadPool::submit,
            () -> newDispatcherThread(peer, entryQueue),
            maxBindingThreadNum / 4,
            maxBindingThreadNum);
    this.dynamicThreadGroup.init();
  }

  public void close() {
    try {
      dynamicThreadGroup.cancelAll();
      dynamicThreadGroup.join();
    } catch (ExecutionException | InterruptedException e) {
      logger.error("Failed to stop threads in {}", dynamicThreadGroup);
    }
  }

  DispatcherThread newDispatcherThread(Peer node, BlockingQueue<VotingEntry> logBlockingQueue) {
    return new DispatcherThread(logDispatcher, node, logBlockingQueue, this);
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

  public int getQueueSize() {
    return entryQueue.size();
  }

  public boolean isNodeEnabled() {
    return nodeEnabled;
  }

  public BlockingQueue<VotingEntry> getEntryQueue() {
    return entryQueue;
  }

  public boolean isDelayed() {
    return delayed;
  }

  public void setDelayed(boolean delayed) {
    this.delayed = delayed;
  }

  public DynamicThreadGroup getDynamicThreadGroup() {
    return dynamicThreadGroup;
  }

  public LogDispatcher getLogDispatcher() {
    return logDispatcher;
  }

  public void wakeUp() {
    synchronized (entryQueue) {
      entryQueue.notifyAll();
    }
  }

  public RateLimiter getRateLimiter() {
    return rateLimiter;
  }
}
