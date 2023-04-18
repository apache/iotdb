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
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;

import org.apache.ratis.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DispatcherGroup {
  private static final Logger logger = LoggerFactory.getLogger(DispatcherGroup.class);
  private final Peer peer;
  private final BlockingQueue<VotingEntry> entryQueue;
  private boolean nodeEnabled;
  private final RateLimiter rateLimiter;
  private final ExecutorService dispatcherThreadPool;
  private final LogDispatcher logDispatcher;
  private final AtomicInteger groupThreadNum = new AtomicInteger();
  private int maxBindingThreadNum;
  private boolean delayed;

  public DispatcherGroup(Peer peer, LogDispatcher logDispatcher, int maxBindingThreadNum) {
    this.logDispatcher = logDispatcher;
    this.peer = peer;
    this.entryQueue = new ArrayBlockingQueue<>(logDispatcher.getConfig().getMaxNumOfLogsInMem());
    this.nodeEnabled = true;
    this.rateLimiter = RateLimiter.create(Double.MAX_VALUE);
    this.maxBindingThreadNum = maxBindingThreadNum;
    this.dispatcherThreadPool = createPool(peer, logDispatcher.getMember().getName());
    for (int i = 0; i < maxBindingThreadNum; i++) {
      addThread();
    }
  }

  public void close() {
    dispatcherThreadPool.shutdownNow();
    boolean closeSucceeded = false;
    try {
      closeSucceeded = dispatcherThreadPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // ignore
    }
    if (!closeSucceeded) {
      logger.warn(
          "Cannot shut down dispatcher pool of {}-{}", logDispatcher.member.getName(), peer);
    }
  }

  public void addThread() {
    int threadNum = groupThreadNum.incrementAndGet();
    if (threadNum <= maxBindingThreadNum) {
      dispatcherThreadPool.submit(newDispatcherThread(peer, entryQueue, rateLimiter));
    } else {
      groupThreadNum.decrementAndGet();
    }
  }

  DispatcherThread newDispatcherThread(
      Peer node, BlockingQueue<VotingEntry> logBlockingQueue, RateLimiter rateLimiter) {
    return new DispatcherThread(logDispatcher, node, logBlockingQueue, rateLimiter, this);
  }

  public void updateRate(double rate) {
    rateLimiter.setRate(rate);
    delayed = rate != Double.MAX_VALUE;
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

  public AtomicInteger getGroupThreadNum() {
    return groupThreadNum;
  }

  public int getMaxBindingThreadNum() {
    return maxBindingThreadNum;
  }

  public boolean isDelayed() {
    return delayed;
  }

  public void setDelayed(boolean delayed) {
    this.delayed = delayed;
  }
}
