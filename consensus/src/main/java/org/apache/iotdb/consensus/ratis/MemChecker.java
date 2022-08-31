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
package org.apache.iotdb.consensus.ratis;

import org.apache.ratis.protocol.Message;

import java.util.concurrent.atomic.AtomicLong;

public class MemChecker {
  private final long maxAllowedDirectBytes;
  private final long maxAllowedHeapBytes;
  private final long maxQueueingRequests;

  private final AtomicLong usedDirectBytes;
  private final AtomicLong usedHeapBytes;
  private final AtomicLong queueingRequests;

  public MemChecker(
      long maxAllowedDirectBytes, long maxAllowedHeapBytes, long maxQueueingRequests) {
    this.maxAllowedDirectBytes = maxAllowedDirectBytes;
    this.maxAllowedHeapBytes = maxAllowedHeapBytes;
    this.maxQueueingRequests = maxQueueingRequests;
    usedDirectBytes = new AtomicLong(0L);
    usedHeapBytes = new AtomicLong(0L);
    queueingRequests = new AtomicLong(0L);
  }

  public boolean tryAcquire(Message requestMessage) {
    if (queueingRequests.getAndAdd(1L) >= maxQueueingRequests) {
      return false;
    }

    long expectDirectUsage = getEstimatedDirectMemoryUsage((RequestMessage) requestMessage);
    if (usedDirectBytes.getAndAdd(expectDirectUsage) > maxAllowedDirectBytes) {
      return false;
    }

    long expectHeapUsage = getEstimatedHeapMemoryUsage((RequestMessage) requestMessage);
    if (usedHeapBytes.getAndAdd(expectHeapUsage) > maxAllowedHeapBytes) {
      return false;
    }

    // permission acquired
    queueingRequests.incrementAndGet();
    usedDirectBytes.addAndGet(expectDirectUsage);
    usedHeapBytes.addAndGet(expectHeapUsage);
    return true;
  }

  public void release(Message requestMessage) {
    long expectDirectUsage = getEstimatedDirectMemoryUsage((RequestMessage) requestMessage);
    long expectHeapUsage = getEstimatedHeapMemoryUsage((RequestMessage) requestMessage);
    queueingRequests.decrementAndGet();
    usedDirectBytes.addAndGet(expectDirectUsage);
    usedHeapBytes.addAndGet(expectHeapUsage);
  }

  /**
   * Ratis direct memory usage estimation.
   *
   * <p>Leader's Extra Ratis uses gRPC to send Log Entries to other follower peers. gRPC in turn
   * uses Netty. For every single follower, Netty will maintain 1 copy of data in direct memory for
   * network transmission. since the data can be GC as soon as it is successfully transmitted, on
   * average every follower will consume 0.5X of original memory
   *
   * <p>TODO optimize Ratis direct memory usage
   */
  private long getEstimatedDirectMemoryUsage(RequestMessage message) {
    return (long) (message.size() * 0.5 * message.getReplicas());
  }

  /**
   * Ratis heap usage estimation. Assumes the request contains 1 MB data to write.
   *
   * <p>(1) Leader's Extra:
   *
   * <p>1. original RPC request: 1 MB.
   *
   * <p>2. IConsensus request message & RaftLog entry: 1 MB.
   *
   * <p>(2) All peers:
   *
   * <p>1. StateMachine MemTable.
   *
   * <p>Since MemTable has its own memory control policy, we only consider leader's extra here. For
   * leader's extra, (1).1 original RPC request can be GC after (1).2 IConsensus is created. So on
   * average, the ratis will consume 1X of original memory.
   */
  private long getEstimatedHeapMemoryUsage(RequestMessage message) {
    return (long) (message.size() * 1.1);
  }

  public long getMaxAllowedDirectBytes() {
    return maxAllowedDirectBytes;
  }

  public long getMaxAllowedHeapBytes() {
    return maxAllowedHeapBytes;
  }

  public long getMaxQueueingRequests() {
    return maxQueueingRequests;
  }

  public AtomicLong getUsedDirectBytes() {
    return usedDirectBytes;
  }

  public AtomicLong getUsedHeapBytes() {
    return usedHeapBytes;
  }

  public AtomicLong getQueueingRequests() {
    return queueingRequests;
  }
}
