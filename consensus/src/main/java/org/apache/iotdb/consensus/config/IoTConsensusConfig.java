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

package org.apache.iotdb.consensus.config;

import java.util.concurrent.TimeUnit;

public class IoTConsensusConfig {

  private final RPC rpc;
  private final Replication replication;

  private IoTConsensusConfig(RPC rpc, Replication replication) {
    this.rpc = rpc;
    this.replication = replication;
  }

  public RPC getRpc() {
    return rpc;
  }

  public Replication getReplication() {
    return replication;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private RPC rpc;
    private Replication replication;

    public IoTConsensusConfig build() {
      return new IoTConsensusConfig(
          rpc != null ? rpc : new RPC.Builder().build(),
          replication != null ? replication : new Replication.Builder().build());
    }

    public Builder setRpc(RPC rpc) {
      this.rpc = rpc;
      return this;
    }

    public Builder setReplication(Replication replication) {
      this.replication = replication;
      return this;
    }
  }

  public static class RPC {

    private final int rpcSelectorThreadNum;
    private final int rpcMinConcurrentClientNum;
    private final int rpcMaxConcurrentClientNum;
    private final int thriftServerAwaitTimeForStopService;
    private final boolean isRpcThriftCompressionEnabled;
    private final int selectorNumOfClientManager;
    private final int connectionTimeoutInMs;
    private final int thriftMaxFrameSize;

    private RPC(
        int rpcSelectorThreadNum,
        int rpcMinConcurrentClientNum,
        int rpcMaxConcurrentClientNum,
        int thriftServerAwaitTimeForStopService,
        boolean isRpcThriftCompressionEnabled,
        int selectorNumOfClientManager,
        int connectionTimeoutInMs,
        int thriftMaxFrameSize) {
      this.rpcSelectorThreadNum = rpcSelectorThreadNum;
      this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
      this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
      this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
      this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
      this.selectorNumOfClientManager = selectorNumOfClientManager;
      this.connectionTimeoutInMs = connectionTimeoutInMs;
      this.thriftMaxFrameSize = thriftMaxFrameSize;
    }

    public int getRpcSelectorThreadNum() {
      return rpcSelectorThreadNum;
    }

    public int getRpcMinConcurrentClientNum() {
      return rpcMinConcurrentClientNum;
    }

    public int getRpcMaxConcurrentClientNum() {
      return rpcMaxConcurrentClientNum;
    }

    public int getThriftServerAwaitTimeForStopService() {
      return thriftServerAwaitTimeForStopService;
    }

    public boolean isRpcThriftCompressionEnabled() {
      return isRpcThriftCompressionEnabled;
    }

    public int getSelectorNumOfClientManager() {
      return selectorNumOfClientManager;
    }

    public int getConnectionTimeoutInMs() {
      return connectionTimeoutInMs;
    }

    public int getThriftMaxFrameSize() {
      return thriftMaxFrameSize;
    }

    public static RPC.Builder newBuilder() {
      return new RPC.Builder();
    }

    public static class Builder {

      private int rpcSelectorThreadNum = 1;
      private int rpcMinConcurrentClientNum = Runtime.getRuntime().availableProcessors();
      private int rpcMaxConcurrentClientNum = 65535;
      private int thriftServerAwaitTimeForStopService = 60;
      private boolean isRpcThriftCompressionEnabled = false;
      private int selectorNumOfClientManager = 1;
      private int connectionTimeoutInMs = (int) TimeUnit.SECONDS.toMillis(20);
      private int thriftMaxFrameSize = 536870912;

      public RPC.Builder setRpcSelectorThreadNum(int rpcSelectorThreadNum) {
        this.rpcSelectorThreadNum = rpcSelectorThreadNum;
        return this;
      }

      public RPC.Builder setRpcMinConcurrentClientNum(int rpcMinConcurrentClientNum) {
        this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
        return this;
      }

      public RPC.Builder setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
        this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
        return this;
      }

      public RPC.Builder setThriftServerAwaitTimeForStopService(
          int thriftServerAwaitTimeForStopService) {
        this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
        return this;
      }

      public RPC.Builder setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
        isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
        return this;
      }

      public RPC.Builder setSelectorNumOfClientManager(int selectorNumOfClientManager) {
        this.selectorNumOfClientManager = selectorNumOfClientManager;
        return this;
      }

      public RPC.Builder setConnectionTimeoutInMs(int connectionTimeoutInMs) {
        this.connectionTimeoutInMs = connectionTimeoutInMs;
        return this;
      }

      public RPC.Builder setThriftMaxFrameSize(int thriftMaxFrameSize) {
        this.thriftMaxFrameSize = thriftMaxFrameSize;
        return this;
      }

      public RPC build() {
        return new RPC(
            rpcSelectorThreadNum,
            rpcMinConcurrentClientNum,
            rpcMaxConcurrentClientNum,
            thriftServerAwaitTimeForStopService,
            isRpcThriftCompressionEnabled,
            selectorNumOfClientManager,
            connectionTimeoutInMs,
            thriftMaxFrameSize);
      }
    }
  }

  public static class Replication {

    private final int maxRequestNumPerBatch;
    private final int maxSizePerBatch;
    private final int maxPendingBatch;
    private final int maxWaitingTimeForAccumulatingBatchInMs;
    private final long basicRetryWaitTimeMs;
    private final long maxRetryWaitTimeMs;
    private final long walThrottleThreshold;
    private final long throttleTimeOutMs;
    private final long checkpointGap;
    private final long allocateMemoryForConsensus;
    private final long allocateMemoryForQueue;

    private Replication(
        int maxRequestNumPerBatch,
        int maxSizePerBatch,
        int maxPendingBatch,
        int maxWaitingTimeForAccumulatingBatchInMs,
        long basicRetryWaitTimeMs,
        long maxRetryWaitTimeMs,
        long walThrottleThreshold,
        long throttleTimeOutMs,
        long checkpointGap,
        long allocateMemoryForConsensus,
        double maxMemoryRatioForQueue) {
      this.maxRequestNumPerBatch = maxRequestNumPerBatch;
      this.maxSizePerBatch = maxSizePerBatch;
      this.maxPendingBatch = maxPendingBatch;
      this.maxWaitingTimeForAccumulatingBatchInMs = maxWaitingTimeForAccumulatingBatchInMs;
      this.basicRetryWaitTimeMs = basicRetryWaitTimeMs;
      this.maxRetryWaitTimeMs = maxRetryWaitTimeMs;
      this.walThrottleThreshold = walThrottleThreshold;
      this.throttleTimeOutMs = throttleTimeOutMs;
      this.checkpointGap = checkpointGap;
      this.allocateMemoryForConsensus = allocateMemoryForConsensus;
      this.allocateMemoryForQueue = (long) (allocateMemoryForConsensus * maxMemoryRatioForQueue);
    }

    public int getMaxRequestNumPerBatch() {
      return maxRequestNumPerBatch;
    }

    public int getMaxSizePerBatch() {
      return maxSizePerBatch;
    }

    public int getMaxPendingBatch() {
      return maxPendingBatch;
    }

    public int getMaxWaitingTimeForAccumulatingBatchInMs() {
      return maxWaitingTimeForAccumulatingBatchInMs;
    }

    public long getBasicRetryWaitTimeMs() {
      return basicRetryWaitTimeMs;
    }

    public long getMaxRetryWaitTimeMs() {
      return maxRetryWaitTimeMs;
    }

    public long getWalThrottleThreshold() {
      return walThrottleThreshold;
    }

    public long getThrottleTimeOutMs() {
      return throttleTimeOutMs;
    }

    public long getCheckpointGap() {
      return checkpointGap;
    }

    public Long getAllocateMemoryForConsensus() {
      return allocateMemoryForConsensus;
    }

    public long getAllocateMemoryForQueue() {
      return allocateMemoryForQueue;
    }

    public static Replication.Builder newBuilder() {
      return new Replication.Builder();
    }

    public static class Builder {

      private int maxRequestNumPerBatch = 30;
      private int maxSizePerBatch = 16 * 1024 * 1024;
      // (IMPORTANT) Value of this variable should be the same with MAX_REQUEST_CACHE_SIZE
      // in DataRegionStateMachine
      private int maxPendingBatch = 5;
      private int maxWaitingTimeForAccumulatingBatchInMs = 500;
      private long basicRetryWaitTimeMs = TimeUnit.MILLISECONDS.toMillis(100);
      private long maxRetryWaitTimeMs = TimeUnit.SECONDS.toMillis(20);
      private long walThrottleThreshold = 50 * 1024 * 1024 * 1024L;
      private long throttleTimeOutMs = TimeUnit.SECONDS.toMillis(30);
      private long checkpointGap = 500;
      private long allocateMemoryForConsensus = Runtime.getRuntime().maxMemory() / 10;
      private double maxMemoryRatioForQueue = 0.6;

      public Replication.Builder setMaxRequestNumPerBatch(int maxRequestNumPerBatch) {
        this.maxRequestNumPerBatch = maxRequestNumPerBatch;
        return this;
      }

      public Builder setMaxSizePerBatch(int maxSizePerBatch) {
        this.maxSizePerBatch = maxSizePerBatch;
        return this;
      }

      public Replication.Builder setMaxPendingBatch(int maxPendingBatch) {
        this.maxPendingBatch = maxPendingBatch;
        return this;
      }

      public Replication.Builder setMaxWaitingTimeForAccumulatingBatchInMs(
          int maxWaitingTimeForAccumulatingBatchInMs) {
        this.maxWaitingTimeForAccumulatingBatchInMs = maxWaitingTimeForAccumulatingBatchInMs;
        return this;
      }

      public Replication.Builder setBasicRetryWaitTimeMs(long basicRetryWaitTimeMs) {
        this.basicRetryWaitTimeMs = basicRetryWaitTimeMs;
        return this;
      }

      public Replication.Builder setMaxRetryWaitTimeMs(long maxRetryWaitTimeMs) {
        this.maxRetryWaitTimeMs = maxRetryWaitTimeMs;
        return this;
      }

      public Replication.Builder setWalThrottleThreshold(long walThrottleThreshold) {
        this.walThrottleThreshold = walThrottleThreshold;
        return this;
      }

      public Replication.Builder setThrottleTimeOutMs(long throttleTimeOutMs) {
        this.throttleTimeOutMs = throttleTimeOutMs;
        return this;
      }

      public Builder setCheckpointGap(long checkpointGap) {
        this.checkpointGap = checkpointGap;
        return this;
      }

      public Replication.Builder setAllocateMemoryForConsensus(long allocateMemoryForConsensus) {
        this.allocateMemoryForConsensus = allocateMemoryForConsensus;
        return this;
      }

      public Builder setMaxMemoryRatioForQueue(double maxMemoryRatioForQueue) {
        this.maxMemoryRatioForQueue = maxMemoryRatioForQueue;
        return this;
      }

      public Replication build() {
        return new Replication(
            maxRequestNumPerBatch,
            maxSizePerBatch,
            maxPendingBatch,
            maxWaitingTimeForAccumulatingBatchInMs,
            basicRetryWaitTimeMs,
            maxRetryWaitTimeMs,
            walThrottleThreshold,
            throttleTimeOutMs,
            checkpointGap,
            allocateMemoryForConsensus,
            maxMemoryRatioForQueue);
      }
    }
  }
}
