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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ConsensusConfig {

  private final TEndPoint thisNode;
  private final File storageDir;
  private final RatisConfig ratisConfig;
  private final MultiLeaderConfig multiLeaderConfig;

  public ConsensusConfig(
      TEndPoint thisNode,
      File storageDir,
      RatisConfig ratisConfig,
      MultiLeaderConfig multiLeaderConfig) {
    this.thisNode = thisNode;
    this.storageDir = storageDir;
    this.ratisConfig = ratisConfig;
    this.multiLeaderConfig = multiLeaderConfig;
  }

  public TEndPoint getThisNode() {
    return thisNode;
  }

  public File getStorageDir() {
    return storageDir;
  }

  public RatisConfig getRatisConfig() {
    return ratisConfig;
  }

  public MultiLeaderConfig getMultiLeaderConfig() {
    return multiLeaderConfig;
  }

  public static ConsensusConfig.Builder newBuilder() {
    return new ConsensusConfig.Builder();
  }

  public static class Builder {

    private TEndPoint thisNode;
    private File storageDir;
    private RatisConfig ratisConfig;
    private MultiLeaderConfig multiLeaderConfig;

    public ConsensusConfig build() {
      return new ConsensusConfig(
          thisNode,
          storageDir,
          ratisConfig != null ? ratisConfig : RatisConfig.newBuilder().build(),
          multiLeaderConfig != null ? multiLeaderConfig : MultiLeaderConfig.newBuilder().build());
    }

    public Builder setThisNode(TEndPoint thisNode) {
      this.thisNode = thisNode;
      return this;
    }

    public Builder setStorageDir(File storageDir) {
      this.storageDir = storageDir;
      return this;
    }

    public Builder setRatisConfig(RatisConfig ratisConfig) {
      this.ratisConfig = ratisConfig;
      return this;
    }

    public Builder setMultiLeaderConfig(MultiLeaderConfig multiLeaderConfig) {
      this.multiLeaderConfig = multiLeaderConfig;
      return this;
    }
  }

  public static class RatisConfig {

    public static RatisConfig.Builder newBuilder() {
      return new RatisConfig.Builder();
    }

    public static class Builder {
      public RatisConfig build() {
        return new RatisConfig();
      }
    }
  }

  public static class MultiLeaderConfig {

    private final RPC rpc;
    private final Replication replication;

    private MultiLeaderConfig(RPC rpc, Replication replication) {
      this.rpc = rpc;
      this.replication = replication;
    }

    public RPC getRpc() {
      return rpc;
    }

    public Replication getReplication() {
      return replication;
    }

    public static MultiLeaderConfig.Builder newBuilder() {
      return new MultiLeaderConfig.Builder();
    }

    public static class Builder {

      private RPC rpc;
      private Replication replication;

      public MultiLeaderConfig build() {
        return new MultiLeaderConfig(
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
      private final int rpcMaxConcurrentClientNum;
      private final int thriftServerAwaitTimeForStopService;
      private final boolean isRpcThriftCompressionEnabled;
      private final int selectorNumOfClientManager;
      private final int connectionTimeoutInMs;

      public RPC(
          int rpcMaxConcurrentClientNum,
          int thriftServerAwaitTimeForStopService,
          boolean isRpcThriftCompressionEnabled,
          int selectorNumOfClientManager,
          int connectionTimeoutInMs) {
        this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
        this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
        this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
        this.selectorNumOfClientManager = selectorNumOfClientManager;
        this.connectionTimeoutInMs = connectionTimeoutInMs;
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

      public static RPC.Builder newBuilder() {
        return new RPC.Builder();
      }

      public static class Builder {
        private int rpcMaxConcurrentClientNum = 65535;
        private int thriftServerAwaitTimeForStopService = 60;
        private boolean isRpcThriftCompressionEnabled = false;
        private int selectorNumOfClientManager = 1;
        private int connectionTimeoutInMs = (int) TimeUnit.SECONDS.toMillis(20);

        public Builder setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
          this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
          return this;
        }

        public Builder setThriftServerAwaitTimeForStopService(
            int thriftServerAwaitTimeForStopService) {
          this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
          return this;
        }

        public Builder setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
          isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
          return this;
        }

        public Builder setSelectorNumOfClientManager(int selectorNumOfClientManager) {
          this.selectorNumOfClientManager = selectorNumOfClientManager;
          return this;
        }

        public Builder setConnectionTimeoutInMs(int connectionTimeoutInMs) {
          this.connectionTimeoutInMs = connectionTimeoutInMs;
          return this;
        }

        public RPC build() {
          return new RPC(
              rpcMaxConcurrentClientNum,
              thriftServerAwaitTimeForStopService,
              isRpcThriftCompressionEnabled,
              selectorNumOfClientManager,
              connectionTimeoutInMs);
        }
      }
    }

    public static class Replication {
      private final int maxPendingRequestNumPerNode;
      private final int maxRequestPerBatch;
      private final int maxPendingBatch;
      private final int maxWaitingTimeForAccumulatingBatchInMs;
      private final long basicRetryWaitTimeMs;
      private final long maxRetryWaitTimeMs;

      private Replication(
          int maxPendingRequestNumPerNode,
          int maxRequestPerBatch,
          int maxPendingBatch,
          int maxWaitingTimeForAccumulatingBatchInMs,
          long basicRetryWaitTimeMs,
          long maxRetryWaitTimeMs) {
        this.maxPendingRequestNumPerNode = maxPendingRequestNumPerNode;
        this.maxRequestPerBatch = maxRequestPerBatch;
        this.maxPendingBatch = maxPendingBatch;
        this.maxWaitingTimeForAccumulatingBatchInMs = maxWaitingTimeForAccumulatingBatchInMs;
        this.basicRetryWaitTimeMs = basicRetryWaitTimeMs;
        this.maxRetryWaitTimeMs = maxRetryWaitTimeMs;
      }

      public int getMaxPendingRequestNumPerNode() {
        return maxPendingRequestNumPerNode;
      }

      public int getMaxRequestPerBatch() {
        return maxRequestPerBatch;
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

      public static Replication.Builder newBuilder() {
        return new Replication.Builder();
      }

      public static class Builder {
        private int maxPendingRequestNumPerNode = 1000;
        private int maxRequestPerBatch = 100;
        private int maxPendingBatch = 50;
        private int maxWaitingTimeForAccumulatingBatchInMs = 10;
        private long basicRetryWaitTimeMs = TimeUnit.MILLISECONDS.toMillis(100);
        private long maxRetryWaitTimeMs = TimeUnit.SECONDS.toMillis(20);

        public Builder setMaxPendingRequestNumPerNode(int maxPendingRequestNumPerNode) {
          this.maxPendingRequestNumPerNode = maxPendingRequestNumPerNode;
          return this;
        }

        public Builder setMaxRequestPerBatch(int maxRequestPerBatch) {
          this.maxRequestPerBatch = maxRequestPerBatch;
          return this;
        }

        public Builder setMaxPendingBatch(int maxPendingBatch) {
          this.maxPendingBatch = maxPendingBatch;
          return this;
        }

        public Builder setMaxWaitingTimeForAccumulatingBatchInMs(
            int maxWaitingTimeForAccumulatingBatchInMs) {
          this.maxWaitingTimeForAccumulatingBatchInMs = maxWaitingTimeForAccumulatingBatchInMs;
          return this;
        }

        public Builder setBasicRetryWaitTimeMs(long basicRetryWaitTimeMs) {
          this.basicRetryWaitTimeMs = basicRetryWaitTimeMs;
          return this;
        }

        public Builder setMaxRetryWaitTimeMs(long maxRetryWaitTimeMs) {
          this.maxRetryWaitTimeMs = maxRetryWaitTimeMs;
          return this;
        }

        public Replication build() {
          return new Replication(
              maxPendingRequestNumPerNode,
              maxRequestPerBatch,
              maxPendingBatch,
              maxWaitingTimeForAccumulatingBatchInMs,
              basicRetryWaitTimeMs,
              maxRetryWaitTimeMs);
        }
      }
    }
  }
}
