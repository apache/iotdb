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

import org.apache.ratis.grpc.GrpcConfigKeys.Server;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ConsensusConfig {

  private final TEndPoint thisNode;
  private final File storageDir;
  private final RatisConfig ratisConfig;
  private final MultiLeaderConfig multiLeaderConfig;

  private ConsensusConfig(
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

    private final Rpc rpc;
    private final LeaderElection leaderElection;
    private final Snapshot snapshot;
    private final ThreadPool threadPool;
    private final Log log;
    private final Grpc grpc;

    private RatisConfig(
        Rpc rpc,
        LeaderElection leaderElection,
        Snapshot snapshot,
        ThreadPool threadPool,
        Log log,
        Grpc grpc) {
      this.rpc = rpc;
      this.leaderElection = leaderElection;
      this.snapshot = snapshot;
      this.threadPool = threadPool;
      this.log = log;
      this.grpc = grpc;
    }

    public Rpc getRpc() {
      return rpc;
    }

    public LeaderElection getLeaderElection() {
      return leaderElection;
    }

    public Snapshot getSnapshot() {
      return snapshot;
    }

    public ThreadPool getThreadPool() {
      return threadPool;
    }

    public Log getLog() {
      return log;
    }

    public Grpc getGrpc() {
      return grpc;
    }

    public static RatisConfig.Builder newBuilder() {
      return new RatisConfig.Builder();
    }

    public static class Builder {
      private Rpc rpc;
      private LeaderElection leaderElection;
      private Snapshot snapshot;
      private ThreadPool threadPool;
      private Log log;
      private Grpc grpc;

      public RatisConfig build() {
        return new RatisConfig(
            rpc != null ? rpc : Rpc.newBuilder().build(),
            leaderElection != null ? leaderElection : LeaderElection.newBuilder().build(),
            snapshot != null ? snapshot : Snapshot.newBuilder().build(),
            threadPool != null ? threadPool : ThreadPool.newBuilder().build(),
            log != null ? log : Log.newBuilder().build(),
            grpc != null ? grpc : Grpc.newBuilder().build());
      }

      public Builder setRpc(Rpc rpc) {
        this.rpc = rpc;
        return this;
      }

      public Builder setLeaderElection(LeaderElection leaderElection) {
        this.leaderElection = leaderElection;
        return this;
      }

      public Builder setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
      }

      public Builder setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
        return this;
      }

      public Builder setLog(Log log) {
        this.log = log;
        return this;
      }

      public Builder setGrpc(Grpc grpc) {
        this.grpc = grpc;
        return this;
      }
    }

    /** server rpc timeout related */
    public static class Rpc {
      private final TimeDuration timeoutMin;
      private final TimeDuration timeoutMax;
      private final TimeDuration requestTimeout;
      private final TimeDuration sleepTime;
      private final TimeDuration slownessTimeout;

      private Rpc(
          TimeDuration timeoutMin,
          TimeDuration timeoutMax,
          TimeDuration requestTimeout,
          TimeDuration sleepTime,
          TimeDuration slownessTimeout) {
        this.timeoutMin = timeoutMin;
        this.timeoutMax = timeoutMax;
        this.requestTimeout = requestTimeout;
        this.sleepTime = sleepTime;
        this.slownessTimeout = slownessTimeout;
      }

      public TimeDuration getTimeoutMin() {
        return timeoutMin;
      }

      public TimeDuration getTimeoutMax() {
        return timeoutMax;
      }

      public TimeDuration getRequestTimeout() {
        return requestTimeout;
      }

      public TimeDuration getSleepTime() {
        return sleepTime;
      }

      public TimeDuration getSlownessTimeout() {
        return slownessTimeout;
      }

      public static Rpc.Builder newBuilder() {
        return new Rpc.Builder();
      }

      public static class Builder {
        private TimeDuration timeoutMin = TimeDuration.valueOf(2, TimeUnit.SECONDS);
        private TimeDuration timeoutMax = TimeDuration.valueOf(8, TimeUnit.SECONDS);
        private TimeDuration requestTimeout = TimeDuration.valueOf(20, TimeUnit.SECONDS);
        private TimeDuration sleepTime = TimeDuration.valueOf(1, TimeUnit.SECONDS);
        private TimeDuration slownessTimeout = TimeDuration.valueOf(10, TimeUnit.MINUTES);

        public Rpc build() {
          return new Rpc(timeoutMin, timeoutMax, requestTimeout, sleepTime, slownessTimeout);
        }

        public Builder setTimeoutMin(TimeDuration timeoutMin) {
          this.timeoutMin = timeoutMin;
          return this;
        }

        public Builder setTimeoutMax(TimeDuration timeoutMax) {
          this.timeoutMax = timeoutMax;
          return this;
        }

        public Builder setRequestTimeout(TimeDuration requestTimeout) {
          this.requestTimeout = requestTimeout;
          return this;
        }

        public Builder setSleepTime(TimeDuration sleepTime) {
          this.sleepTime = sleepTime;
          return this;
        }

        public Builder setSlownessTimeout(TimeDuration slownessTimeout) {
          this.slownessTimeout = slownessTimeout;
          return this;
        }
      }
    }

    public static class LeaderElection {
      private final TimeDuration leaderStepDownWaitTimeKey;
      private final boolean preVote;

      private LeaderElection(TimeDuration leaderStepDownWaitTimeKey, boolean preVote) {
        this.leaderStepDownWaitTimeKey = leaderStepDownWaitTimeKey;
        this.preVote = preVote;
      }

      public TimeDuration getLeaderStepDownWaitTimeKey() {
        return leaderStepDownWaitTimeKey;
      }

      public boolean isPreVote() {
        return preVote;
      }

      public static LeaderElection.Builder newBuilder() {
        return new LeaderElection.Builder();
      }

      public static class Builder {
        private TimeDuration leaderStepDownWaitTimeKey = TimeDuration.valueOf(30, TimeUnit.SECONDS);
        private boolean preVote = RaftServerConfigKeys.LeaderElection.PRE_VOTE_DEFAULT;

        public LeaderElection build() {
          return new LeaderElection(leaderStepDownWaitTimeKey, preVote);
        }

        public Builder setLeaderStepDownWaitTimeKey(TimeDuration leaderStepDownWaitTimeKey) {
          this.leaderStepDownWaitTimeKey = leaderStepDownWaitTimeKey;
          return this;
        }

        public Builder setPreVote(boolean preVote) {
          this.preVote = preVote;
          return this;
        }
      }
    }

    public static class Snapshot {
      private final boolean autoTriggerEnabled;
      private final long creationGap;
      private final long autoTriggerThreshold;
      private final int retentionFileNum;

      private Snapshot(
          boolean autoTriggerEnabled,
          long creationGap,
          long autoTriggerThreshold,
          int retentionFileNum) {
        this.autoTriggerEnabled = autoTriggerEnabled;
        this.creationGap = creationGap;
        this.autoTriggerThreshold = autoTriggerThreshold;
        this.retentionFileNum = retentionFileNum;
      }

      public boolean isAutoTriggerEnabled() {
        return autoTriggerEnabled;
      }

      public long getCreationGap() {
        return creationGap;
      }

      public long getAutoTriggerThreshold() {
        return autoTriggerThreshold;
      }

      public int getRetentionFileNum() {
        return retentionFileNum;
      }

      public static Snapshot.Builder newBuilder() {
        return new Snapshot.Builder();
      }

      public static class Builder {
        private boolean autoTriggerEnabled = true;
        private long creationGap = RaftServerConfigKeys.Snapshot.CREATION_GAP_DEFAULT;
        private long autoTriggerThreshold =
            RaftServerConfigKeys.Snapshot.AUTO_TRIGGER_THRESHOLD_DEFAULT;
        private int retentionFileNum = RaftServerConfigKeys.Snapshot.RETENTION_FILE_NUM_DEFAULT;

        public Snapshot build() {
          return new Snapshot(
              autoTriggerEnabled, creationGap, autoTriggerThreshold, retentionFileNum);
        }

        public Builder setAutoTriggerEnabled(boolean autoTriggerEnabled) {
          this.autoTriggerEnabled = autoTriggerEnabled;
          return this;
        }

        public Builder setCreationGap(long creationGap) {
          this.creationGap = creationGap;
          return this;
        }

        public Builder setAutoTriggerThreshold(long autoTriggerThreshold) {
          this.autoTriggerThreshold = autoTriggerThreshold;
          return this;
        }

        public Builder setRetentionFileNum(int retentionFileNum) {
          this.retentionFileNum = retentionFileNum;
          return this;
        }
      }
    }

    public static class ThreadPool {
      private final boolean proxyCached;
      private final int proxySize;
      private final boolean serverCached;
      private final int serverSize;
      private final boolean clientCached;
      private final int clientSize;

      private ThreadPool(
          boolean proxyCached,
          int proxySize,
          boolean serverCached,
          int serverSize,
          boolean clientCached,
          int clientSize) {
        this.proxyCached = proxyCached;
        this.proxySize = proxySize;
        this.serverCached = serverCached;
        this.serverSize = serverSize;
        this.clientCached = clientCached;
        this.clientSize = clientSize;
      }

      public boolean isProxyCached() {
        return proxyCached;
      }

      public int getProxySize() {
        return proxySize;
      }

      public boolean isServerCached() {
        return serverCached;
      }

      public int getServerSize() {
        return serverSize;
      }

      public boolean isClientCached() {
        return clientCached;
      }

      public int getClientSize() {
        return clientSize;
      }

      public static ThreadPool.Builder newBuilder() {
        return new ThreadPool.Builder();
      }

      public static class Builder {
        private boolean proxyCached = RaftServerConfigKeys.ThreadPool.PROXY_CACHED_DEFAULT;
        private int proxySize = RaftServerConfigKeys.ThreadPool.PROXY_SIZE_DEFAULT;
        private boolean serverCached = RaftServerConfigKeys.ThreadPool.SERVER_CACHED_DEFAULT;
        private int serverSize = RaftServerConfigKeys.ThreadPool.SERVER_SIZE_DEFAULT;
        private boolean clientCached = RaftServerConfigKeys.ThreadPool.CLIENT_CACHED_DEFAULT;
        private int clientSize = RaftServerConfigKeys.ThreadPool.CLIENT_SIZE_DEFAULT;

        public ThreadPool build() {
          return new ThreadPool(
              proxyCached, proxySize, serverCached, serverSize, clientCached, clientSize);
        }

        public Builder setProxyCached(boolean proxyCached) {
          this.proxyCached = proxyCached;
          return this;
        }

        public Builder setProxySize(int proxySize) {
          this.proxySize = proxySize;
          return this;
        }

        public Builder setServerCached(boolean serverCached) {
          this.serverCached = serverCached;
          return this;
        }

        public Builder setServerSize(int serverSize) {
          this.serverSize = serverSize;
          return this;
        }

        public Builder setClientCached(boolean clientCached) {
          this.clientCached = clientCached;
          return this;
        }

        public Builder setClientSize(int clientSize) {
          this.clientSize = clientSize;
          return this;
        }
      }
    }

    public static class Log {

      private final boolean useMemory;
      private final int queueElementLimit;
      private final SizeInBytes queueByteLimit;
      private final int purgeGap;
      private final boolean purgeUptoSnapshotIndex;
      private final SizeInBytes segmentSizeMax;
      private final int segmentCacheNumMax;
      private final SizeInBytes segmentCacheSizeMax;
      private final SizeInBytes preallocatedSize;
      private final SizeInBytes writeBufferSize;
      private final int forceSyncNum;
      private final boolean unsafeFlushEnabled;

      private Log(
          boolean useMemory,
          int queueElementLimit,
          SizeInBytes queueByteLimit,
          int purgeGap,
          boolean purgeUptoSnapshotIndex,
          SizeInBytes segmentSizeMax,
          int segmentCacheNumMax,
          SizeInBytes segmentCacheSizeMax,
          SizeInBytes preallocatedSize,
          SizeInBytes writeBufferSize,
          int forceSyncNum,
          boolean unsafeFlushEnabled) {
        this.useMemory = useMemory;
        this.queueElementLimit = queueElementLimit;
        this.queueByteLimit = queueByteLimit;
        this.purgeGap = purgeGap;
        this.purgeUptoSnapshotIndex = purgeUptoSnapshotIndex;
        this.segmentSizeMax = segmentSizeMax;
        this.segmentCacheNumMax = segmentCacheNumMax;
        this.segmentCacheSizeMax = segmentCacheSizeMax;
        this.preallocatedSize = preallocatedSize;
        this.writeBufferSize = writeBufferSize;
        this.forceSyncNum = forceSyncNum;
        this.unsafeFlushEnabled = unsafeFlushEnabled;
      }

      public boolean isUseMemory() {
        return useMemory;
      }

      public int getQueueElementLimit() {
        return queueElementLimit;
      }

      public SizeInBytes getQueueByteLimit() {
        return queueByteLimit;
      }

      public int getPurgeGap() {
        return purgeGap;
      }

      public boolean isPurgeUptoSnapshotIndex() {
        return purgeUptoSnapshotIndex;
      }

      public SizeInBytes getSegmentSizeMax() {
        return segmentSizeMax;
      }

      public int getSegmentCacheNumMax() {
        return segmentCacheNumMax;
      }

      public SizeInBytes getSegmentCacheSizeMax() {
        return segmentCacheSizeMax;
      }

      public SizeInBytes getPreallocatedSize() {
        return preallocatedSize;
      }

      public SizeInBytes getWriteBufferSize() {
        return writeBufferSize;
      }

      public int getForceSyncNum() {
        return forceSyncNum;
      }

      public boolean isUnsafeFlushEnabled() {
        return unsafeFlushEnabled;
      }

      public static Log.Builder newBuilder() {
        return new Log.Builder();
      }

      public static class Builder {
        private boolean useMemory = false;
        private int queueElementLimit = 4096;
        private SizeInBytes queueByteLimit = SizeInBytes.valueOf("64MB");
        private int purgeGap = 1024;
        private boolean purgeUptoSnapshotIndex = false;
        private SizeInBytes segmentSizeMax = SizeInBytes.valueOf("8MB");
        private int segmentCacheNumMax = 6;
        private SizeInBytes segmentCacheSizeMax = SizeInBytes.valueOf("200MB");
        private SizeInBytes preallocatedSize = SizeInBytes.valueOf("4MB");
        private SizeInBytes writeBufferSize = SizeInBytes.valueOf("64KB");
        private int forceSyncNum = 128;
        private boolean unsafeFlushEnabled = false;

        public Log build() {
          return new Log(
              useMemory,
              queueElementLimit,
              queueByteLimit,
              purgeGap,
              purgeUptoSnapshotIndex,
              segmentSizeMax,
              segmentCacheNumMax,
              segmentCacheSizeMax,
              preallocatedSize,
              writeBufferSize,
              forceSyncNum,
              unsafeFlushEnabled);
        }

        public Builder setUseMemory(boolean useMemory) {
          this.useMemory = useMemory;
          return this;
        }

        public Builder setQueueElementLimit(int queueElementLimit) {
          this.queueElementLimit = queueElementLimit;
          return this;
        }

        public Builder setQueueByteLimit(SizeInBytes queueByteLimit) {
          this.queueByteLimit = queueByteLimit;
          return this;
        }

        public Builder setPurgeGap(int purgeGap) {
          this.purgeGap = purgeGap;
          return this;
        }

        public Builder setPurgeUptoSnapshotIndex(boolean purgeUptoSnapshotIndex) {
          this.purgeUptoSnapshotIndex = purgeUptoSnapshotIndex;
          return this;
        }

        public Builder setSegmentSizeMax(SizeInBytes segmentSizeMax) {
          this.segmentSizeMax = segmentSizeMax;
          return this;
        }

        public Builder setSegmentCacheNumMax(int segmentCacheNumMax) {
          this.segmentCacheNumMax = segmentCacheNumMax;
          return this;
        }

        public Builder setSegmentCacheSizeMax(SizeInBytes segmentCacheSizeMax) {
          this.segmentCacheSizeMax = segmentCacheSizeMax;
          return this;
        }

        public Builder setPreallocatedSize(SizeInBytes preallocatedSize) {
          this.preallocatedSize = preallocatedSize;
          return this;
        }

        public Builder setWriteBufferSize(SizeInBytes writeBufferSize) {
          this.writeBufferSize = writeBufferSize;
          return this;
        }

        public Builder setForceSyncNum(int forceSyncNum) {
          this.forceSyncNum = forceSyncNum;
          return this;
        }

        public Builder setUnsafeFlushEnabled(boolean unsafeFlushEnabled) {
          this.unsafeFlushEnabled = unsafeFlushEnabled;
          return this;
        }
      }
    }

    public static class Grpc {
      private final SizeInBytes messageSizeMax;
      private final SizeInBytes flowControlWindow;
      private final boolean asyncRequestThreadPoolCached;
      private final int asyncRequestThreadPoolSize;
      private final int leaderOutstandingAppendsMax;

      private Grpc(
          SizeInBytes messageSizeMax,
          SizeInBytes flowControlWindow,
          boolean asyncRequestThreadPoolCached,
          int asyncRequestThreadPoolSize,
          int leaderOutstandingAppendsMax) {
        this.messageSizeMax = messageSizeMax;
        this.flowControlWindow = flowControlWindow;
        this.asyncRequestThreadPoolCached = asyncRequestThreadPoolCached;
        this.asyncRequestThreadPoolSize = asyncRequestThreadPoolSize;
        this.leaderOutstandingAppendsMax = leaderOutstandingAppendsMax;
      }

      public SizeInBytes getMessageSizeMax() {
        return messageSizeMax;
      }

      public SizeInBytes getFlowControlWindow() {
        return flowControlWindow;
      }

      public boolean isAsyncRequestThreadPoolCached() {
        return asyncRequestThreadPoolCached;
      }

      public int getAsyncRequestThreadPoolSize() {
        return asyncRequestThreadPoolSize;
      }

      public int getLeaderOutstandingAppendsMax() {
        return leaderOutstandingAppendsMax;
      }

      public static Grpc.Builder newBuilder() {
        return new Grpc.Builder();
      }

      public static class Builder {
        private SizeInBytes messageSizeMax = SizeInBytes.valueOf("512MB");
        private SizeInBytes flowControlWindow = SizeInBytes.valueOf("4MB");
        private boolean asyncRequestThreadPoolCached =
            Server.ASYNC_REQUEST_THREAD_POOL_CACHED_DEFAULT;
        private int asyncRequestThreadPoolSize = Server.ASYNC_REQUEST_THREAD_POOL_SIZE_DEFAULT;
        private int leaderOutstandingAppendsMax = Server.LEADER_OUTSTANDING_APPENDS_MAX_DEFAULT;

        public Grpc build() {
          return new Grpc(
              messageSizeMax,
              flowControlWindow,
              asyncRequestThreadPoolCached,
              asyncRequestThreadPoolSize,
              leaderOutstandingAppendsMax);
        }

        public Builder setMessageSizeMax(SizeInBytes messageSizeMax) {
          this.messageSizeMax = messageSizeMax;
          return this;
        }

        public Builder setFlowControlWindow(SizeInBytes flowControlWindow) {
          this.flowControlWindow = flowControlWindow;
          return this;
        }

        public Builder setAsyncRequestThreadPoolCached(boolean asyncRequestThreadPoolCached) {
          this.asyncRequestThreadPoolCached = asyncRequestThreadPoolCached;
          return this;
        }

        public Builder setAsyncRequestThreadPoolSize(int asyncRequestThreadPoolSize) {
          this.asyncRequestThreadPoolSize = asyncRequestThreadPoolSize;
          return this;
        }

        public Builder setLeaderOutstandingAppendsMax(int leaderOutstandingAppendsMax) {
          this.leaderOutstandingAppendsMax = leaderOutstandingAppendsMax;
          return this;
        }
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

      private RPC(
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
