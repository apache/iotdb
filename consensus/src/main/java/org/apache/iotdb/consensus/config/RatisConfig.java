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

import org.apache.ratis.grpc.GrpcConfigKeys.Server;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

public class RatisConfig {

  private final Rpc rpc;
  private final LeaderElection leaderElection;
  private final Snapshot snapshot;
  private final ThreadPool threadPool;
  private final Log log;
  private final LeaderLogAppender leaderLogAppender;
  private final Grpc grpc;
  private final RatisConsensus ratisConsensus;

  private RatisConfig(
      Rpc rpc,
      LeaderElection leaderElection,
      Snapshot snapshot,
      ThreadPool threadPool,
      Log log,
      Grpc grpc,
      LeaderLogAppender leaderLogAppender,
      RatisConsensus ratisConsensus) {
    this.rpc = rpc;
    this.leaderElection = leaderElection;
    this.snapshot = snapshot;
    this.threadPool = threadPool;
    this.log = log;
    this.leaderLogAppender = leaderLogAppender;
    this.grpc = grpc;
    this.ratisConsensus = ratisConsensus;
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

  public LeaderLogAppender getLeaderLogAppender() {
    return leaderLogAppender;
  }

  public Grpc getGrpc() {
    return grpc;
  }

  public RatisConsensus getRatisConsensus() {
    return ratisConsensus;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Rpc rpc;
    private LeaderElection leaderElection;
    private Snapshot snapshot;
    private ThreadPool threadPool;
    private Log log;
    private LeaderLogAppender leaderLogAppender;
    private Grpc grpc;
    private RatisConsensus ratisConsensus;

    public RatisConfig build() {
      return new RatisConfig(
          rpc != null ? rpc : Rpc.newBuilder().build(),
          leaderElection != null ? leaderElection : LeaderElection.newBuilder().build(),
          snapshot != null ? snapshot : Snapshot.newBuilder().build(),
          threadPool != null ? threadPool : ThreadPool.newBuilder().build(),
          log != null ? log : Log.newBuilder().build(),
          grpc != null ? grpc : Grpc.newBuilder().build(),
          leaderLogAppender != null ? leaderLogAppender : LeaderLogAppender.newBuilder().build(),
          ratisConsensus != null ? ratisConsensus : RatisConsensus.newBuilder().build());
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

    public Builder setRatisConsensus(RatisConsensus ratisConsensus) {
      this.ratisConsensus = ratisConsensus;
      return this;
    }

    public Builder setLeaderLogAppender(LeaderLogAppender leaderLogAppender) {
      this.leaderLogAppender = leaderLogAppender;
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
      private TimeDuration timeoutMax = TimeDuration.valueOf(4, TimeUnit.SECONDS);
      private TimeDuration requestTimeout = TimeDuration.valueOf(20, TimeUnit.SECONDS);
      private TimeDuration sleepTime = TimeDuration.valueOf(1, TimeUnit.SECONDS);
      private TimeDuration slownessTimeout = TimeDuration.valueOf(10, TimeUnit.MINUTES);

      public Rpc build() {
        return new Rpc(timeoutMin, timeoutMax, requestTimeout, sleepTime, slownessTimeout);
      }

      public Rpc.Builder setTimeoutMin(TimeDuration timeoutMin) {
        this.timeoutMin = timeoutMin;
        return this;
      }

      public Rpc.Builder setTimeoutMax(TimeDuration timeoutMax) {
        this.timeoutMax = timeoutMax;
        return this;
      }

      public Rpc.Builder setRequestTimeout(TimeDuration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
      }

      public Rpc.Builder setSleepTime(TimeDuration sleepTime) {
        this.sleepTime = sleepTime;
        return this;
      }

      public Rpc.Builder setSlownessTimeout(TimeDuration slownessTimeout) {
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

      public LeaderElection.Builder setLeaderStepDownWaitTimeKey(
          TimeDuration leaderStepDownWaitTimeKey) {
        this.leaderStepDownWaitTimeKey = leaderStepDownWaitTimeKey;
        return this;
      }

      public LeaderElection.Builder setPreVote(boolean preVote) {
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

      public Snapshot.Builder setAutoTriggerEnabled(boolean autoTriggerEnabled) {
        this.autoTriggerEnabled = autoTriggerEnabled;
        return this;
      }

      public Snapshot.Builder setCreationGap(long creationGap) {
        this.creationGap = creationGap;
        return this;
      }

      public Snapshot.Builder setAutoTriggerThreshold(long autoTriggerThreshold) {
        this.autoTriggerThreshold = autoTriggerThreshold;
        return this;
      }

      public Snapshot.Builder setRetentionFileNum(int retentionFileNum) {
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

      public ThreadPool.Builder setProxyCached(boolean proxyCached) {
        this.proxyCached = proxyCached;
        return this;
      }

      public ThreadPool.Builder setProxySize(int proxySize) {
        this.proxySize = proxySize;
        return this;
      }

      public ThreadPool.Builder setServerCached(boolean serverCached) {
        this.serverCached = serverCached;
        return this;
      }

      public ThreadPool.Builder setServerSize(int serverSize) {
        this.serverSize = serverSize;
        return this;
      }

      public ThreadPool.Builder setClientCached(boolean clientCached) {
        this.clientCached = clientCached;
        return this;
      }

      public ThreadPool.Builder setClientSize(int clientSize) {
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
      private boolean purgeUptoSnapshotIndex = true;
      private SizeInBytes segmentSizeMax = SizeInBytes.valueOf("24MB");
      private int segmentCacheNumMax = 2;
      private SizeInBytes segmentCacheSizeMax = SizeInBytes.valueOf("200MB");
      private SizeInBytes preallocatedSize = SizeInBytes.valueOf("4MB");
      private SizeInBytes writeBufferSize = SizeInBytes.valueOf("64KB");
      private int forceSyncNum = 128;
      private boolean unsafeFlushEnabled = true;

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

      public Log.Builder setUseMemory(boolean useMemory) {
        this.useMemory = useMemory;
        return this;
      }

      public Log.Builder setQueueElementLimit(int queueElementLimit) {
        this.queueElementLimit = queueElementLimit;
        return this;
      }

      public Log.Builder setQueueByteLimit(SizeInBytes queueByteLimit) {
        this.queueByteLimit = queueByteLimit;
        return this;
      }

      public Log.Builder setPurgeGap(int purgeGap) {
        this.purgeGap = purgeGap;
        return this;
      }

      public Log.Builder setPurgeUptoSnapshotIndex(boolean purgeUptoSnapshotIndex) {
        this.purgeUptoSnapshotIndex = purgeUptoSnapshotIndex;
        return this;
      }

      public Log.Builder setSegmentSizeMax(SizeInBytes segmentSizeMax) {
        this.segmentSizeMax = segmentSizeMax;
        return this;
      }

      public Log.Builder setSegmentCacheNumMax(int segmentCacheNumMax) {
        this.segmentCacheNumMax = segmentCacheNumMax;
        return this;
      }

      public Log.Builder setSegmentCacheSizeMax(SizeInBytes segmentCacheSizeMax) {
        this.segmentCacheSizeMax = segmentCacheSizeMax;
        return this;
      }

      public Log.Builder setPreallocatedSize(SizeInBytes preallocatedSize) {
        this.preallocatedSize = preallocatedSize;
        return this;
      }

      public Log.Builder setWriteBufferSize(SizeInBytes writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
      }

      public Log.Builder setForceSyncNum(int forceSyncNum) {
        this.forceSyncNum = forceSyncNum;
        return this;
      }

      public Log.Builder setUnsafeFlushEnabled(boolean unsafeFlushEnabled) {
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

      public Grpc.Builder setMessageSizeMax(SizeInBytes messageSizeMax) {
        this.messageSizeMax = messageSizeMax;
        return this;
      }

      public Grpc.Builder setFlowControlWindow(SizeInBytes flowControlWindow) {
        this.flowControlWindow = flowControlWindow;
        return this;
      }

      public Grpc.Builder setAsyncRequestThreadPoolCached(boolean asyncRequestThreadPoolCached) {
        this.asyncRequestThreadPoolCached = asyncRequestThreadPoolCached;
        return this;
      }

      public Grpc.Builder setAsyncRequestThreadPoolSize(int asyncRequestThreadPoolSize) {
        this.asyncRequestThreadPoolSize = asyncRequestThreadPoolSize;
        return this;
      }

      public Grpc.Builder setLeaderOutstandingAppendsMax(int leaderOutstandingAppendsMax) {
        this.leaderOutstandingAppendsMax = leaderOutstandingAppendsMax;
        return this;
      }
    }
  }

  public static class RatisConsensus {
    private final int retryTimesMax;
    private final long retryWaitMillis;

    private RatisConsensus(int retryTimesMax, long retryWaitMillis) {
      this.retryTimesMax = retryTimesMax;
      this.retryWaitMillis = retryWaitMillis;
    }

    public int getRetryTimesMax() {
      return retryTimesMax;
    }

    public long getRetryWaitMillis() {
      return retryWaitMillis;
    }

    public static RatisConsensus.Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {
      private int retryTimesMax = 3;
      private long retryWaitMillis = 500;

      public RatisConsensus build() {
        return new RatisConsensus(retryTimesMax, retryWaitMillis);
      }

      public RatisConsensus.Builder setRetryTimesMax(int retryTimesMax) {
        this.retryTimesMax = retryTimesMax;
        return this;
      }

      public RatisConsensus.Builder setRetryWaitMillis(long retryWaitMillis) {
        this.retryWaitMillis = retryWaitMillis;
        return this;
      }
    }
  }

  public static class LeaderLogAppender {
    private final SizeInBytes bufferByteLimit;
    private final SizeInBytes snapshotChunkSizeMax;
    private final boolean installSnapshotEnabled;

    private LeaderLogAppender(
        SizeInBytes bufferByteLimit,
        SizeInBytes snapshotChunkSizeMax,
        boolean installSnapshotEnabled) {
      this.bufferByteLimit = bufferByteLimit;
      this.snapshotChunkSizeMax = snapshotChunkSizeMax;
      this.installSnapshotEnabled = installSnapshotEnabled;
    }

    public SizeInBytes getBufferByteLimit() {
      return bufferByteLimit;
    }

    public SizeInBytes getSnapshotChunkSizeMax() {
      return snapshotChunkSizeMax;
    }

    public boolean isInstallSnapshotEnabled() {
      return installSnapshotEnabled;
    }

    public static LeaderLogAppender.Builder newBuilder() {
      return new LeaderLogAppender.Builder();
    }

    public static class Builder {
      private SizeInBytes bufferByteLimit =
          RaftServerConfigKeys.Log.Appender.BUFFER_BYTE_LIMIT_DEFAULT;
      private SizeInBytes snapshotChunkSizeMax =
          RaftServerConfigKeys.Log.Appender.SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT;
      private boolean installSnapshotEnabled =
          RaftServerConfigKeys.Log.Appender.INSTALL_SNAPSHOT_ENABLED_DEFAULT;

      public LeaderLogAppender build() {
        return new LeaderLogAppender(bufferByteLimit, snapshotChunkSizeMax, installSnapshotEnabled);
      }

      public LeaderLogAppender.Builder setBufferByteLimit(long bufferByteLimit) {
        this.bufferByteLimit = SizeInBytes.valueOf(bufferByteLimit);
        return this;
      }

      public LeaderLogAppender.Builder setSnapshotChunkSizeMax(long snapshotChunkSizeMax) {
        this.snapshotChunkSizeMax = SizeInBytes.valueOf(snapshotChunkSizeMax);
        return this;
      }

      public LeaderLogAppender.Builder setInstallSnapshotEnabled(boolean installSnapshotEnabled) {
        this.installSnapshotEnabled = installSnapshotEnabled;
        return this;
      }
    }
  }
}
