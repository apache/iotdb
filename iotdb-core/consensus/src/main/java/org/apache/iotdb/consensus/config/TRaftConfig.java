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

import java.util.Optional;

/**
 * Runtime knobs for TRaft.
 *
 * <p>TRaft still relies on standard Raft timing and quorum rules for safety-critical behavior, but
 * it also persists time-partition metadata and snapshot parameters that are specific to time-series
 * workloads.
 */
public class TRaftConfig {

  private final Replication replication;
  private final Election election;
  private final Snapshot snapshot;

  private TRaftConfig(Replication replication, Election election, Snapshot snapshot) {
    this.replication = replication;
    this.election = election;
    this.snapshot = snapshot;
  }

  public Replication getReplication() {
    return replication;
  }

  public Election getElection() {
    return election;
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Replication replication;
    private Election election;
    private Snapshot snapshot;

    public Builder setReplication(Replication replication) {
      this.replication = replication;
      return this;
    }

    public Builder setElection(Election election) {
      this.election = election;
      return this;
    }

    public Builder setSnapshot(Snapshot snapshot) {
      this.snapshot = snapshot;
      return this;
    }

    public TRaftConfig build() {
      return new TRaftConfig(
          Optional.ofNullable(replication).orElseGet(() -> Replication.newBuilder().build()),
          Optional.ofNullable(election).orElseGet(() -> Election.newBuilder().build()),
          Optional.ofNullable(snapshot).orElseGet(() -> Snapshot.newBuilder().build()));
    }
  }

  /** Replication and catch-up parameters used by append, retry, and snapshot shipping. */
  public static class Replication {

    private final int maxPendingRetryEntriesPerFollower;
    private final long waitingReplicationTimeMs;
    private final long requestTimeoutMs;
    private final int maxEntriesPerAppend;

    private Replication(
        int maxPendingRetryEntriesPerFollower,
        long waitingReplicationTimeMs,
        long requestTimeoutMs,
        int maxEntriesPerAppend) {
      this.maxPendingRetryEntriesPerFollower = maxPendingRetryEntriesPerFollower;
      this.waitingReplicationTimeMs = waitingReplicationTimeMs;
      this.requestTimeoutMs = requestTimeoutMs;
      this.maxEntriesPerAppend = maxEntriesPerAppend;
    }

    public int getMaxPendingRetryEntriesPerFollower() {
      return maxPendingRetryEntriesPerFollower;
    }

    public long getWaitingReplicationTimeMs() {
      return waitingReplicationTimeMs;
    }

    public long getRequestTimeoutMs() {
      return requestTimeoutMs;
    }

    public int getMaxEntriesPerAppend() {
      return maxEntriesPerAppend;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {

      private int maxPendingRetryEntriesPerFollower = 100_000;
      private long waitingReplicationTimeMs = 200L;
      private long requestTimeoutMs = 5_000L;
      private int maxEntriesPerAppend = 128;

      public Builder setMaxPendingRetryEntriesPerFollower(int maxPendingRetryEntriesPerFollower) {
        this.maxPendingRetryEntriesPerFollower = maxPendingRetryEntriesPerFollower;
        return this;
      }

      public Builder setWaitingReplicationTimeMs(long waitingReplicationTimeMs) {
        this.waitingReplicationTimeMs = waitingReplicationTimeMs;
        return this;
      }

      public Builder setRequestTimeoutMs(long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
        return this;
      }

      public Builder setMaxEntriesPerAppend(int maxEntriesPerAppend) {
        this.maxEntriesPerAppend = maxEntriesPerAppend;
        return this;
      }

      public Replication build() {
        return new Replication(
            maxPendingRetryEntriesPerFollower,
            waitingReplicationTimeMs,
            requestTimeoutMs,
            maxEntriesPerAppend);
      }
    }
  }

  /** Election and heartbeat timing. */
  public static class Election {

    private final long randomSeed;
    private final long heartbeatIntervalMs;
    private final long timeoutMinMs;
    private final long timeoutMaxMs;

    private Election(
        long randomSeed, long heartbeatIntervalMs, long timeoutMinMs, long timeoutMaxMs) {
      this.randomSeed = randomSeed;
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      this.timeoutMinMs = timeoutMinMs;
      this.timeoutMaxMs = timeoutMaxMs;
    }

    public long getRandomSeed() {
      return randomSeed;
    }

    public long getHeartbeatIntervalMs() {
      return heartbeatIntervalMs;
    }

    public long getTimeoutMinMs() {
      return timeoutMinMs;
    }

    public long getTimeoutMaxMs() {
      return timeoutMaxMs;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {

      private long randomSeed = System.nanoTime();
      private long heartbeatIntervalMs = 200L;
      private long timeoutMinMs = 800L;
      private long timeoutMaxMs = 1_600L;

      public Builder setRandomSeed(long randomSeed) {
        this.randomSeed = randomSeed;
        return this;
      }

      public Builder setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        return this;
      }

      public Builder setTimeoutMinMs(long timeoutMinMs) {
        this.timeoutMinMs = timeoutMinMs;
        return this;
      }

      public Builder setTimeoutMaxMs(long timeoutMaxMs) {
        this.timeoutMaxMs = timeoutMaxMs;
        return this;
      }

      public Election build() {
        return new Election(randomSeed, heartbeatIntervalMs, timeoutMinMs, timeoutMaxMs);
      }
    }
  }

  /** Snapshot compaction parameters. */
  public static class Snapshot {

    private final long autoTriggerLogThreshold;

    private Snapshot(long autoTriggerLogThreshold) {
      this.autoTriggerLogThreshold = autoTriggerLogThreshold;
    }

    public long getAutoTriggerLogThreshold() {
      return autoTriggerLogThreshold;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {

      private long autoTriggerLogThreshold = 10_000L;

      public Builder setAutoTriggerLogThreshold(long autoTriggerLogThreshold) {
        this.autoTriggerLogThreshold = autoTriggerLogThreshold;
        return this;
      }

      public Snapshot build() {
        return new Snapshot(autoTriggerLogThreshold);
      }
    }
  }
}
