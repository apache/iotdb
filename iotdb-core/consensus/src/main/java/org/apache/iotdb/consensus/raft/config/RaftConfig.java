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

package org.apache.iotdb.consensus.raft.config;

import org.apache.iotdb.consensus.config.IoTConsensusConfig;

/**
 * All {@code raft_*} parameters with their defaults. Plain mutable sections so that the loader can
 * fill them from properties and {@code reload} can replace hot-swappable values in place.
 */
public class RaftConfig {

  public static final class Rpc {
    public int rpcMaxConcurrentClientNum = 65535;
    public int thriftServerAwaitTimeForStopService = 60;
    public boolean rpcThriftCompressionEnabled = false;
    public int selectorNumOfClientManager = 1;
    public int connectionTimeoutInMs = 60_000;
    public boolean printLogWhenThriftClientEncounterException = true;
    public int maxClientNumForEachNode = 300;
    public boolean enableSsl = false;
    public String sslKeyStorePath = "";
    public String sslKeyStorePassword = "";
    public String sslTrustStorePath = "";
    public String sslTrustStorePassword = "";
    public long rpcTimeoutMs = 10_000;

    /** Disabled by default; benchmark data-link serialization model, bytes per second. */
    public long replicationBytesPerSecond = 0;

    /** Copy the Thrift parameters IoTDB already configured for the consensus port. */
    public Rpc from(IoTConsensusConfig.RPC r) {
      rpcMaxConcurrentClientNum = r.getRpcMaxConcurrentClientNum();
      thriftServerAwaitTimeForStopService = r.getThriftServerAwaitTimeForStopService();
      rpcThriftCompressionEnabled = r.isRpcThriftCompressionEnabled();
      selectorNumOfClientManager = r.getSelectorNumOfClientManager();
      connectionTimeoutInMs = r.getConnectionTimeoutInMs();
      printLogWhenThriftClientEncounterException = r.isPrintLogWhenThriftClientEncounterException();
      maxClientNumForEachNode = r.getMaxClientNumForEachNode();
      enableSsl = r.isEnableSSL();
      sslKeyStorePath = r.getSslKeyStorePath();
      sslKeyStorePassword = r.getSslKeyStorePassword();
      sslTrustStorePath = r.getSslTrustStorePath();
      sslTrustStorePassword = r.getSslTrustStorePassword();
      return this;
    }
  }

  public static final class Election {
    public long electionTimeoutMinMs = 2000;
    public long electionTimeoutMaxMs = 4000;
    public long firstElectionTimeoutMinMs = 500;
    public long firstElectionTimeoutMaxMs = 1500;
    public long heartbeatIntervalMs = 1000;
    public boolean enablePreVote = true;
    public long leaderTransferTimeoutMs = 10_000;
  }

  public static final class Replication {
    public int maxEntriesPerAppend = 256;
    public long maxBytesPerAppend = 4L << 20;
    public int maxInflightBatches = 8;
    public long maxInflightBytes = 64L << 20;
    public long basicRetryWaitMs = 100;
    public long maxRetryWaitMs = 5000;
    public long maxUncommittedEntries = 100_000;
    public long maxUncommittedBytes = 256L << 20;
    public long writeThrottleTimeoutMs = 30_000;
    public long writeTimeoutMs = 20_000;
    public long leaderReadyWaitMs = 20_000;

    /** addRemotePeer: promote the learner once it is within this many entries of commitIndex. */
    public long catchUpLagEntries = 1000;

    public long catchUpTimeoutMs = 600_000;
    public long configChangeTimeoutMs = 60_000;
  }

  public static final class Log {
    public long segmentBytes = 64L << 20;

    /**
     * Force policy of the pipelined log writer (the same as TRaft's journal flush policy): written
     * entries are forced once this many are unforced, or once the oldest has waited {@link
     * #fsyncMaxDelayMs} (0: as soon as the previous force ends). Metadata writes force at once.
     */
    public int fsyncMaxBatchEntries = 1024;

    public long fsyncMaxDelayMs = 2;
    public long cacheBytes = Math.min(Runtime.getRuntime().maxMemory() / 16, 4L << 30);
  }

  public static final class Snapshot {
    public long triggerEntries = 400_000;
    public long triggerBytes = 1L << 30;
    public long preserveEntriesAfterSnapshot = 10_000;
    public int retentionNum = 2;
    public long transferBytesPerSecond = 0;
    public long appliedCheckpointGap = 0;
  }

  public enum ReadConsistency {
    LOCAL,
    LEASE,
    READ_INDEX
  }

  public static final class Read {
    public ReadConsistency consistency = ReadConsistency.READ_INDEX;
    public double leaseRatio = 0.8;

    /** READ_INDEX / follower reads: how long to wait for the confirmed commit index to apply. */
    public long readIndexTimeoutMs = 10_000;

    /** Lease check period on a leader (0 = off). */
    public long leaseCheckIntervalMs = 1000;
  }

  /** L3: invariant violations throw (stop the group) instead of only being counted. */
  public boolean strictAsserts = false;

  /** Runs of committed data entries are applied with one IStateMachine.writeBatch call. */
  public boolean applyBatch = true;

  /**
   * A leader submits all normalized rows of a request before waiting for the first (TRaft's
   * traft_pipeline_rows); false: each row waits for the previous one to apply.
   */
  public boolean pipelineRows = true;

  /**
   * 0 (the default): a cached pool, so every busy group's executor has a thread of its own, as
   * TRaft's group executors do. A positive value: a fixed pool of that size shared by all groups.
   */
  public static final class Threads {
    public int coreThreads = 0;
    public int applyThreads = 0;
  }

  public final Rpc rpc = new Rpc();
  public final Election election = new Election();
  public final Replication replication = new Replication();
  public final Log log = new Log();
  public final Snapshot snapshot = new Snapshot();
  public final Read read = new Read();
  public final Threads threads = new Threads();

  /** Small, fast values for tests. */
  public static RaftConfig forTests() {
    RaftConfig c = new RaftConfig();
    c.election.electionTimeoutMinMs = 150;
    c.election.electionTimeoutMaxMs = 300;
    c.election.firstElectionTimeoutMinMs = 50;
    c.election.firstElectionTimeoutMaxMs = 100;
    c.election.heartbeatIntervalMs = 50;
    c.log.segmentBytes = 1L << 20;
    c.log.cacheBytes = 4L << 20;
    c.rpc.rpcTimeoutMs = 3000;
    c.rpc.connectionTimeoutInMs = 3000;
    c.replication.writeTimeoutMs = 10_000;
    c.replication.leaderReadyWaitMs = 10_000;
    return c;
  }
}
