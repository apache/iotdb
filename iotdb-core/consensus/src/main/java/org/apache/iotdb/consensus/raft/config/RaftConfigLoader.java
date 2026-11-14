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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.config.ConsensusConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

/**
 * Three-level configuration without touching IoTDB's descriptors: (1) values derived from the
 * {@link ConsensusConfig} IoTDB already built (endpoint, storage dir, Thrift parameters), (2)
 * {@code raft_*} / {@code traft_*} keys in {@code iotdb-system.properties} or {@code
 * raft.properties} in the conf dir, (3) {@code -Draft.<key>=} system properties.
 */
public final class RaftConfigLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftConfigLoader.class);

  private RaftConfigLoader() {}

  public static RaftConfig load(ConsensusConfig cc) {
    RaftConfig c = new RaftConfig();
    return load(cc, c);
  }

  public static RaftConfig load(ConsensusConfig cc, RaftConfig c) {
    if (cc != null && cc.getIotConsensusConfig() != null) {
      c.rpc.from(cc.getIotConsensusConfig().getRpc());
    }
    apply(collect(), c);
    return c;
  }

  /** The {@code raft_*} / {@code traft_*} keys from the conf dir files and system properties. */
  public static Properties collect() {
    return collect("raft", "traft");
  }

  /**
   * The keys of these sections ({@code <prefix>_*} in {@code iotdb-system.properties} / {@code
   * raft.properties}, overridden by {@code -D<prefix>.<key>=} system properties, which become
   * {@code <prefix>_<key>}).
   */
  public static Properties collect(String... prefixes) {
    Properties p = new Properties();
    String confDir = null;
    try {
      confDir = CommonDescriptor.getInstance().getConfDir();
    } catch (Exception e) {
      LOGGER.debug("no conf dir: {}", e.toString());
    }
    if (confDir != null) {
      loadInto(p, new File(confDir, "iotdb-system.properties"), prefixes);
      loadInto(p, new File(confDir, "raft.properties"), prefixes);
    }
    for (String name : System.getProperties().stringPropertyNames()) {
      if (hasPrefix(name, '.', prefixes)) {
        p.setProperty(name.replace('.', '_'), System.getProperty(name));
      }
    }
    return p;
  }

  private static boolean hasPrefix(String name, char separator, String... prefixes) {
    for (String prefix : prefixes) {
      if (name.startsWith(prefix + separator)) {
        return true;
      }
    }
    return false;
  }

  private static void loadInto(Properties p, File f, String... prefixes) {
    if (!f.exists()) {
      return;
    }
    try (InputStream in = Files.newInputStream(f.toPath())) {
      Properties file = new Properties();
      file.load(in);
      for (String name : file.stringPropertyNames()) {
        if (hasPrefix(name, '_', prefixes)) {
          p.setProperty(name, file.getProperty(name));
        }
      }
    } catch (IOException e) {
      LOGGER.warn("cannot read {}", f, e);
    }
  }

  /** Apply known keys; unknown keys are ignored (TRaft reads its own keys the same way). */
  public static void apply(Properties p, RaftConfig c) {
    c.election.electionTimeoutMinMs =
        getLong(p, "raft_election_timeout_min_ms", c.election.electionTimeoutMinMs);
    c.election.electionTimeoutMaxMs =
        getLong(p, "raft_election_timeout_max_ms", c.election.electionTimeoutMaxMs);
    c.election.firstElectionTimeoutMinMs =
        getLong(p, "raft_first_election_timeout_min_ms", c.election.firstElectionTimeoutMinMs);
    c.election.firstElectionTimeoutMaxMs =
        getLong(p, "raft_first_election_timeout_max_ms", c.election.firstElectionTimeoutMaxMs);
    c.election.heartbeatIntervalMs =
        getLong(p, "raft_heartbeat_interval_ms", c.election.heartbeatIntervalMs);
    c.election.enablePreVote = getBool(p, "raft_enable_prevote", c.election.enablePreVote);
    c.election.leaderTransferTimeoutMs =
        getLong(p, "raft_leader_transfer_timeout_ms", c.election.leaderTransferTimeoutMs);
    c.rpc.rpcTimeoutMs = getLong(p, "raft_rpc_timeout_ms", c.rpc.rpcTimeoutMs);
    c.rpc.replicationBytesPerSecond =
        getLong(p, "raft_replication_bytes_per_second", c.rpc.replicationBytesPerSecond);
    c.replication.maxEntriesPerAppend =
        (int) getLong(p, "raft_max_entries_per_append", c.replication.maxEntriesPerAppend);
    c.replication.maxBytesPerAppend =
        getLong(p, "raft_max_bytes_per_append", c.replication.maxBytesPerAppend);
    c.replication.maxInflightBatches =
        (int) getLong(p, "raft_max_inflight_batches", c.replication.maxInflightBatches);
    c.replication.maxInflightBytes =
        getLong(p, "raft_max_inflight_bytes", c.replication.maxInflightBytes);
    c.replication.maxUncommittedEntries =
        getLong(p, "raft_max_uncommitted_entries", c.replication.maxUncommittedEntries);
    c.replication.maxUncommittedBytes =
        getLong(p, "raft_max_uncommitted_bytes", c.replication.maxUncommittedBytes);
    c.replication.writeThrottleTimeoutMs =
        getLong(p, "raft_write_throttle_timeout_ms", c.replication.writeThrottleTimeoutMs);
    c.replication.writeTimeoutMs =
        getLong(p, "raft_write_timeout_ms", c.replication.writeTimeoutMs);
    c.replication.leaderReadyWaitMs =
        getLong(p, "raft_leader_ready_wait_ms", c.replication.leaderReadyWaitMs);
    c.log.segmentBytes = getLong(p, "raft_log_segment_bytes", c.log.segmentBytes);
    c.log.fsyncMaxBatchEntries =
        (int) getLong(p, "raft_log_fsync_max_batch_entries", c.log.fsyncMaxBatchEntries);
    c.log.fsyncMaxDelayMs = getLong(p, "raft_log_fsync_max_delay_ms", c.log.fsyncMaxDelayMs);
    c.log.cacheBytes = getLong(p, "raft_log_cache_bytes", c.log.cacheBytes);
    c.snapshot.triggerEntries =
        getLong(p, "raft_snapshot_trigger_entries", c.snapshot.triggerEntries);
    c.snapshot.triggerBytes = getLong(p, "raft_snapshot_trigger_bytes", c.snapshot.triggerBytes);
    c.snapshot.preserveEntriesAfterSnapshot =
        getLong(
            p, "raft_log_preserve_entries_after_snapshot", c.snapshot.preserveEntriesAfterSnapshot);
    c.snapshot.retentionNum =
        (int) getLong(p, "raft_snapshot_retention_num", c.snapshot.retentionNum);
    c.snapshot.appliedCheckpointGap =
        getLong(p, "raft_applied_checkpoint_gap", c.snapshot.appliedCheckpointGap);
    String rc = p.getProperty("raft_read_consistency");
    if (rc != null) {
      try {
        c.read.consistency = RaftConfig.ReadConsistency.valueOf(rc.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        LOGGER.warn("unknown raft_read_consistency {}", rc);
      }
    }
    c.read.leaseRatio = getDouble(p, "raft_lease_ratio", c.read.leaseRatio);
    c.read.readIndexTimeoutMs = getLong(p, "raft_read_index_timeout_ms", c.read.readIndexTimeoutMs);
    c.read.leaseCheckIntervalMs =
        getLong(p, "raft_lease_check_interval_ms", c.read.leaseCheckIntervalMs);
    c.replication.basicRetryWaitMs =
        getLong(p, "raft_basic_retry_wait_ms", c.replication.basicRetryWaitMs);
    c.replication.maxRetryWaitMs =
        getLong(p, "raft_max_retry_wait_ms", c.replication.maxRetryWaitMs);
    c.strictAsserts = getBool(p, "raft_strict_asserts", c.strictAsserts);
    c.applyBatch = getBool(p, "raft_apply_batch", c.applyBatch);
    c.pipelineRows = getBool(p, "raft_pipeline_rows", c.pipelineRows);
    c.threads.coreThreads = (int) getLong(p, "raft_core_threads", c.threads.coreThreads);
    c.threads.applyThreads = (int) getLong(p, "raft_apply_threads", c.threads.applyThreads);
  }

  public static long getLong(Properties p, String key, long def) {
    String v = p.getProperty(key);
    if (v == null) {
      return def;
    }
    try {
      return Long.parseLong(v.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("bad value for {}: {}", key, v);
      return def;
    }
  }

  public static double getDouble(Properties p, String key, double def) {
    String v = p.getProperty(key);
    if (v == null) {
      return def;
    }
    try {
      return Double.parseDouble(v.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("bad value for {}: {}", key, v);
      return def;
    }
  }

  public static boolean getBool(Properties p, String key, boolean def) {
    String v = p.getProperty(key);
    return v == null ? def : Boolean.parseBoolean(v.trim());
  }
}
