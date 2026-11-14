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
 * KIND, either express or implied, See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leader-side table of waiting client writes. A future completes with the state machine's result
 * once the entry is applied, or exceptionally with {@link NotLeaderException} when the leader steps
 * down before commitment.
 */
public final class ClientRequestTable {

  private static final class EntryIdentity {
    final long index;
    final long term;

    EntryIdentity(long index, long term) {
      this.index = index;
      this.term = term;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof EntryIdentity)) {
        return false;
      }
      EntryIdentity that = (EntryIdentity) other;
      return index == that.index && term == that.term;
    }

    @Override
    public int hashCode() {
      return 31 * Long.hashCode(index) + Long.hashCode(term);
    }
  }

  private static final class Pending {
    final CompletableFuture<TSStatus> future;
    boolean committed;
    long admittedNanos;
    EntryIdentity identity;

    Pending(CompletableFuture<TSStatus> future) {
      this.future = future;
    }
  }

  /** Prefix of the latency counters ({@code raft} for Raft, the profile's name otherwise). */
  private final String leaderCommitMetric;

  private final String leaderApplyMetric;

  private final AtomicLong seq = new AtomicLong();
  // Both maps share this monitor. No future is completed under it: user callbacks may reenter
  // the consensus server or wait for another thread that needs this table.
  private final Map<Long, Pending> pending = new HashMap<>();
  private final Map<EntryIdentity, Long> byEntry = new HashMap<>();

  /** A table feeding the {@code raft.*} counters. */
  public ClientRequestTable() {
    this("raft");
  }

  /** A table feeding the {@code <metricsPrefix>.leader_commit / leader_apply} counters. */
  public ClientRequestTable(String metricsPrefix) {
    this.leaderCommitMetric = metricsPrefix + ".leader_commit";
    this.leaderApplyMetric = metricsPrefix + ".leader_apply";
  }

  public long register(CompletableFuture<TSStatus> future) {
    long id = seq.incrementAndGet();
    Pending p = new Pending(future);
    synchronized (this) {
      pending.put(id, p);
    }
    future.whenComplete(
        (status, failure) -> {
          synchronized (this) {
            remove(id, p);
          }
        });
    return id;
  }

  public synchronized void onAdmitted(long id, long nanos) {
    Pending p = pending.get(id);
    if (p != null) {
      p.admittedNanos = nanos;
    }
  }

  /**
   * Bind before submitting the local entry to the writer. This association survives payload-cache
   * eviction and commitment, but only lives as long as the waiting client future. It is neither a
   * replicated request ID nor a durable exactly-once record.
   */
  synchronized void bind(long reqId, long index, long term) {
    Pending p = pending.get(reqId);
    if (p == null) {
      return; // cancellation / terminal outcome won the race with persistence dispatch
    }
    EntryIdentity identity = new EntryIdentity(index, term);
    Long previous = byEntry.get(identity);
    if ((p.identity != null && !p.identity.equals(identity))
        || (previous != null && previous != reqId)) {
      throw new IllegalStateException("client request bound to conflicting log identity");
    }
    p.identity = identity;
    byEntry.put(identity, reqId);
  }

  public void bind(org.apache.iotdb.consensus.raft.core.log.LogEntry entry) {
    bind(entry.reqId, entry.index, entry.term);
  }

  public synchronized boolean isLocalEntry(long index, long term) {
    return byEntry.containsKey(new EntryIdentity(index, term));
  }

  /** Kernel outcome for a request (commit / not leader / timeout / rejected). */
  public void onOutcome(Action.CompleteClient a) {
    Pending p;
    long commitLatency = -1;
    synchronized (this) {
      p = pending.get(a.reqId);
      if (p == null) {
        return;
      }
      if (a.outcome == Action.Outcome.COMMITTED) {
        if (!p.committed && p.admittedNanos != 0) {
          commitLatency = System.nanoTime() - p.admittedNanos;
        }
        p.committed = true;
      } else {
        remove(a.reqId, p);
      }
    }
    if (commitLatency >= 0) {
      ResearchMetrics.latency(leaderCommitMetric, commitLatency);
    }
    switch (a.outcome) {
      case COMMITTED:
        break;
      case NOT_LEADER:
        p.future.completeExceptionally(new NotLeaderException(a.leaderHint));
        break;
      case TIMEOUT:
        p.future.complete(
            RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "consensus write timed out"));
        break;
      case READ_ONLY:
        p.future.complete(RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY));
        break;
      default:
        p.future.complete(
            RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "consensus write rejected"));
        break;
    }
  }

  /** The applier finished this exact log identity, including when it was read from disk. */
  public void onApplied(long index, long term, TSStatus status) {
    Pending p;
    synchronized (this) {
      Long reqId = byEntry.get(new EntryIdentity(index, term));
      p = reqId == null ? null : pending.get(reqId);
      if (p != null) {
        remove(reqId, p);
      }
    }
    if (p != null) {
      if (p.admittedNanos != 0) {
        ResearchMetrics.latency(leaderApplyMetric, System.nanoTime() - p.admittedNanos);
      }
      p.future.complete(status);
    }
  }

  public synchronized int size() {
    return pending.size();
  }

  synchronized int boundSize() {
    return byEntry.size();
  }

  /**
   * Caller holds this monitor; identity removal cannot affect a later request at the same index.
   */
  private void remove(long id, Pending p) {
    if (pending.remove(id, p) && p.identity != null) {
      byEntry.remove(p.identity, id);
    }
  }

  public void failAll(NotLeaderException e) {
    List<Pending> failed;
    synchronized (this) {
      failed = new ArrayList<>(pending.values());
      pending.clear();
      byEntry.clear();
    }
    for (Pending p : failed) {
      p.future.completeExceptionally(e);
    }
  }
}
