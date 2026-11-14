/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.consensus.traft.paper.core;

import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Application;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableStore;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Proposal;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Role;

import java.io.IOException;
import java.io.PrintStream;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Seeded crash and fault simulation over TRaftEngine, driven the way TRaftGroup drives it: its
 * receive grouping, drive round and Channel are transcribed here over a simulated wire, so a change
 * to them should be mirrored. Every run is a pure function of (Config, List of Action), so a
 * failing seed can be replayed and minimized by removing actions. TRaftFuzzTest runs a short fixed
 * range; main runs campaigns, for example with the test classpath:
 *
 * <pre>
 * java -cp ... org.apache.iotdb.consensus.traft.paper.core.TRaftFuzz campaign 0 40000 2 label
 * java -cp ... org.apache.iotdb.consensus.traft.paper.core.TRaftFuzz one 16
 * java -cp ... org.apache.iotdb.consensus.traft.paper.core.TRaftFuzz min 16 60000
 * </pre>
 *
 * The store loses the unforced tail on a crash, including torn batches and deferred markers.
 * Checked throughout: an ACK or a commit count has durable backing, the committed set is held by
 * every leader, application is partition-ordered with the same contents, logs match and one replica
 * wins each term. After healing every replica must converge and a probe write must apply. Not
 * modelled: the real TRaftFileStore (its checks are mirrored), snapshots, apply failures and real
 * threads.
 *
 * <p>About half of the seeds compact (chosen from the seed, so the other parameters of a seed do
 * not change). Logs are then compared as logical views: the compacted prefix, rebuilt from the
 * node's own forced manifests, followed by the engine's suffix. A compaction must leave every other
 * node durably holding the compacted chain; after healing, a final load burst must bring every
 * node's frontier within three partitions of its open one with bounded state.
 */
public final class TRaftFuzz {

  // ---------------------------------------------------------------- errors

  static final class Violation extends Error {
    final String category;
    final String detail;

    Violation(String category, String detail) {
      super(category + ": " + detail);
      this.category = category;
      this.detail = detail;
    }
  }

  static final class SimCrash extends Error {
    final double survive;
    final double down;

    SimCrash(double survive, double down) {
      super("simulated crash", null, false, false);
      this.survive = survive;
      this.down = down;
    }
  }

  // ---------------------------------------------------------------- scenario

  static final int DELIVER = 0;
  static final int TICKALL = 1;
  static final int TICK = 2;
  static final int CLIENT = 3;
  static final int CRASH = 4;
  static final int CRASHMANY = 5;
  static final int ARM = 6;
  static final int CUT = 7;
  static final int ELECT = 8;
  static final int LAG = 9;
  static final int FAILOVER = 10;
  static final String[] ACTION_NAMES = {
    "DELIVER",
    "TICKALL",
    "TICK",
    "CLIENT",
    "CRASH",
    "CRASHMANY",
    "ARM",
    "CUT",
    "ELECT",
    "LAG",
    "FAILOVER"
  };

  static final int[][] PROFILE_WEIGHTS = {
    // deliver tickall tick client crash many arm cut elect lag failover
    {600, 80, 40, 150, 6, 1, 8, 8, 4, 8, 3}, // mixed
    {550, 80, 20, 250, 2, 0, 4, 2, 1, 40, 1}, // laggard
    {560, 80, 30, 200, 30, 4, 30, 4, 4, 8, 6}, // crashy
    {600, 80, 40, 150, 8, 1, 4, 10, 40, 5, 6}, // storm
    {620, 100, 40, 150, 2, 0, 2, 10, 2, 5, 2}, // lossy
    {530, 80, 20, 250, 8, 1, 50, 2, 2, 40, 3}, // torn
    {560, 60, 20, 300, 2, 0, 2, 4, 2, 12, 12}, // failover
    {540, 70, 20, 280, 1, 0, 1, 2, 1, 60, 1}, // tight window, long lags: repair competes for slots
  };

  static final class Config {
    long seed;
    int profile;
    int nodes;
    int theta;
    int maxEntries;
    int inflightBatches;
    int lossPct;
    int dupPct;
    int fifoPct;
    int fallbackPct;
    long partitionMs;
    long maxInflightBytes;
    long maxBytes;
    long budget;
    long electionMin;
    long electionMax;
    long heartbeat;
    boolean volatileMode;
    int actions;
    int controlPage;
    boolean compaction;
    // Negative control: compaction ignores the cluster cap (TraftEngine test hook).
    boolean ignoreCap;

    int window() {
      return maxEntries * inflightBatches;
    }

    @Override
    public String toString() {
      return "seed="
          + seed
          + " profile="
          + profile
          + " nodes="
          + nodes
          + " theta="
          + theta
          + " maxEntries="
          + maxEntries
          + " inflightBatches="
          + inflightBatches
          + " window="
          + window()
          + " loss="
          + lossPct
          + " dup="
          + dupPct
          + " fifo="
          + fifoPct
          + " fallback="
          + fallbackPct
          + " partitionMs="
          + partitionMs
          + " inflightBytes="
          + maxInflightBytes
          + " budget="
          + budget
          + " election="
          + electionMin
          + ".."
          + electionMax
          + " heartbeat="
          + heartbeat
          + " volatile="
          + volatileMode
          + " actions="
          + actions
          + " controlPage="
          + controlPage
          + " compaction="
          + compaction
          + (ignoreCap ? " IGNORE-CAP" : "");
    }
  }

  static final class Action {
    final int type;
    final int a;
    final int b;
    final int c;
    final double x;
    final double y;

    Action(int type, int a, int b, int c, double x, double y) {
      this.type = type;
      this.a = a;
      this.b = b;
      this.c = c;
      this.x = x;
      this.y = y;
    }

    @Override
    public String toString() {
      return ACTION_NAMES[type]
          + "("
          + a
          + ","
          + b
          + ","
          + c
          + ","
          + String.format("%.3f", x)
          + ","
          + String.format("%.3f", y)
          + ")";
    }
  }

  private static int pick(Random r, int... values) {
    return values[r.nextInt(values.length)];
  }

  static Config config(long seed) {
    Random r = new Random(seed * 0x9E3779B97F4A7C15L + 1);
    Config c = new Config();
    c.seed = seed;
    c.profile = r.nextInt(PROFILE_WEIGHTS.length);
    c.nodes = r.nextInt(100) < 85 ? 3 : 5;
    c.theta = pick(r, 2, 3, 3, 4, 4, 5, 8, 16);
    c.maxEntries = pick(r, 1, 2, 4, 8, 16);
    c.inflightBatches = pick(r, 1, 2, 2, 4, 8);
    c.partitionMs = pick(r, 0, 0, 40, 200);
    c.maxInflightBytes = r.nextInt(100) < 15 ? 65536 : 64L << 20;
    c.maxBytes = 4L << 20;
    c.budget = r.nextInt(100) < 15 ? 1 : c.maxInflightBytes;
    c.volatileMode = r.nextInt(100) < 60;
    c.fallbackPct = r.nextInt(100) < 15 ? 20 : 0;
    c.fifoPct = pick(r, 20, 50, 70, 70, 90, 95);
    c.heartbeat = pick(r, 10, 10, 20, 30);
    switch (c.profile) {
      case 7: // tight window
        c.lossPct = pick(r, 0, 0, 1);
        c.dupPct = pick(r, 0, 0, 1);
        c.maxEntries = pick(r, 1, 2, 3);
        c.inflightBatches = pick(r, 1, 2, 3);
        c.theta = pick(r, 4, 5, 6, 8, 12, 16);
        c.fifoPct = pick(r, 70, 90, 95);
        break;
      case 6: // failover: many outstanding entries, reordered, then the leader dies
        c.lossPct = pick(r, 0, 2, 5);
        c.dupPct = pick(r, 0, 2);
        c.fifoPct = pick(r, 10, 20, 50);
        c.maxEntries = pick(r, 2, 4, 8, 16);
        c.inflightBatches = pick(r, 2, 4, 8);
        break;
      case 1: // laggard
        c.lossPct = pick(r, 0, 0, 1, 2);
        c.dupPct = pick(r, 0, 1, 3);
        break;
      case 3: // storm
        c.lossPct = pick(r, 1, 3, 5, 10);
        c.dupPct = pick(r, 0, 3, 5);
        break;
      case 4: // lossy
        c.lossPct = pick(r, 10, 20, 30, 40);
        c.dupPct = pick(r, 5, 10, 20);
        break;
      default:
        c.lossPct = pick(r, 0, 2, 5, 10);
        c.dupPct = pick(r, 0, 2, 5);
    }
    if (c.profile == 3) {
      c.electionMin = pick(r, 30, 40, 60);
      c.electionMax = c.electionMin + pick(r, 10, 30, 60);
    } else {
      c.electionMin = pick(r, 100, 200, 400);
      c.electionMax = c.electionMin * 2;
    }
    double u = r.nextDouble();
    c.actions = (int) (120 * Math.exp(u * Math.log(2000.0 / 120.0)));
    long extra = (seed * 0x2545F4914F6CDD1DL) >>> 40;
    c.controlPage = extra % 10 == 0 ? 1024 : 32 * 1024;
    // Like controlPage, from the seed itself: no draw of r, so the other parameters keep.
    c.compaction = ((seed * 0xBF58476D1CE4E5B9L) >>> 41) % 2 == 0;
    String compaction = System.getProperty("traft.fuzz.compaction");
    if (compaction != null) {
      c.compaction = Boolean.parseBoolean(compaction);
    }
    if (Boolean.getBoolean("traft.fuzz.ignoreCap")) {
      c.compaction = true;
      c.ignoreCap = true;
    }
    int actionsMax = Integer.getInteger("traft.fuzz.actionsMax", 0);
    if (actionsMax > 0) {
      c.actions = actionsMax / 4 + (int) (u * actionsMax * 3 / 4);
    }
    c.nodes = Integer.getInteger("traft.fuzz.forceNodes", c.nodes);
    c.maxEntries = Integer.getInteger("traft.fuzz.forceMaxEntries", c.maxEntries);
    c.inflightBatches = Integer.getInteger("traft.fuzz.forceInflight", c.inflightBatches);
    c.theta = Integer.getInteger("traft.fuzz.forceTheta", c.theta);
    if (c.budget > 1) {
      c.budget = c.maxInflightBytes;
    }
    return c;
  }

  static List<Action> actions(Config c) {
    Random r = new Random(c.seed * 0xC2B2AE3D27D4EB4FL + 3);
    int[] w = PROFILE_WEIGHTS[c.profile];
    int total = 0;
    for (int v : w) {
      total += v;
    }
    List<Action> out = new ArrayList<>(c.actions + 40);
    // A leader early, so that most of the budget exercises replication rather than timers.
    int first = r.nextInt(c.nodes);
    out.add(new Action(ELECT, first, -1, 0, 0, 0));
    for (int i = 0; i < 20; i++) {
      out.add(new Action(DELIVER, 3, 0, 0, r.nextDouble(), 1.0));
    }
    for (int i = 0; i < c.actions; i++) {
      int v = r.nextInt(total);
      int type = 0;
      while (v >= w[type]) {
        v -= w[type];
        type++;
      }
      out.add(randomAction(c, r, type));
    }
    return out;
  }

  private static Action randomAction(Config c, Random r, int type) {
    switch (type) {
      case DELIVER:
        return new Action(
            DELIVER,
            1 + r.nextInt(3),
            r.nextInt(100) < c.fifoPct ? 0 : 1,
            0,
            r.nextDouble(),
            r.nextDouble());
      case TICKALL:
        return new Action(TICKALL, 0, 0, 0, 0, 0);
      case TICK:
        return new Action(TICK, r.nextInt(c.nodes), 0, 0, 0, 0);
      case CLIENT:
        {
          int mode = pick(r, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2);
          int count = 1 + r.nextInt(Math.max(2, 2 * c.maxEntries));
          return new Action(CLIENT, mode, count, 0, r.nextDouble(), r.nextDouble());
        }
      case CRASH:
        return new Action(CRASH, r.nextInt(c.nodes), 0, 0, r.nextDouble(), r.nextDouble());
      case CRASHMANY:
        return new Action(CRASHMANY, r.nextInt(1 << c.nodes), 0, 0, r.nextDouble(), r.nextDouble());
      case ARM:
        {
          int filter = c.profile == 5 ? pick(r, 1, 1, 1, 1, 2, 0) : pick(r, 0, 0, 1, 2);
          int mode = pick(r, 0, 1, 1, 2, 2, 2);
          return new Action(
              ARM,
              r.nextInt(c.nodes),
              (1 + r.nextInt(6)) * 10 + filter,
              mode,
              r.nextDouble(),
              r.nextDouble());
        }
      case CUT:
        return new Action(
            CUT,
            r.nextInt(c.nodes),
            r.nextInt(c.nodes),
            r.nextInt(2),
            r.nextDouble(),
            r.nextDouble());
      case ELECT:
        return new Action(
            ELECT, r.nextInt(c.nodes), r.nextInt(3) == 0 ? r.nextInt(c.nodes) : -1, 0, 0, 0);
      case LAG:
        return new Action(LAG, r.nextInt(c.nodes), 0, 0, r.nextDouble(), r.nextDouble());
      case FAILOVER:
        return new Action(
            FAILOVER, r.nextInt(c.nodes), r.nextInt(2), 0, r.nextDouble(), r.nextDouble());
      default:
        throw new IllegalStateException();
    }
  }

  // ---------------------------------------------------------------- result / stats

  static final class Stats {
    long steps;
    long deliveries;
    long overtakes;
    long frames;
    long lost;
    long dups;
    long crashes;
    long armedCrashes;
    long armedEntryCrashes;
    long restarts;
    long tornBatches;
    long forces;
    long entryOnlyBatches;
    long entryOnlyRecords;
    long entryRuns;
    long entryRunsTrue;
    long entryRunsFalse;
    long transferFrames;
    long transferRuns;
    long fetchEntry;
    long nacks;
    long proposals;
    long winners;
    long maxTerm;
    long commits;
    long applied;
    long maxLog;
    long rounds;
    long maxRounds;
    long clientEntries;
    long fallbacks;
    long finalCandidates;
    long finalPendingWork;
    long finalPendingSeeds;
    long finalFetches;
    long converged;
    long compactionSeeds;
    long compactions;
    long boundedChecks;
    // Restarts that replayed a COMPACT record, of which those that lost the newest one.
    long compactedRestarts;
    long compactionRollbacks;
    // Terms won by a replica that had compacted.
    long compactedWinners;

    void add(Stats o) {
      steps += o.steps;
      deliveries += o.deliveries;
      overtakes += o.overtakes;
      frames += o.frames;
      lost += o.lost;
      dups += o.dups;
      crashes += o.crashes;
      armedCrashes += o.armedCrashes;
      armedEntryCrashes += o.armedEntryCrashes;
      restarts += o.restarts;
      tornBatches += o.tornBatches;
      forces += o.forces;
      entryOnlyBatches += o.entryOnlyBatches;
      entryOnlyRecords += o.entryOnlyRecords;
      entryRuns += o.entryRuns;
      entryRunsTrue += o.entryRunsTrue;
      entryRunsFalse += o.entryRunsFalse;
      transferFrames += o.transferFrames;
      transferRuns += o.transferRuns;
      fetchEntry += o.fetchEntry;
      nacks += o.nacks;
      proposals += o.proposals;
      winners += o.winners;
      maxTerm = Math.max(maxTerm, o.maxTerm);
      commits += o.commits;
      applied += o.applied;
      maxLog = Math.max(maxLog, o.maxLog);
      rounds += o.rounds;
      maxRounds = Math.max(maxRounds, o.maxRounds);
      clientEntries += o.clientEntries;
      fallbacks += o.fallbacks;
      finalCandidates += o.finalCandidates;
      finalPendingWork += o.finalPendingWork;
      finalPendingSeeds += o.finalPendingSeeds;
      finalFetches += o.finalFetches;
      converged += o.converged;
      compactionSeeds += o.compactionSeeds;
      compactions += o.compactions;
      boundedChecks += o.boundedChecks;
      compactedRestarts += o.compactedRestarts;
      compactionRollbacks += o.compactionRollbacks;
      compactedWinners += o.compactedWinners;
    }

    @Override
    public String toString() {
      return "steps="
          + steps
          + " deliveries="
          + deliveries
          + " overtakes="
          + overtakes
          + " frames="
          + frames
          + " lost="
          + lost
          + " dups="
          + dups
          + " crashes="
          + crashes
          + " armedCrashes="
          + armedCrashes
          + " armedEntryCrashes="
          + armedEntryCrashes
          + " tornBatches="
          + tornBatches
          + " restarts="
          + restarts
          + " forces="
          + forces
          + " entryOnlyBatches="
          + entryOnlyBatches
          + " entryOnlyRecords="
          + entryOnlyRecords
          + " entryRuns="
          + entryRuns
          + " entryRunsTrue="
          + entryRunsTrue
          + " entryRunsFalse="
          + entryRunsFalse
          + " transferFrames="
          + transferFrames
          + " transferRuns="
          + transferRuns
          + " fetchEntry="
          + fetchEntry
          + " nacks="
          + nacks
          + " proposals="
          + proposals
          + " winners="
          + winners
          + " maxTerm="
          + maxTerm
          + " commits="
          + commits
          + " applied="
          + applied
          + " maxLog="
          + maxLog
          + " clientEntries="
          + clientEntries
          + " fallbacks="
          + fallbacks
          + " converged="
          + converged
          + " finalCandidates="
          + finalCandidates
          + " finalPendingWork="
          + finalPendingWork
          + " finalPendingSeeds="
          + finalPendingSeeds
          + " finalFetches="
          + finalFetches
          + " roundsTotal="
          + rounds
          + " roundsMax="
          + maxRounds
          + " compactionSeeds="
          + compactionSeeds
          + " compactions="
          + compactions
          + " boundedChecks="
          + boundedChecks
          + " compactedRestarts="
          + compactedRestarts
          + " compactionRollbacks="
          + compactionRollbacks
          + " compactedWinners="
          + compactedWinners;
    }
  }

  static final class Result {
    final Config config;
    final String category; // null when clean
    final String detail;
    final Stats stats;
    final long finalDigest;
    final String trace;

    Result(Config config, String category, String detail, Stats stats, long digest, String trace) {
      this.config = config;
      this.category = category;
      this.detail = detail;
      this.stats = stats;
      this.finalDigest = digest;
      this.trace = trace;
    }
  }

  // ---------------------------------------------------------------- simulated cluster

  static boolean ackCheck = !Boolean.getBoolean("traft.fuzz.noAckCheck");
  static boolean countCheck = !Boolean.getBoolean("traft.fuzz.noCountCheck");

  static final class Command {
    final Kind kind;
    final long timestamp;
    final byte[] bytes;

    Command(Kind kind, long timestamp, byte[] bytes) {
      this.kind = kind;
      this.timestamp = timestamp;
      this.bytes = bytes;
    }
  }

  static final class Frame {
    final byte[] bytes;
    final String dataKey;
    final long reservation;
    final boolean deferrable;

    Frame(byte[] bytes, String dataKey, long reservation, boolean deferrable) {
      this.bytes = bytes;
      this.dataKey = dataKey;
      this.reservation = reservation;
      this.deferrable = deferrable;
    }
  }

  /** One RPC request or response on the wire. */
  final class Batch {
    final int from;
    final int to;
    final List<byte[]> frames;
    final List<String> dataKeys;
    final long bytes;
    final Channel channel; // null for a ghost copy
    final boolean response;
    final long seq;
    final Batch request;
    final int toIncarnation;

    Batch(
        int from,
        int to,
        List<byte[]> frames,
        List<String> dataKeys,
        long bytes,
        Channel channel,
        boolean response,
        Batch request,
        int toIncarnation) {
      this.from = from;
      this.to = to;
      this.frames = frames;
      this.dataKeys = dataKeys;
      this.bytes = bytes;
      this.channel = channel;
      this.response = response;
      this.request = request;
      this.toIncarnation = toIncarnation;
      this.seq = batchSeq++;
    }
  }

  /** TRaftGroup.Channel with the network executor replaced by the simulator's wire. */
  final class Channel {
    final int owner;
    final int to;
    final ArrayDeque<Frame> queue = new ArrayDeque<>();
    final ArrayDeque<Frame> deferred = new ArrayDeque<>();
    final Set<String> outstandingData = new HashSet<>();
    int active;
    long reservedBytes;

    Channel(int owner, int to) {
      this.owner = owner;
      this.to = to;
    }

    int tryReserve(long bytes, String dataKey, boolean payloadBudgetAvailable) {
      if (dataKey != null && outstandingData.contains(dataKey)) {
        return 1; // DUPLICATE
      }
      if (bytes < 0 || bytes > cfg.maxInflightBytes - reservedBytes || !payloadBudgetAvailable) {
        return 2; // BYTE_LIMIT
      }
      reservedBytes += bytes;
      if (dataKey != null) {
        outstandingData.add(dataKey);
      }
      return 0; // ACCEPTED
    }

    void release(long bytes) {
      reservedBytes -= bytes;
    }

    void offerReserved(List<Frame> frames, boolean allowDeferral) {
      boolean onlyDeferrable = allowDeferral && !frames.isEmpty();
      for (Frame frame : frames) {
        release(frame.reservation - frame.bytes.length);
        if (!frame.deferrable) {
          onlyDeferrable = false;
        }
      }
      if (onlyDeferrable) {
        deferred.addAll(frames);
        return;
      }
      queue.addAll(deferred);
      deferred.clear();
      queue.addAll(frames);
      dispatch();
    }

    void flushDeferred() {
      if (deferred.isEmpty()) {
        return;
      }
      queue.addAll(deferred);
      deferred.clear();
      dispatch();
    }

    void dispatch() {
      while (active < cfg.inflightBatches && !queue.isEmpty()) {
        List<byte[]> batch = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        long bytes = 0;
        while (!queue.isEmpty()
            && batch.size() < cfg.maxEntries
            && (bytes == 0 || bytes + queue.peekFirst().bytes.length <= cfg.maxBytes)) {
          Frame frame = queue.removeFirst();
          bytes += frame.bytes.length;
          batch.add(frame.bytes);
          if (frame.dataKey != null) {
            keys.add(frame.dataKey);
          }
        }
        active++;
        network.add(new Batch(owner, to, batch, keys, bytes, this, false, null, -1));
      }
    }

    void done(Batch batch) {
      active--;
      release(batch.bytes);
      outstandingData.removeAll(batch.dataKeys);
      dispatch();
    }
  }

  final class Store implements DurableStore {
    final int node;
    final List<DurableRecord> log = new ArrayList<>();
    int forced;
    final Set<EntryId> forcedPayload = new HashSet<>();
    final Set<EntryId> forcedInstall = new HashSet<>();
    final Set<EntryId> forcedManifest = new HashSet<>();
    final Map<EntryId, Proposal> forcedRetained = new HashMap<>();
    // Pipelined boundaries: forced ENTRY metadata, and the (partition, term) of every forced
    // retained opening, so that a staged entry under a forced opening counts as certified.
    final Map<EntryId, Entry> forcedEntries = new HashMap<>();
    final Set<String> forcedOpenings = new HashSet<>();
    final Map<Long, Integer> forcedVote = new HashMap<>();
    long maxForcedTerm;
    long writtenVoteTerm = -1;
    int writtenVote = -1;
    final Map<EntryId, Entry> written = new HashMap<>();
    int armOps = -1;
    int armFilter;
    int armMode;
    double armSurvive;
    double armDown;

    Store(int node) {
      this.node = node;
    }

    /**
     * Like TRaftFileStore: the index is final (pruned at each COMPACT) after the crash rebuild, and
     * iterating the loaded records re-indexes them in step, pruning where they did.
     */
    @Override
    public List<DurableRecord> load() {
      List<DurableRecord> records = new ArrayList<>(log);
      return new AbstractList<DurableRecord>() {
        @Override
        public int size() {
          return records.size();
        }

        @Override
        public DurableRecord get(int index) {
          return records.get(index);
        }

        @Override
        public Iterator<DurableRecord> iterator() {
          Iterator<DurableRecord> replay = records.iterator();
          return new Iterator<DurableRecord>() {
            @Override
            public boolean hasNext() {
              return replay.hasNext();
            }

            @Override
            public DurableRecord next() {
              DurableRecord record = replay.next();
              index(record);
              return record;
            }
          };
        }
      };
    }

    private void index(DurableRecord r) {
      if (r.entry != null) {
        written.putIfAbsent(r.entry.id, Entry.metadata(r.entry));
      }
      if (r.proposal != null) {
        written.putIfAbsent(r.proposal.id(), Entry.metadata(r.proposal.first));
      }
      if (r.type == RecordType.COMPACT) {
        written.values().removeIf(e -> e.partition <= r.frontier);
      }
    }

    /** The same checks TRaftFileStore.verifyEntry applies; a rejection fences the engine. */
    private void verifyLikeFileStore(Entry entry) throws IOException {
      if (entry == null) {
        return;
      }
      Entry previous = written.get(entry.id);
      if (previous == null) {
        if (!entry.hasPayload()) {
          throw new IOException(
              "Metadata references an entry without durable payload: " + entry.id);
        }
        written.put(entry.id, Entry.metadata(entry));
      } else if (!previous.equals(entry)) {
        throw new IOException("Durable entry identity changed: " + entry.id);
      }
    }

    @Override
    public Entry residentEntry(Entry entry) {
      if (!written.containsKey(entry.id)) {
        throw new IllegalStateException("Entry is not durable: " + entry.id);
      }
      return entry;
    }

    private DurableRecord roundTrip(DurableRecord record) throws IOException {
      registerRecord(record);
      verifyLikeFileStore(record.entry);
      if (record.proposal != null) {
        verifyLikeFileStore(record.proposal.first);
      }
      if (record.type == RecordType.COMPACT) {
        // TRaftFileStore prunes its payload index here; later metadata naming one fails closed.
        written.values().removeIf(e -> e.partition <= record.frontier);
      }
      if (record.type == RecordType.TERM_VOTE) {
        if (record.term == writtenVoteTerm
            && writtenVote != -1
            && record.vote != -1
            && record.vote != writtenVote) {
          throw new Violation(
              "DOUBLE_VOTE",
              "n"
                  + node
                  + " term "
                  + record.term
                  + " voted "
                  + writtenVote
                  + " and "
                  + record.vote);
        }
        if (record.term != writtenVoteTerm || record.vote != -1) {
          writtenVoteTerm = record.term;
          writtenVote = record.vote;
        }
      }
      return TRaftCodec.decodeRecord(TRaftCodec.encodeRecord(record));
    }

    private boolean matchesArm(List<DurableRecord> records) {
      if (armOps < 0) {
        return false;
      }
      boolean match;
      switch (armFilter) {
        case 1: // staging or repair payload only
          match = true;
          for (DurableRecord r : records) {
            match &= r.type == RecordType.ENTRY;
          }
          break;
        case 2: // an installation batch
          match = true;
          for (DurableRecord r : records) {
            match &= r.type == RecordType.INSTALL && r.entry != null;
          }
          break;
        default:
          match = true;
      }
      if (!match) {
        return false;
      }
      armOps--;
      return armOps <= 0;
    }

    private SimCrash fire(List<DurableRecord> records) {
      armOps = -1;
      stats.armedCrashes++;
      boolean entryOnly = true;
      for (DurableRecord r : records) {
        entryOnly &= r.type == RecordType.ENTRY;
      }
      if (entryOnly) {
        stats.armedEntryCrashes++;
      }
      if (armMode == 1 && records.size() > 1) {
        stats.tornBatches++;
      }
      trace("  store crash at n" + node + " mode=" + armMode + " " + describe(records));
      return new SimCrash(armSurvive, armDown);
    }

    @Override
    public void append(DurableRecord record) throws IOException {
      List<DurableRecord> one = Collections.singletonList(record);
      boolean fire = matchesArm(one);
      if (fire && armMode == 0) {
        throw fire(one);
      }
      log.add(roundTrip(record));
      if (fire && armMode == 1) {
        throw fire(one);
      }
      force();
      if (fire && armMode == 2) {
        throw fire(one);
      }
    }

    @Override
    public void appendDeferred(DurableRecord record) throws IOException {
      // Compacting seeds can also crash around a COMPACT record, which is then easily lost: it
      // stays unforced until the next force. Other seeds keep their armed-crash schedule.
      List<DurableRecord> one = Collections.singletonList(record);
      boolean fire = cfg.compaction && record.type == RecordType.COMPACT && matchesArm(one);
      if (fire && armMode == 0) {
        throw fire(one);
      }
      log.add(roundTrip(record));
      if (fire) {
        throw fire(one);
      }
    }

    @Override
    public void appendBatch(List<DurableRecord> records) throws IOException {
      if (records.isEmpty()) {
        return;
      }
      boolean fire = matchesArm(records);
      boolean entryOnly = true;
      for (DurableRecord r : records) {
        entryOnly &= r.type == RecordType.ENTRY;
      }
      if (entryOnly) {
        stats.entryOnlyBatches++;
        stats.entryOnlyRecords += records.size();
      }
      if (fire && armMode == 0) {
        throw fire(records);
      }
      for (DurableRecord record : records) {
        log.add(roundTrip(record));
      }
      if (fire && armMode == 1) {
        throw fire(records);
      }
      force();
      if (fire && armMode == 2) {
        throw fire(records);
      }
    }

    void forcePending() {
      if (forced < log.size()) {
        // Compacting seeds can also die just before the ticker's force: the deferred COMPACT
        // and INSTALL records written since the last force may then be lost.
        List<DurableRecord> pending = new ArrayList<>(log.subList(forced, log.size()));
        if (cfg.compaction && matchesArm(pending)) {
          throw fire(pending);
        }
        force();
      }
    }

    private void force() {
      stats.forces++;
      markDurable();
    }

    private void markDurable() {
      for (; forced < log.size(); forced++) {
        DurableRecord r = log.get(forced);
        switch (r.type) {
          case TERM_VOTE:
            forcedVote.put(r.term, r.vote);
            maxForcedTerm = Math.max(maxForcedTerm, r.term);
            break;
          case ENTRY:
            forcedPayload.add(r.entry.id);
            forcedEntries.put(r.entry.id, r.entry);
            break;
          case INSTALL:
            if (r.entry != null) {
              forcedPayload.add(r.entry.id);
              forcedInstall.add(r.entry.id);
            }
            break;
          case RETAIN_PROPOSAL:
            forcedRetained.put(r.proposal.id(), r.proposal);
            forcedManifest.add(r.proposal.id());
            forcedManifest.addAll(r.proposal.members);
            forcedOpenings.add(openingKey(r.proposal.first.partition, r.proposal.first.id.term));
            break;
          case COMMIT:
            for (EntryId id : r.identities) {
              Long old = globalCommitted.get(id);
              if (old == null || r.term < old) {
                globalCommitted.put(id, r.term);
              }
            }
            break;
          default:
            break;
        }
      }
    }

    /** The process died: keep every forced record and a random prefix of the unforced tail. */
    void crash(double survive) {
      int extra = log.size() - forced;
      int keep = 0;
      if (extra > 0 && survive >= 0.5) {
        keep = Math.min(extra, (int) ((survive - 0.5) * 2 * (extra + 1)));
      }
      if (log.size() > forced + keep) {
        trace("  n" + node + " loses " + (log.size() - forced - keep) + " unforced records");
        log.subList(forced + keep, log.size()).clear();
      }
      markDurable();
      armOps = -1;
      writtenVoteTerm = -1;
      writtenVote = -1;
      written.clear();
      for (DurableRecord r : log) {
        if (r.type == RecordType.TERM_VOTE && (r.term != writtenVoteTerm || r.vote != -1)) {
          writtenVoteTerm = r.term;
          writtenVote = r.vote;
        }
        index(r);
      }
    }

    /**
     * Durable evidence an ACK may certify: a forced payload that is installed, named by a forced
     * manifest, or (pipelined boundaries) staged in a partition whose opening is forced retained.
     */
    boolean certified(EntryId id) {
      if (!forcedPayload.contains(id)) {
        return false;
      }
      if (forcedInstall.contains(id) || forcedManifest.contains(id)) {
        return true;
      }
      Entry entry = forcedEntries.get(id);
      return entry != null && forcedOpenings.contains(openingKey(entry.partition, entry.id.term));
    }

    private String openingKey(long partition, long term) {
      return partition + ":" + term;
    }
  }

  final class Node implements Application {
    final int id;
    final Store store;
    final Random timerRand;
    TRaftEngine engine;
    boolean up;
    int incarnation;
    long restartAt = -1;
    final Map<Integer, Channel> channels = new LinkedHashMap<>();
    final ArrayDeque<Command> admissions = new ArrayDeque<>();
    long deadline;
    long lastHeartbeat;
    Role prevRole = Role.FOLLOWER;
    long prevTerm;
    final Set<EntryId> appState = new HashSet<>();
    final Map<Long, Set<EntryId>> appliedByPartition = new HashMap<>();
    long lastAppliedPartition;
    // The compaction anchor this incarnation was last checked at (COMPACTION_UNSAFE).
    EntryId checkedAnchor;
    // The frontier of the incarnation that crashed last.
    long frontierAtCrash = -1;

    Node(int id) {
      this.id = id;
      this.store = new Store(id);
      this.timerRand = new Random(cfg.seed * 31 + id);
    }

    @Override
    public void apply(Entry entry) throws IOException {
      onApply(this, entry);
    }

    @Override
    public boolean requiresDurableAppliedRecords() {
      return !cfg.volatileMode;
    }

    @Override
    public boolean requiresDurableCommitRecords() {
      return !cfg.volatileMode;
    }
  }

  final Config cfg;
  final List<Action> script;
  final Stats stats = new Stats();
  final StringBuilder traceBuffer;
  final Node[] nodes;
  final Set<Integer> members = new HashSet<>();
  final int quorum;
  final List<Batch> network = new ArrayList<>();
  final Map<EntryId, Long> globalCommitted = new HashMap<>();
  final Map<EntryId, Entry> registry = new HashMap<>();
  final Map<Long, Integer> winners = new HashMap<>();
  final Map<String, Integer> votesGiven = new HashMap<>();
  final Map<Long, Set<EntryId>> canonicalClosed = new HashMap<>();
  final long[] lagUntil;
  final long[][] cutUntil;
  long batchSeq;
  long step;
  long now = 1000;
  long clientClock = 1;
  long tokenCounter;
  Random healRandom;
  boolean healing;
  int lossPct;
  int dupPct;

  TRaftFuzz(Config cfg, List<Action> script, boolean trace) {
    this.cfg = cfg;
    this.script = script;
    this.traceBuffer = trace ? new StringBuilder() : null;
    this.nodes = new Node[cfg.nodes + 1];
    this.quorum = cfg.nodes / 2 + 1;
    this.lagUntil = new long[cfg.nodes + 1];
    this.cutUntil = new long[cfg.nodes + 1][cfg.nodes + 1];
    this.lossPct = cfg.lossPct;
    this.dupPct = cfg.dupPct;
    for (int i = 1; i <= cfg.nodes; i++) {
      members.add(i);
    }
    for (int i = 1; i <= cfg.nodes; i++) {
      nodes[i] = new Node(i);
    }
  }

  void trace(String line) {
    if (traceBuffer != null) {
      traceBuffer.append(line).append('\n');
    }
  }

  static String describe(List<DurableRecord> records) {
    StringBuilder b = new StringBuilder("[");
    for (DurableRecord r : records) {
      b.append(r.type);
      if (r.entry != null) {
        b.append(' ').append(r.entry.id).append("/p").append(r.entry.partition);
      }
      if (r.proposal != null) {
        b.append(' ').append(r.proposal.id()).append("/p").append(r.proposal.first.partition);
      }
      if (r.reference != null) {
        b.append(" ref=").append(r.reference);
      }
      b.append(';');
    }
    return b.append(']').toString();
  }

  static String describe(Message m) {
    StringBuilder b = new StringBuilder();
    b.append(m.type)
        .append(' ')
        .append(m.from)
        .append("->")
        .append(m.to)
        .append(" t")
        .append(m.term);
    if (m.entry != null) {
      b.append(" e=").append(m.entry.id).append("/p").append(m.entry.partition);
      b.append(m.entry.hasPayload() ? "/" + m.entry.kind : "/meta");
    }
    if (m.proposal != null) {
      b.append(" P=")
          .append(m.proposal.id())
          .append("/p")
          .append(m.proposal.first.partition)
          .append(" base=")
          .append(m.proposal.base)
          .append(" members=")
          .append(m.proposal.members)
          .append(m.proposal.recovery ? " rec" : "")
          .append(m.proposal.first.hasPayload() ? " carried" : "");
    }
    if (m.reference != null) {
      b.append(" ref=").append(m.reference);
    }
    if (m.frontier != -1) {
      b.append(" frontier=").append(m.frontier);
    }
    if (!m.identities.isEmpty()) {
      b.append(" ids=").append(m.identities);
    }
    return b.toString();
  }

  void registerRecord(DurableRecord r) {
    if (r.entry != null) {
      register(r.entry);
    }
    if (r.proposal != null) {
      register(r.proposal.first);
    }
  }

  void register(Entry entry) {
    Entry old = registry.get(entry.id);
    if (old == null) {
      registry.put(entry.id, entry);
    } else if (!old.equals(entry)) {
      throw new Violation("IDENTITY_CONFLICT", entry.id + " has two contents");
    }
  }

  // ---------------------------------------------------------------- application and commit oracle

  void onApply(Node n, Entry entry) throws IOException {
    if (n.appState.contains(entry.id)) {
      return;
    }
    if (!globalCommitted.containsKey(entry.id)) {
      throw new Violation("APPLY_UNCOMMITTED", "n" + n.id + " applied " + entry.id);
    }
    n.appState.add(entry.id);
    stats.applied++;
    Entry known = registry.get(entry.id);
    if (known == null || !known.equals(entry)) {
      throw new Violation("APPLY_CONTENT", "n" + n.id + " applied changed " + entry.id);
    }
    if (entry.partition < n.lastAppliedPartition) {
      throw new Violation(
          "APPLY_ORDER",
          "n"
              + n.id
              + " applied "
              + entry.id
              + "/p"
              + entry.partition
              + " after partition "
              + n.lastAppliedPartition);
    }
    for (long q = n.lastAppliedPartition; q < entry.partition; q++) {
      Set<EntryId> set = n.appliedByPartition.getOrDefault(q, Collections.<EntryId>emptySet());
      Set<EntryId> canon = canonicalClosed.get(q);
      if (canon == null) {
        canonicalClosed.put(q, new HashSet<>(set));
      } else if (!canon.equals(set)) {
        throw new Violation(
            "APPLY_DIVERGENCE",
            "n" + n.id + " closed partition " + q + " applied=" + set + " canonical=" + canon);
      }
    }
    n.lastAppliedPartition = entry.partition;
    n.appliedByPartition.computeIfAbsent(entry.partition, ignored -> new HashSet<>()).add(entry.id);
  }

  void onCommit(Node n, EntryId id) {
    long term = n.engine.term();
    Role role = n.engine.role();
    Long old = globalCommitted.get(id);
    if (old == null || term < old) {
      globalCommitted.put(id, term);
    }
    if (old == null) {
      stats.commits++;
    }
    if (countCheck && (role == Role.LEADER || role == Role.LEADER_CANDIDATE)) {
      int durable = 0;
      for (int i = 1; i <= cfg.nodes; i++) {
        if (nodes[i].store.certified(id)) {
          durable++;
        }
      }
      if (durable < quorum) {
        throw new Violation(
            "COUNT_NOT_DURABLE",
            "n" + n.id + " committed " + id + " with " + durable + " durable replicas");
      }
    }
  }

  void checkOutgoing(Node n, Message m) {
    if (m.type == MessageType.VOTE) {
      String key = n.id + "/" + m.term;
      Integer prior = votesGiven.putIfAbsent(key, m.to);
      if (prior != null && prior != m.to) {
        throw new Violation(
            "DOUBLE_VOTE", "n" + n.id + " voted " + prior + " and " + m.to + " in term " + m.term);
      }
    }
    if (m.type == MessageType.NACK && m.reference != null) {
      stats.nacks++;
    }
    if (m.type == MessageType.FETCH_ENTRY) {
      stats.fetchEntry++;
    }
    if (m.type == MessageType.PROPOSAL) {
      stats.proposals++;
    }
    if (!ackCheck) {
      return;
    }
    Store s = n.store;
    if (s.maxForcedTerm < m.term) {
      throw new Violation("TERM_NOT_DURABLE:" + m.type, "n" + n.id + " " + describe(m));
    }
    switch (m.type) {
      case ENTRY_ACK:
        checkEntryAck(n, m.reference, m);
        for (EntryId id : m.identities) {
          checkEntryAck(n, id, m);
        }
        break;
      case PROPOSAL_ACK:
        {
          Proposal p = s.forcedRetained.get(m.reference);
          if (p == null) {
            throw new Violation(
                "ACK_NOT_DURABLE:PROPOSAL_ACK", "n" + n.id + " " + describe(m) + " not retained");
          }
          if (p.base != null && !s.forcedRetained.containsKey(p.base)) {
            throw new Violation(
                "ACK_NOT_DURABLE:PROPOSAL_BASE", "n" + n.id + " " + describe(m) + " base missing");
          }
          for (EntryId id : p.members) {
            if (!s.forcedPayload.contains(id)) {
              throw new Violation(
                  "ACK_NOT_DURABLE:PROPOSAL_MEMBER",
                  "n" + n.id + " " + describe(m) + " member " + id);
            }
          }
          if (!s.forcedPayload.contains(p.id())) {
            throw new Violation("ACK_NOT_DURABLE:PROPOSAL_FIRST", "n" + n.id + " " + describe(m));
          }
          break;
        }
      case TRANSFER_ACK:
        if (!s.forcedPayload.contains(m.reference)) {
          throw new Violation("ACK_NOT_DURABLE:TRANSFER_ACK", "n" + n.id + " " + describe(m));
        }
        break;
      case VOTE:
        {
          Integer vote = s.forcedVote.get(m.term);
          if (vote == null || vote != m.to) {
            throw new Violation("ACK_NOT_DURABLE:VOTE", "n" + n.id + " " + describe(m));
          }
          break;
        }
      default:
        break;
    }
  }

  void checkEntryAck(Node n, EntryId id, Message m) {
    if (!n.store.certified(id)) {
      throw new Violation(
          "ACK_NOT_DURABLE:ENTRY_ACK",
          "n" + n.id + " acked " + id + " without forced install/manifest: " + describe(m));
    }
  }

  void leaderCompleteness(Node n) {
    Set<EntryId> have = new HashSet<>();
    for (Entry e : logical(n)) {
      have.add(e.id);
    }
    long term = n.engine.term();
    for (Map.Entry<EntryId, Long> c : globalCommitted.entrySet()) {
      if (c.getValue() <= term && !have.contains(c.getKey())) {
        throw new Violation(
            "COMMITTED_LOST",
            "leader n"
                + n.id
                + " term "
                + term
                + " lacks "
                + c.getKey()
                + " committed in term "
                + c.getValue());
      }
    }
  }

  TreeMap<Long, Map<EntryId, Entry>> view(Node n) {
    TreeMap<Long, Map<EntryId, Entry>> parts = new TreeMap<>();
    for (Entry e : logical(n)) {
      parts.computeIfAbsent(e.partition, ignored -> new LinkedHashMap<>()).put(e.id, e);
    }
    return parts;
  }

  // ------------------------------------------------------------- compaction: logical views

  /** Compacted prefix per anchor: the members of the anchor's chain, from the forced manifests. */
  final Map<EntryId, List<Entry>> compactedPrefixes = new HashMap<>();

  /** The node's whole installed log: its compacted prefix followed by the engine's suffix. */
  List<Entry> logical(Node n) {
    List<Entry> suffix = n.engine.installedEntries();
    EntryId anchor = n.engine.compactionAnchor();
    if (anchor == null) {
      return suffix;
    }
    List<Entry> all = new ArrayList<>(compactedPrefix(n, anchor));
    all.addAll(suffix);
    return all;
  }

  Set<EntryId> logicalCommitted(Node n) {
    Set<EntryId> committed = new HashSet<>(n.engine.committedIds());
    EntryId anchor = n.engine.compactionAnchor();
    if (anchor != null) {
      for (Entry e : compactedPrefix(n, anchor)) {
        committed.add(e.id);
      }
    }
    return committed;
  }

  /** The members of every proposal from the anchor down to the root, in partition order. */
  List<Entry> compactedPrefix(Node n, EntryId anchor) {
    List<Entry> known = compactedPrefixes.get(anchor);
    if (known != null) {
      return known;
    }
    List<Proposal> chain = new ArrayList<>();
    Proposal cursor = n.store.forcedRetained.get(anchor);
    List<Entry> base = Collections.emptyList();
    while (cursor != null) {
      // The anchor itself is not memoized yet; an older anchor on its chain may be.
      if (compactedPrefixes.containsKey(cursor.id())) {
        base = compactedPrefixes.get(cursor.id());
        break;
      }
      chain.add(cursor);
      cursor = cursor.base == null ? null : n.store.forcedRetained.get(cursor.base);
      if (cursor == null && chain.get(chain.size() - 1).base != null) {
        throw new Violation(
            "COMPACTION_UNSAFE", "n" + n.id + " lacks its own forced chain below " + anchor);
      }
    }
    if (chain.isEmpty()) {
      throw new Violation("COMPACTION_UNSAFE", "n" + n.id + " lacks its forced anchor " + anchor);
    }
    List<Entry> prefix = new ArrayList<>(base);
    for (int i = chain.size() - 1; i >= 0; i--) {
      List<Entry> members = new ArrayList<>();
      for (EntryId id : chain.get(i).members) {
        members.add(registry.get(id));
      }
      members.sort(Comparator.comparingLong(e -> e.id.index));
      prefix.addAll(members);
    }
    List<Entry> result = Collections.unmodifiableList(prefix);
    compactedPrefixes.put(anchor, result);
    return result;
  }

  /**
   * After a node compacts, every other node, up or down, must durably hold the compacted chain: the
   * forced manifests from the new anchor down to the previous one and every payload they name.
   */
  void checkCompaction(Node n) {
    EntryId anchor = n.engine.compactionAnchor();
    if (anchor == null || anchor.equals(n.checkedAnchor)) {
      return;
    }
    stats.compactions++;
    trace("  n" + n.id + " compacted through " + n.engine.compactedThrough() + " anchor " + anchor);
    Proposal cursor = n.store.forcedRetained.get(anchor);
    while (cursor != null) {
      for (int i = 1; i <= cfg.nodes; i++) {
        if (i == n.id) {
          continue;
        }
        Store other = nodes[i].store;
        Proposal held = other.forcedRetained.get(cursor.id());
        if (held == null
            || !held.equals(cursor)
            || !other.forcedPayload.contains(cursor.id())
            || !other.forcedPayload.containsAll(cursor.members)) {
          throw new Violation(
              "COMPACTION_UNSAFE",
              "n"
                  + n.id
                  + " compacted through "
                  + n.engine.compactedThrough()
                  + " but n"
                  + i
                  + " does not durably hold "
                  + cursor.id()
                  + "/p"
                  + cursor.first.partition);
        }
      }
      if (cursor.id().equals(n.checkedAnchor) || cursor.base == null) {
        break;
      }
      cursor = n.store.forcedRetained.get(cursor.base);
    }
    compactedPrefix(n, anchor);
    n.checkedAnchor = anchor;
  }

  void fullCheck() {
    List<Node> live = new ArrayList<>();
    List<TreeMap<Long, Map<EntryId, Entry>>> views = new ArrayList<>();
    for (int i = 1; i <= cfg.nodes; i++) {
      Node n = nodes[i];
      if (!n.up) {
        continue;
      }
      if (n.engine.role() == Role.LEADER) {
        leaderCompleteness(n);
      }
      try {
        n.engine.checkCompactionInvariants();
      } catch (IllegalStateException e) {
        throw new Violation("COMPACTION_INVARIANT", "n" + n.id + " " + e.getMessage());
      }
      TreeMap<Long, Map<EntryId, Entry>> parts = view(n);
      stats.maxLog = Math.max(stats.maxLog, n.engine.installedEntries().size());
      long expected = 0;
      for (Map.Entry<Long, Map<EntryId, Entry>> part : parts.entrySet()) {
        if (part.getKey() != expected) {
          throw new Violation("LOG_SHAPE:GAP", "n" + n.id + " partition " + part.getKey());
        }
        expected++;
        long term = -1;
        for (Entry e : part.getValue().values()) {
          if (term == -1) {
            term = e.id.term;
          } else if (term != e.id.term) {
            throw new Violation(
                "LOG_SHAPE:TERM", "n" + n.id + " partition " + part.getKey() + " mixes terms");
          }
          long prevSize = part.getKey() == 0 ? 0 : parts.get(part.getKey() - 1).size();
          if (e.prevSize != prevSize) {
            throw new Violation(
                "LOG_SHAPE:PREVSIZE",
                "n"
                    + n.id
                    + " partition "
                    + part.getKey()
                    + " entry "
                    + e.id
                    + " prevSize="
                    + e.prevSize
                    + " actual="
                    + prevSize);
          }
        }
      }
      live.add(n);
      views.add(parts);
    }
    for (int i = 0; i < live.size(); i++) {
      for (int j = i + 1; j < live.size(); j++) {
        compareLogs(live.get(i), views.get(i), live.get(j), views.get(j));
      }
    }
  }

  private static long partTerm(Map<EntryId, Entry> part) {
    return part.values().iterator().next().id.term;
  }

  void compareLogs(
      Node na,
      TreeMap<Long, Map<EntryId, Entry>> a,
      Node nb,
      TreeMap<Long, Map<EntryId, Entry>> b) {
    if (a.isEmpty() || b.isEmpty()) {
      return;
    }
    long curA = a.lastKey();
    long curB = b.lastKey();
    long top = Math.min(curA, curB);
    for (long p = top; p >= 0; p--) {
      if (partTerm(a.get(p)) == partTerm(b.get(p))) {
        long limit = p < curA && p < curB ? p : p - 1;
        for (long q = limit; q >= 0; q--) {
          if (!a.get(q).keySet().equals(b.get(q).keySet())) {
            throw new Violation(
                "LOG_MATCH",
                "n"
                    + na.id
                    + " and n"
                    + nb.id
                    + " share (term "
                    + partTerm(a.get(p))
                    + ",partition "
                    + p
                    + ") but partition "
                    + q
                    + " is "
                    + a.get(q).keySet()
                    + " versus "
                    + b.get(q).keySet());
          }
        }
        return;
      }
    }
  }

  // ---------------------------------------------------------------- TRaftGroup: drive and receive

  void resetElection(Node n) {
    n.deadline =
        now
            + cfg.electionMin
            + (long) (n.timerRand.nextDouble() * Math.max(1, cfg.electionMax - cfg.electionMin));
  }

  static String dataKey(Message message) {
    return message.entry != null
            && (message.type == MessageType.ENTRY
                || message.type == MessageType.TRANSFER_ENTRY
                || message.type == MessageType.PROPOSAL)
        ? message.term + "/" + message.type + "/" + message.entry.id
        : null;
  }

  static final class ControlKey {
    final Message message;

    ControlKey(Message message) {
      this.message = message;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ControlKey)) {
        return false;
      }
      Message that = ((ControlKey) other).message;
      return message.type == that.type
          && message.from == that.from
          && message.to == that.to
          && message.term == that.term
          && Objects.equals(message.reference, that.reference)
          && Objects.equals(message.proposal, that.proposal);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          message.type,
          message.from,
          message.to,
          message.term,
          message.reference,
          message.proposal);
    }
  }

  static void coalesceControlMessages(List<Message> messages) {
    Set<ControlKey> seen = new HashSet<>();
    Iterator<Message> iterator = messages.iterator();
    while (iterator.hasNext()) {
      Message message = iterator.next();
      if (message.entry != null
          || message.rank != null
          || message.report != null
          || message.frontier != -1
          || !message.identities.isEmpty()) {
        continue;
      }
      boolean acknowledgement =
          (message.type == MessageType.ENTRY_ACK
                  || message.type == MessageType.PROPOSAL_ACK
                  || message.type == MessageType.TRANSFER_ACK)
              && message.reference != null
              && message.proposal == null;
      boolean proposal =
          (message.type == MessageType.PROPOSAL || message.type == MessageType.CONFIRM)
              && message.proposal != null
              && message.reference == null;
      if ((acknowledgement || proposal) && !seen.add(new ControlKey(message))) {
        iterator.remove();
      }
    }
  }

  boolean reserveFrame(
      Node n, Channel channel, Message message, long bytes, String dataKey, boolean budget) {
    if (message.type == MessageType.PROPOSAL
        && message.entry != null
        && !n.engine.isProposalPayloadInFlight(message.to, message.entry.id)) {
      return false;
    }
    boolean forceFallback =
        cfg.fallbackPct > 0
            && !healing
            && message.type == MessageType.PROPOSAL
            && message.entry != null
            && n.timerRand.nextInt(100) < cfg.fallbackPct;
    int result = forceFallback ? 2 : channel.tryReserve(bytes, dataKey, budget);
    if (result == 2 && message.type == MessageType.PROPOSAL && message.entry != null) {
      stats.fallbacks++;
      n.engine.fallbackProposalPayload(message.to, message.entry.id);
    }
    return result == 0;
  }

  int appendOrdinaryBatch(Node n, int remaining) throws IOException {
    if (cfg.nodes == 1 || remaining < 2 || n.admissions.size() < 2) {
      return 0;
    }
    List<Command> commands = new ArrayList<>();
    List<TRaftEngine.DataInput> inputs = new ArrayList<>();
    long bytes = 0;
    long limit = Math.min(cfg.maxBytes, cfg.maxInflightBytes);
    for (Command command : n.admissions) {
      long size = command.bytes.length + 256;
      if (command.kind != Kind.DATA
          || inputs.size() >= remaining
          || size > limit - bytes
          || (!commands.isEmpty()
              && command.timestamp <= commands.get(commands.size() - 1).timestamp)) {
        break;
      }
      commands.add(command);
      inputs.add(new TRaftEngine.DataInput(command.timestamp, command.bytes));
      bytes += size;
    }
    if (inputs.size() < 2) {
      return 0;
    }
    List<Entry> accepted = n.engine.appendDataBatch(inputs, now);
    for (int i = 0; i < accepted.size(); i++) {
      n.admissions.removeFirst();
      stats.clientEntries++;
    }
    return accepted.size();
  }

  List<byte[]> drive(Node n, int replyTo) throws IOException {
    TRaftEngine engine = n.engine;
    if (n.prevRole == Role.LEADER
        && (engine.role() != Role.LEADER || engine.term() != n.prevTerm)) {
      n.admissions.clear();
    }
    if (engine.role() == Role.LEADER) {
      int admitted = 0;
      while (!n.admissions.isEmpty() && !engine.hasPendingProposal() && admitted < cfg.maxEntries) {
        int batched = appendOrdinaryBatch(n, cfg.maxEntries - admitted);
        if (batched != 0) {
          admitted += batched;
          continue;
        }
        Command c = n.admissions.peekFirst();
        Entry entry = engine.append(c.kind, c.timestamp, c.bytes, now);
        if (entry == null) {
          break;
        }
        trace("  n" + n.id + " append " + entry.id + "/p" + entry.partition + "/" + entry.kind);
        stats.clientEntries++;
        n.admissions.removeFirst();
        admitted++;
      }
    }
    Map<EntryId, byte[]> roundPayloads = new HashMap<>();
    long roundPayloadBytes = 0;
    List<Message> outgoing = engine.drainMessages();
    coalesceControlMessages(outgoing);
    outgoing.sort(Comparator.comparingInt(m -> m.entry != null && m.entry.hasPayload() ? 1 : 0));
    Map<Channel, List<Frame>> batches = new LinkedHashMap<>();
    List<byte[]> replies = new ArrayList<>();
    for (Message m : outgoing) {
      checkOutgoing(n, m);
      if (!members.contains(m.to) || m.to == n.id) {
        continue;
      }
      if (m.to == replyTo && (m.entry == null || !m.entry.hasPayload())) {
        replies.add(TRaftCodec.encodeMessage(m, roundPayloads));
        continue;
      }
      long newPayloadBytes =
          m.entry != null && m.entry.hasPayload() && !roundPayloads.containsKey(m.entry.id)
              ? m.entry.payloadSize()
              : 0;
      Channel channel = n.channels.get(m.to);
      if (channel == null) {
        channel = new Channel(n.id, m.to);
        n.channels.put(m.to, channel);
      }
      String key = dataKey(m);
      long reservation = TRaftCodec.encodedMessageUpperBound(m);
      if (!reserveFrame(
          n,
          channel,
          m,
          reservation,
          key,
          newPayloadBytes <= cfg.maxInflightBytes - roundPayloadBytes)) {
        if (traceBuffer != null) {
          trace("  not reserved at n" + n.id + ": " + describe(m) + " key=" + key);
        }
        continue;
      }
      if (traceBuffer != null) {
        trace("  snd " + describe(m));
      }
      byte[] encoded = TRaftCodec.encodeMessage(m, roundPayloads);
      if (encoded.length > reservation) {
        throw new Violation("FRAME_RESERVATION", describe(m));
      }
      roundPayloadBytes += newPayloadBytes;
      channel.release(reservation - encoded.length);
      List<Frame> list = batches.get(channel);
      if (list == null) {
        list = new ArrayList<>();
        batches.put(channel, list);
      }
      list.add(
          new Frame(
              encoded,
              key,
              encoded.length,
              m.type == MessageType.COMMIT || m.type == MessageType.CONFIRM));
    }
    for (Map.Entry<Channel, List<Frame>> batch : batches.entrySet()) {
      batch.getKey().offerReserved(batch.getValue(), true);
    }
    engine.drainApplied();
    publish(n);
    return replies;
  }

  void publish(Node n) {
    checkCompaction(n);
    Role role = n.engine.role();
    long term = n.engine.term();
    if (term < n.prevTerm) {
      throw new Violation("TERM_REGRESSION", "n" + n.id + " " + n.prevTerm + " to " + term);
    }
    if (role == Role.LEADER_CANDIDATE || role == Role.LEADER) {
      Integer winner = winners.putIfAbsent(term, n.id);
      if (winner == null) {
        stats.winners++;
        if (n.engine.compactedThrough() >= 0) {
          stats.compactedWinners++;
        }
        trace("  n" + n.id + " wins term " + term);
      } else if (winner != n.id) {
        throw new Violation(
            "ELECTION_SAFETY", "term " + term + " won by n" + winner + " and n" + n.id);
      }
    }
    if (role == Role.LEADER && (n.prevRole != Role.LEADER || n.prevTerm != term)) {
      trace("  n" + n.id + " is LEADER of term " + term);
      leaderCompleteness(n);
    }
    stats.maxTerm = Math.max(stats.maxTerm, term);
    n.prevRole = role;
    n.prevTerm = term;
  }

  void receiveMessage(Node n, Message message) throws IOException {
    long progress = n.engine.electionProgress();
    n.engine.receive(message);
    if (n.engine.electionProgress() != progress
        || (message.term == n.engine.term() && message.from == n.engine.leaderId())) {
      resetElection(n);
    }
  }

  void receiveEntryRun(Node n, List<Message> entries) throws IOException {
    if (entries.isEmpty()) {
      return;
    }
    if (entries.size() > 1) {
      stats.entryRuns++;
      if (n.engine.receiveEntries(entries)) {
        stats.entryRunsTrue++;
        resetElection(n);
        return;
      }
      stats.entryRunsFalse++;
    }
    for (Message entry : entries) {
      receiveMessage(n, entry);
    }
  }

  void receiveTransferRun(Node n, List<Message> transfers) throws IOException {
    if (transfers.size() < 2) {
      for (Message transfer : transfers) {
        receiveMessage(n, transfer);
      }
      return;
    }
    stats.transferRuns++;
    long progress = n.engine.electionProgress();
    Message first = transfers.get(0);
    n.engine.receiveTransfers(transfers);
    if (n.engine.electionProgress() != progress
        || (first.term == n.engine.term() && first.from == n.engine.leaderId())) {
      resetElection(n);
    }
  }

  private static boolean sameEntryRun(Message first, Message next) {
    return first.from == next.from
        && first.to == next.to
        && first.term == next.term
        && Objects.equals(first.reference, next.reference);
  }

  List<byte[]> receive(Node n, List<byte[]> messages, boolean replyAllowed) throws IOException {
    List<Message> entries = new ArrayList<>();
    List<Message> transfers = new ArrayList<>();
    long entryBytes = 0;
    int sender = -1;
    boolean singleSender = true;
    long limit = Math.min(cfg.maxBytes, cfg.maxInflightBytes);
    for (byte[] bytes : messages) {
      Message m = TRaftCodec.decodeMessage(bytes);
      stats.frames++;
      if (traceBuffer != null) {
        trace("  rcv " + describe(m));
      }
      if (sender == -1) {
        sender = m.from;
      } else if (sender != m.from) {
        singleSender = false;
      }
      long size = m.entry == null ? 0 : m.entry.payloadSize() + 256;
      boolean ordinary =
          m.type == MessageType.ENTRY
              && m.entry != null
              && m.entry.kind == Kind.DATA
              && m.entry.hasPayload();
      if (!entries.isEmpty()
          && (!ordinary
              || entries.size() >= cfg.maxEntries
              || size > limit - entryBytes
              || !sameEntryRun(entries.get(0), m))) {
        receiveEntryRun(n, entries);
        entries.clear();
        entryBytes = 0;
      }
      if (m.type != MessageType.TRANSFER_ENTRY && !transfers.isEmpty()) {
        receiveTransferRun(n, transfers);
        transfers.clear();
      }
      if (m.type == MessageType.TRANSFER_ENTRY) {
        stats.transferFrames++;
      }
      if (ordinary && size <= limit) {
        entries.add(m);
        entryBytes += size;
      } else if (m.type == MessageType.TRANSFER_ENTRY) {
        transfers.add(m);
      } else {
        receiveMessage(n, m);
      }
    }
    receiveEntryRun(n, entries);
    receiveTransferRun(n, transfers);
    int replyTo = replyAllowed && singleSender && sender != n.id ? sender : -1;
    return drive(n, replyTo);
  }

  void tick(Node n) throws IOException {
    TRaftEngine engine = n.engine;
    if (engine.role() != Role.LEADER && now >= n.deadline) {
      trace("  n" + n.id + " election timeout, term " + engine.term() + "+1");
      engine.startElection();
      resetElection(n);
    }
    if (now - n.lastHeartbeat >= cfg.heartbeat) {
      engine.tick(now);
      n.lastHeartbeat = now;
    }
    for (Channel channel : n.channels.values()) {
      channel.flushDeferred();
    }
    n.store.forcePending();
    drive(n, -1);
  }

  interface Op {
    void run() throws IOException;
  }

  /** One serialized event on a node's executor. False when the node died during the event. */
  boolean event(Node n, Op op) {
    try {
      op.run();
      return true;
    } catch (SimCrash crash) {
      crashNode(n, crash.survive, crash.down);
      return false;
    } catch (IOException | RuntimeException e) {
      throw new Violation(
          "FENCE:" + e.getClass().getSimpleName() + ":" + normalize(e.getMessage()),
          "n" + n.id + " " + e + " at " + firstFrame(e));
    }
  }

  static String firstFrame(Throwable e) {
    StackTraceElement[] st = e.getStackTrace();
    return st.length == 0 ? "?" : st[0].toString();
  }

  static String normalize(String s) {
    return s == null ? "null" : s.replaceAll("[0-9]+", "#");
  }

  void crashNode(Node n, double survive, double down) {
    if (!n.up) {
      return;
    }
    trace("  CRASH n" + n.id + " survive=" + String.format("%.2f", survive));
    stats.crashes++;
    n.frontierAtCrash = n.engine.compactedThrough();
    n.store.crash(survive);
    n.engine = null;
    n.up = false;
    n.channels.clear();
    n.admissions.clear();
    n.prevRole = Role.FOLLOWER;
    if (cfg.volatileMode) {
      n.appState.clear();
      n.appliedByPartition.clear();
      n.lastAppliedPartition = 0;
    }
    n.restartAt = step + (long) (down * down * 200);
  }

  void restart(Node n) {
    n.restartAt = -1;
    n.incarnation++;
    n.up = true;
    boolean ok =
        event(
            n,
            () -> {
              long minimumTerm = n.store.maxForcedTerm;
              n.engine = new TRaftEngine(n.id, members, cfg.theta, cfg.partitionMs, n.store, n);
              n.engine.setCommitObserver(id -> onCommit(n, id));
              n.engine.setReplicationWindow(cfg.window());
              n.engine.setProposalPayloadBudget(cfg.budget);
              n.engine.setControlPageBytes(cfg.controlPage);
              n.engine.setCompaction(cfg.compaction);
              if (cfg.ignoreCap) {
                n.engine.ignoreCompactionCapForTesting();
              }
              // A lost deferred COMPACT lowers the frontier: check this incarnation afresh.
              n.checkedAnchor = null;
              checkCompaction(n);
              if (n.engine.compactedThrough() >= 0) {
                stats.compactedRestarts++;
              }
              if (n.engine.compactedThrough() < n.frontierAtCrash) {
                stats.compactionRollbacks++;
              }
              if (n.engine.term() < minimumTerm) {
                throw new Violation(
                    "TERM_REGRESSION",
                    "n" + n.id + " restarted at " + n.engine.term() + " below " + minimumTerm);
              }
              n.prevTerm = n.engine.term();
              n.prevRole = n.engine.role();
              n.engine.replayCommitted(n);
              n.engine.applyCommitted();
              stats.restarts++;
              resetElection(n);
              n.lastHeartbeat = now;
              drive(n, -1);
            });
    if (!ok) {
      trace("  n" + n.id + " crashed again during restart");
    }
  }

  // ---------------------------------------------------------------- the schedule

  Run driver;

  final class Run {
    void deliverPicked(double x, double y, boolean uniform) {
      List<Integer> eligible = new ArrayList<>();
      for (int i = 0; i < network.size(); i++) {
        Batch b = network.get(i);
        if (healing || lagUntil[b.to] <= step) {
          eligible.add(i);
        }
      }
      if (eligible.isEmpty()) {
        return;
      }
      int index = uniform ? eligible.get((int) (x * eligible.size())) : eligible.get(0);
      Batch b = network.remove(index);
      if (index != 0) {
        for (int i = 0; i < index; i++) {
          Batch older = network.get(i);
          if (older.from == b.from && older.to == b.to && !older.response && !b.response) {
            stats.overtakes++;
            break;
          }
        }
      }
      stats.deliveries++;
      deliver(b, y);
    }

    void deliver(Batch b, double y) {
      boolean cut = !healing && cutUntil[b.from][b.to] > step;
      double pct = y * 100;
      boolean lost = cut || pct < lossPct;
      boolean dup = !lost && pct < lossPct + dupPct;
      if (lost) {
        stats.lost++;
        trace("  lost batch " + b.from + "->" + b.to + " frames=" + b.frames.size());
        if (b.channel != null) {
          b.channel.done(b.response ? b.request : b);
        }
        return;
      }
      if (dup) {
        stats.dups++;
        network.add(new Batch(b.from, b.to, b.frames, b.dataKeys, b.bytes, null, false, null, -1));
      }
      if (b.response) {
        if (b.channel != null) {
          b.channel.done(b.request);
        }
        Node n = nodes[b.to];
        if (!n.up || n.incarnation != b.toIncarnation) {
          return;
        }
        trace(" response " + b.from + "->" + b.to);
        event(n, () -> receive(n, b.frames, false));
        return;
      }
      Node n = nodes[b.to];
      if (!n.up) {
        if (b.channel != null) {
          b.channel.done(b);
        }
        return;
      }
      trace(" batch " + b.from + "->" + b.to + (b.channel == null ? " ghost" : ""));
      final List<List<byte[]>> holder = new ArrayList<>();
      boolean alive = event(n, () -> holder.add(receive(n, b.frames, b.channel != null)));
      if (b.channel == null) {
        return;
      }
      if (!alive || holder.get(0).isEmpty()) {
        b.channel.done(b);
        return;
      }
      List<String> none = Collections.emptyList();
      network.add(
          new Batch(
              b.to, b.from, holder.get(0), none, 0, b.channel, true, b, nodes[b.from].incarnation));
    }

    void exec(Action a) {
      switch (a.type) {
        case DELIVER:
          for (int i = 0; i < a.a; i++) {
            double x = (a.x + i * 0.6180339887) % 1.0;
            double y = (a.y + i * 0.7548776662) % 1.0;
            if (a.y >= 1.0) {
              y = 0.999999; // setup deliveries are reliable
            }
            deliverPicked(x, y, a.b != 0);
          }
          break;
        case TICKALL:
          for (int i = 1; i <= cfg.nodes; i++) {
            Node n = nodes[i];
            if (n.up) {
              event(n, () -> tick(n));
            }
          }
          break;
        case TICK:
          {
            Node n = nodes[a.a % cfg.nodes + 1];
            if (n.up) {
              event(n, () -> tick(n));
            }
            break;
          }
        case CLIENT:
          client(a);
          break;
        case CRASH:
          crashNode(nodes[a.a % cfg.nodes + 1], a.x, a.y);
          break;
        case CRASHMANY:
          for (int i = 1; i <= cfg.nodes; i++) {
            if ((a.a >> (i - 1) & 1) != 0) {
              crashNode(nodes[i], (a.x + i * 0.37) % 1.0, a.y);
            }
          }
          break;
        case ARM:
          {
            Store s = nodes[a.a % cfg.nodes + 1].store;
            s.armOps = a.b / 10;
            s.armFilter = a.b % 10;
            s.armMode = a.c;
            s.armSurvive = a.x;
            s.armDown = a.y;
            break;
          }
        case CUT:
          {
            int p = a.a % cfg.nodes + 1;
            int q = a.b % cfg.nodes + 1;
            long until = step + (long) (a.x * a.x * 300);
            cutUntil[p][q] = until;
            if (a.c == 1) {
              cutUntil[q][p] = until;
            }
            break;
          }
        case ELECT:
          {
            Node n = nodes[a.a % cfg.nodes + 1];
            campaign(n);
            if (a.b >= 0) {
              Node o = nodes[a.b % cfg.nodes + 1];
              if (o != n) {
                campaign(o);
              }
            }
            break;
          }
        case LAG:
          lagUntil[a.a % cfg.nodes + 1] = step + 20 + (long) (a.x * a.x * 400);
          break;
        case FAILOVER:
          {
            Node leader = currentLeader();
            if (leader != null) {
              crashNode(leader, a.x, a.y);
            }
            Node n = nodes[a.a % cfg.nodes + 1];
            if (a.b == 1 && n != leader) {
              campaign(n);
            }
            break;
          }
        default:
          throw new IllegalStateException();
      }
    }

    void campaign(Node n) {
      if (!n.up || n.engine.role() == Role.LEADER) {
        return;
      }
      event(
          n,
          () -> {
            trace("  n" + n.id + " campaigns");
            n.engine.startElection();
            resetElection(n);
            drive(n, -1);
          });
    }

    void client(Action a) {
      Node leader = currentLeader();
      if (leader == null) {
        return;
      }
      double jitter = a.x;
      for (int i = 0; i < a.b && leader.admissions.size() < 64; i++) {
        Kind kind = Kind.DATA;
        long ts;
        jitter = (jitter + 0.6180339887) % 1.0;
        if (a.a == 2) {
          if (i == 0) {
            kind = Kind.BARRIER;
            ts = 0;
          } else {
            ts = clientClock += 10;
          }
        } else if (a.a == 1 && jitter < 0.5) {
          ts = Math.max(0, clientClock - (long) (jitter * 2000));
        } else {
          ts = clientClock += 10;
        }
        byte[] payload = new byte[12];
        long token = ++tokenCounter;
        for (int k = 0; k < 8; k++) {
          payload[k] = (byte) (token >>> (8 * k));
        }
        payload[8] = (byte) leader.id;
        leader.admissions.addLast(new Command(kind, ts, payload));
      }
      event(leader, () -> drive(leader, -1));
    }
  }

  Node currentLeader() {
    Node best = null;
    for (int i = 1; i <= cfg.nodes; i++) {
      Node n = nodes[i];
      if (n.up
          && n.engine.role() == Role.LEADER
          && (best == null || n.engine.term() > best.engine.term())) {
        best = n;
      }
    }
    return best;
  }

  /** Null when converged; otherwise why not. Throws on a final-state safety violation. */
  String notConverged() {
    long maxTerm = 0;
    for (int i = 1; i <= cfg.nodes; i++) {
      if (!nodes[i].up) {
        return "n" + i + " down";
      }
      maxTerm = Math.max(maxTerm, nodes[i].engine.term());
    }
    Node leader = null;
    for (int i = 1; i <= cfg.nodes; i++) {
      Node n = nodes[i];
      if (n.engine.role() == Role.LEADER && n.engine.term() == maxTerm) {
        leader = n;
      }
    }
    if (leader == null) {
      return "no leader at term " + maxTerm;
    }
    if (leader.engine.hasPendingProposal()) {
      return "leader pending";
    }
    if (!leader.admissions.isEmpty()) {
      return "leader admissions";
    }
    Set<EntryId> installed = new LinkedHashSet<>();
    Set<EntryId> effects = new HashSet<>();
    for (Entry e : logical(leader)) {
      installed.add(e.id);
      if (e.kind != Kind.NOOP) {
        effects.add(e.id);
      }
    }
    if (!installed.containsAll(globalCommitted.keySet())) {
      leaderCompleteness(leader);
      return "leader lacks commit of a later term";
    }
    if (!logicalCommitted(leader).containsAll(installed)) {
      return "leader has uncommitted installed entries";
    }
    for (int i = 1; i <= cfg.nodes; i++) {
      Node n = nodes[i];
      if (n.engine.term() != maxTerm) {
        return "n" + i + " term";
      }
      Set<EntryId> have = new HashSet<>();
      for (Entry e : logical(n)) {
        have.add(e.id);
      }
      if (!have.equals(installed)) {
        return "n" + i + " installed differs";
      }
      if (!logicalCommitted(n).containsAll(installed)) {
        return "n" + i + " commit knowledge";
      }
      if (!n.appState.equals(effects)) {
        Set<EntryId> extra = new HashSet<>(n.appState);
        extra.removeAll(effects);
        if (!extra.isEmpty()) {
          throw new Violation("APPLY_EXTRA", "n" + i + " applied " + extra + " not in leader log");
        }
        return "n" + i + " application";
      }
    }
    return null;
  }

  String dump() {
    StringBuilder b = new StringBuilder();
    for (int i = 1; i <= cfg.nodes; i++) {
      Node n = nodes[i];
      if (!n.up) {
        b.append("n").append(i).append(" down\n");
        continue;
      }
      TRaftEngine e = n.engine;
      b.append("n")
          .append(i)
          .append(' ')
          .append(e.role())
          .append(" term=")
          .append(e.term())
          .append(" leader=")
          .append(e.leaderId())
          .append(" cur=")
          .append(e.currentPartition())
          .append(" installed=")
          .append(e.installedEntries().size())
          .append(" committed=")
          .append(e.committedIds().size())
          .append(" frontier=")
          .append(e.committedFrontier())
          .append(" compacted=")
          .append(e.compactedThrough())
          .append(" app=")
          .append(n.appState.size())
          .append(" pendingWork=")
          .append(e.pendingIncomingWork())
          .append(" pendingProposal=")
          .append(e.hasPendingProposal())
          .append(" admissions=")
          .append(n.admissions.size());
      for (int p = 1; p <= cfg.nodes; p++) {
        if (p != i) {
          b.append(" [->")
              .append(p)
              .append(" flight=")
              .append(e.inFlightEntries(p))
              .append(" fetch=")
              .append(e.outstandingFetches(p))
              .append("]");
        }
      }
      b.append('\n');
    }
    b.append("network=").append(network.size());
    return b.toString();
  }

  static final int LIVENESS_ROUNDS = Integer.getInteger("traft.fuzz.rounds", 600);
  static final boolean LOCKSTEP = Boolean.getBoolean("traft.fuzz.lockstep");
  static final int HEAL_LOAD = Integer.getInteger("traft.fuzz.healLoad", 0);
  static final int ROUND_DELIVERY_CAP = 200000;

  void heal() {
    healing = true;
    lossPct = 0;
    dupPct = 0;
    healRandom = new Random(cfg.seed * 1000003L + 7);
    for (int i = 1; i <= cfg.nodes; i++) {
      nodes[i].store.armOps = -1;
    }
    for (int i = 1; i <= cfg.nodes; i++) {
      if (!nodes[i].up) {
        restart(nodes[i]);
      }
    }
    fullCheck();
    int convergedAt = -1;
    boolean probed = false;
    EntryId probe = null;
    String reason = "?";
    int loadStopped = -1;
    for (int round = 0; round < LIVENESS_ROUNDS + HEAL_LOAD; round++) {
      now += 10;
      step++;
      stats.rounds++;
      if (round < HEAL_LOAD) {
        // The link stays saturated: a client keeps writing and only part of the wire drains, so a
        // follower's window toward the leader can stay full for many heartbeats in a row.
        Node leader = currentLeader();
        if (leader != null) {
          final Node writer = leader;
          for (int i = 0; i < cfg.maxEntries && writer.admissions.size() < 64; i++) {
            byte[] payload = new byte[12];
            long token = ++tokenCounter;
            for (int k = 0; k < 8; k++) {
              payload[k] = (byte) (token >>> (8 * k));
            }
            payload[8] = (byte) 77;
            boolean disorder = healRandom.nextInt(16) == 0;
            writer.admissions.addLast(
                new Command(
                    Kind.DATA,
                    disorder ? Math.max(0, clientClock - 500) : (clientClock += 10),
                    payload));
          }
          event(writer, () -> drive(writer, -1));
        }
        for (int i = 1; i <= cfg.nodes; i++) {
          Node n = nodes[i];
          if (healRandom.nextInt(100) < 50) {
            event(n, () -> tick(n));
          }
        }
        int budget = 1 + healRandom.nextInt(3 * cfg.nodes);
        for (int i = 0; i < budget && !network.isEmpty(); i++) {
          driver.deliverPicked(healRandom.nextDouble(), 0.999999, healRandom.nextInt(100) < 30);
        }
        continue;
      }
      if (loadStopped < 0) {
        loadStopped = round;
      }
      if (LOCKSTEP) {
        for (int i = 1; i <= cfg.nodes; i++) {
          Node n = nodes[i];
          event(n, () -> tick(n));
        }
      } else {
        // Real heartbeat timers are not phase locked to each other or to the network: ticks and
        // deliveries interleave, and any node can skip a round.
        List<Node> order = new ArrayList<>();
        for (int i = 1; i <= cfg.nodes; i++) {
          order.add(nodes[i]);
        }
        Collections.shuffle(order, healRandom);
        for (Node n : order) {
          if (healRandom.nextInt(100) < 80) {
            event(n, () -> tick(n));
          }
          int partial = healRandom.nextInt(6);
          for (int i = 0; i < partial && !network.isEmpty(); i++) {
            driver.deliverPicked(healRandom.nextDouble(), 0.999999, healRandom.nextInt(100) < 50);
          }
        }
      }
      int deliveries = 0;
      while (!network.isEmpty()) {
        if (deliveries++ > ROUND_DELIVERY_CAP) {
          throw new Violation("MESSAGE_STORM", "round " + round + "\n" + dump());
        }
        driver.deliverPicked(healRandom.nextDouble(), 0.999999, healRandom.nextInt(100) < 30);
      }
      reason = notConverged();
      if (round % 40 == 39 && reason != null && reason.startsWith("no leader")) {
        forceBestCandidate();
      }
      if (reason == null) {
        if (!probed) {
          final Node leader = currentLeader();
          byte[] payload = new byte[12];
          long token = ++tokenCounter;
          for (int k = 0; k < 8; k++) {
            payload[k] = (byte) (token >>> (8 * k));
          }
          payload[8] = (byte) 99;
          leader.admissions.addLast(new Command(Kind.DATA, clientClock += 10, payload));
          event(leader, () -> drive(leader, -1));
          probed = true;
          continue;
        }
        if (network.isEmpty()) {
          convergedAt = round;
          break;
        }
      }
    }
    fullCheck();
    if (convergedAt < 0) {
      throw new Violation(
          "STALL:" + normalize(reason), "after " + LIVENESS_ROUNDS + " rounds\n" + dump());
    }
    convergedAt -= Math.max(loadStopped, 0);
    stats.maxRounds = Math.max(stats.maxRounds, convergedAt);
    stats.rounds -= Math.max(loadStopped, 0);
    stats.converged++;
    boolean pending = false;
    for (int i = 1; i <= cfg.nodes; i++) {
      TRaftEngine e = nodes[i].engine;
      if (e.role() == Role.CANDIDATE) {
        stats.finalCandidates++;
      }
      int work = e.pendingIncomingWork();
      stats.finalPendingWork += work;
      pending |= work != 0;
      for (int p = 1; p <= cfg.nodes; p++) {
        stats.finalFetches += p == i ? 0 : e.outstandingFetches(p);
      }
    }
    if (pending) {
      stats.finalPendingSeeds++;
    }
    if (cfg.compaction && !cfg.ignoreCap) {
      boundedAfterBurst();
    }
  }

  static final int BURST_SETTLE_ROUNDS = 8;

  /**
   * After convergence: a burst of ordered writes closes several partitions, then every node's
   * frontier must trail its open partition by at most three, with state bounded by the window.
   */
  void boundedAfterBurst() {
    int writes = 6 * cfg.theta;
    for (int round = 0; round < writes + BURST_SETTLE_ROUNDS; round++) {
      now += 10;
      step++;
      Node leader = currentLeader();
      if (round < writes && leader != null) {
        byte[] payload = new byte[12];
        long token = ++tokenCounter;
        for (int k = 0; k < 8; k++) {
          payload[k] = (byte) (token >>> (8 * k));
        }
        payload[8] = (byte) 55;
        leader.admissions.addLast(new Command(Kind.DATA, clientClock += 10, payload));
        event(leader, () -> drive(leader, -1));
      }
      for (int i = 1; i <= cfg.nodes; i++) {
        Node n = nodes[i];
        event(n, () -> tick(n));
      }
      int deliveries = 0;
      while (!network.isEmpty()) {
        if (deliveries++ > ROUND_DELIVERY_CAP) {
          throw new Violation("MESSAGE_STORM", "burst round " + round + "\n" + dump());
        }
        driver.deliverPicked(healRandom.nextDouble(), 0.999999, false);
      }
    }
    fullCheck();
    for (int i = 1; i <= cfg.nodes; i++) {
      TRaftEngine e = nodes[i].engine;
      long open = e.currentPartition();
      long frontier = e.compactedThrough();
      if (frontier < open - 3) {
        throw new Violation(
            "STATE_BOUNDED", "n" + i + " compacted " + frontier + " of " + open + "\n" + dump());
      }
      Map<String, Integer> sizes = e.stateSizes();
      long entries = (open - frontier + 2) * cfg.theta + 2L * cfg.window();
      long proposals = open - frontier + 4;
      long transport = (long) cfg.nodes * cfg.window();
      String[][] bounds = {
        {"installed", "stored", "knownCommitted", "applied", "entryAcks"},
        {"partitions", "retained", "proposalIndex", "confirmations", "requestedHistory"},
        {"inFlight", "pendingData", "rejectedData", "requestedEntries"}
      };
      long[] limits = {entries, proposals, transport};
      for (int k = 0; k < bounds.length; k++) {
        for (String key : bounds[k]) {
          if (sizes.get(key) > limits[k]) {
            throw new Violation(
                "STATE_BOUNDED", "n" + i + " " + key + "=" + sizes.get(key) + " " + sizes);
          }
        }
      }
    }
    stats.boundedChecks++;
  }

  void forceBestCandidate() {
    Node best = null;
    for (int i = 1; i <= cfg.nodes; i++) {
      Node n = nodes[i];
      if (best == null || n.engine.currentRank().compareTo(best.engine.currentRank()) > 0) {
        best = n;
      }
    }
    final Node chosen = best;
    event(
        chosen,
        () -> {
          trace("  forcing election at best-ranked n" + chosen.id);
          chosen.engine.startElection();
          resetElection(chosen);
          drive(chosen, -1);
        });
  }

  long digest() {
    long h = 1125899906842597L;
    for (int i = 1; i <= cfg.nodes; i++) {
      TreeMap<EntryId, Boolean> sorted = new TreeMap<>();
      if (nodes[i].up) {
        for (Entry e : logical(nodes[i])) {
          sorted.put(e.id, Boolean.TRUE);
        }
      }
      for (EntryId id : sorted.keySet()) {
        h = 31 * h + id.hashCode();
      }
    }
    return h;
  }

  Result run() {
    String category = null;
    String detail = null;
    driver = new Run();
    if (cfg.compaction) {
      stats.compactionSeeds++;
    }
    try {
      for (int i = 1; i <= cfg.nodes; i++) {
        restart(nodes[i]);
      }
      for (Action a : script) {
        step++;
        now++;
        stats.steps++;
        for (int i = 1; i <= cfg.nodes; i++) {
          if (!nodes[i].up && nodes[i].restartAt >= 0 && nodes[i].restartAt <= step) {
            trace("step " + step + " restart n" + i);
            restart(nodes[i]);
          }
        }
        if (traceBuffer != null) {
          trace("step " + step + " now=" + now + " " + a);
        }
        driver.exec(a);
        if (step % 32 == 0) {
          fullCheck();
        }
      }
      trace("=== heal at step " + step + "\n" + dump());
      heal();
      trace("=== converged\n" + dump());
    } catch (Violation v) {
      category = v.category;
      detail = "step " + step + " " + v.detail;
      trace("=== VIOLATION " + v.category + " " + v.detail + "\n" + safeDump());
    }
    return new Result(
        cfg,
        category,
        detail,
        stats,
        category == null ? digest() : 0,
        traceBuffer == null ? null : traceBuffer.toString());
  }

  String safeDump() {
    try {
      return dump();
    } catch (Throwable t) {
      return "dump failed " + t;
    }
  }

  // ---------------------------------------------------------------- campaign, replay and minimize

  static Result runSeed(long seed, boolean trace) {
    return runSeed(seed, trace, ignored -> {});
  }

  /** A seed with its configuration changed first, for example a negative control. */
  static Result runSeed(long seed, boolean trace, Consumer<Config> change) {
    Config c = config(seed);
    change.accept(c);
    return new TRaftFuzz(c, actions(c), trace).run();
  }

  /** ddmin over the action list; keeps the same violation category. */
  static List<Action> minimize(Config c, List<Action> script, String category, long budgetMillis) {
    long deadline = System.currentTimeMillis() + budgetMillis;
    List<Action> current = new ArrayList<>(script);
    int chunk = Math.max(1, current.size() / 2);
    int runs = 0;
    while (chunk >= 1 && System.currentTimeMillis() < deadline) {
      boolean reduced = false;
      for (int start = 0; start < current.size() && System.currentTimeMillis() < deadline; ) {
        int end = Math.min(current.size(), start + chunk);
        List<Action> candidate = new ArrayList<>(current.subList(0, start));
        candidate.addAll(current.subList(end, current.size()));
        Result r = new TRaftFuzz(c, candidate, false).run();
        runs++;
        if (category.equals(r.category)) {
          current = candidate;
          reduced = true;
        } else {
          start += chunk;
        }
      }
      if (!reduced) {
        if (chunk == 1) {
          break;
        }
        chunk = Math.max(1, chunk / 2);
      } else if (chunk > current.size()) {
        chunk = Math.max(1, current.size() / 2);
      }
    }
    System.err.println(
        "minimize: " + script.size() + " -> " + current.size() + " in " + runs + " runs");
    return current;
  }

  public static void main(String[] args) throws Exception {
    String mode = args[0];
    switch (mode) {
      case "campaign":
        campaign(args);
        break;
      case "one":
        one(Long.parseLong(args[1]));
        break;
      case "min":
        min(Long.parseLong(args[1]), args.length > 2 ? Long.parseLong(args[2]) : 60000);
        break;
      default:
        throw new IllegalArgumentException(mode);
    }
  }

  static void one(long seed) {
    Result r = runSeed(seed, true);
    System.out.println(r.trace);
    System.out.println(r.config);
    System.out.println("category=" + r.category + " detail=" + r.detail);
    System.out.println(r.stats);
    System.out.println("digest=" + r.finalDigest);
  }

  static void min(long seed, long budget) {
    Config c = config(seed);
    List<Action> script = actions(c);
    Result r = new TRaftFuzz(c, script, false).run();
    System.out.println(c);
    System.out.println("category=" + r.category + " detail=" + r.detail);
    if (r.category == null) {
      return;
    }
    List<Action> reduced = minimize(c, script, r.category, budget);
    System.out.println("minimal schedule (" + reduced.size() + " actions): " + reduced);
    Result again = new TRaftFuzz(c, reduced, true).run();
    System.out.println(again.trace);
    System.out.println("category=" + again.category + " detail=" + again.detail);
  }

  static void campaign(String[] args) throws Exception {
    long from = Long.parseLong(args[1]);
    long count = Long.parseLong(args[2]);
    int threads = Integer.parseInt(args[3]);
    String label = args.length > 4 ? args[4] : "tree";
    AtomicLong next = new AtomicLong(from);
    long end = from + count;
    Map<String, List<Long>> byCategory = new TreeMap<>();
    Map<String, String> sampleDetail = new HashMap<>();
    Stats total = new Stats();
    AtomicLong done = new AtomicLong();
    long started = System.currentTimeMillis();
    long[] digests = new long[(int) count];
    Thread[] workers = new Thread[threads];
    Object lock = new Object();
    for (int t = 0; t < threads; t++) {
      workers[t] =
          new Thread(
              () -> {
                while (true) {
                  long seed = next.getAndIncrement();
                  if (seed >= end) {
                    return;
                  }
                  Result r;
                  try {
                    r = runSeed(seed, false);
                  } catch (Throwable other) {
                    r =
                        new Result(
                            config(seed),
                            "HARNESS:" + other.getClass().getSimpleName(),
                            other.toString() + " at " + firstFrame(other),
                            new Stats(),
                            0,
                            null);
                  }
                  synchronized (lock) {
                    total.add(r.stats);
                    digests[(int) (seed - from)] = r.finalDigest;
                    if (r.category != null) {
                      byCategory.computeIfAbsent(r.category, k -> new ArrayList<>()).add(seed);
                      sampleDetail.putIfAbsent(r.category, r.detail);
                    }
                  }
                  long d = done.incrementAndGet();
                  if (d % 2000 == 0) {
                    System.err.println(
                        label
                            + " "
                            + d
                            + "/"
                            + count
                            + " elapsed "
                            + (System.currentTimeMillis() - started) / 1000
                            + "s");
                  }
                }
              });
      workers[t].start();
    }
    for (Thread worker : workers) {
      worker.join();
    }
    long elapsed = System.currentTimeMillis() - started;
    long h = 17;
    for (long d : digests) {
      h = 31 * h + d;
    }
    PrintStream out = System.out;
    out.println(
        "tree="
            + label
            + " seeds="
            + count
            + " from="
            + from
            + " threads="
            + threads
            + " elapsedMs="
            + elapsed
            + " runDigest="
            + h);
    long bad = 0;
    for (List<Long> v : byCategory.values()) {
      bad += v.size();
    }
    out.println("violatingSeeds=" + bad);
    for (Map.Entry<String, List<Long>> e : byCategory.entrySet()) {
      List<Long> seeds = e.getValue();
      Collections.sort(seeds);
      out.println(
          e.getKey()
              + " count="
              + seeds.size()
              + " seeds="
              + seeds.subList(0, Math.min(25, seeds.size())));
      out.println("   sample: " + firstLine(sampleDetail.get(e.getKey())));
    }
    out.println("stats " + total);
    if (Boolean.getBoolean("traft.fuzz.seedList")) {
      for (Map.Entry<String, List<Long>> e : byCategory.entrySet()) {
        out.println("SEEDS " + e.getKey() + " " + e.getValue());
      }
    }
  }

  static String firstLine(String s) {
    if (s == null) {
      return "";
    }
    int i = s.indexOf('\n');
    return i < 0 ? s : s.substring(0, i);
  }
}
