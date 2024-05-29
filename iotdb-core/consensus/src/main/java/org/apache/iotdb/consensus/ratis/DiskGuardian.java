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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.ratis.utils.Utils;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link DiskGuardian} manages to take snapshots periodically for each Raft Group in order to
 * control disk storage consumption.
 */
class DiskGuardian {
  private static final Logger logger = LoggerFactory.getLogger(DiskGuardian.class);

  /**
   * A summary of Raft Log disk file snapshot. Containing following summary statistics:
   *
   * <p>
   * <li>1. Disk space consumed by this group's Raft Logs.
   * <li>2. How many raft log segments exists.
   */
  static final class RaftLogSummary {

    /** Whether the path denotes an open segment under active writing progress */
    private static final Predicate<Path> isOpenSegment =
        p -> p.toFile().getName().startsWith("log_inprogress");

    private final RaftGroupId gid;
    private final File raftLogStorageRoot;
    private long totalSize;
    private Set<Path> logFiles;

    private RaftLogSummary(RaftGroupId gid, File raftLogStorageRoot) {
      this.gid = gid;
      this.raftLogStorageRoot = raftLogStorageRoot;
      this.totalSize = 0;
      this.logFiles = Collections.emptySet();
    }

    public long getTotalSize() {
      return totalSize;
    }

    void updateNow() {
      try (final Stream<Path> files = Files.list(raftLogStorageRoot.toPath())) {
        final Set<Path> latest = files.filter(isOpenSegment).collect(Collectors.toSet());
        this.totalSize += diff(logFiles, latest);
        this.logFiles = latest;
      } catch (IOException e) {
        // keep the files unchanged
        logger.warn("{}: Error caught when listing files for {} at {}:", this, gid, e);
      }
    }

    private static long diff(Set<Path> old, Set<Path> latest) {
      final long incremental = totalSize(latest.stream().filter(p -> !old.contains(p)));
      final long decremental = totalSize(old.stream().filter(p -> !latest.contains(p)));
      return incremental - decremental;
    }

    private static long totalSize(Stream<Path> files) {
      return files.mapToLong(p -> p.toFile().length()).sum();
    }

    @Override
    public String toString() {
      return String.format(
          "[Raft Log Summary]: group=%s, total size=%d, files=%s", gid, totalSize, logFiles);
    }
  }

  /** Use a {@link MemoizedSupplier} here to avoid this reference leak */
  private final MemoizedSupplier<RatisConsensus> serverRef;

  /** Whether a specific RaftGroup need to take snapshot during next check period */
  private final Map<RaftGroupId, AtomicBoolean> snapshotFlag = new ConcurrentHashMap<>();

  /** Raft Log disk usage summary statistics */
  private final Map<RaftGroupId, RaftLogSummary> bookkeeper = new ConcurrentHashMap<>();

  /** Registered checkers */
  private final Map<TimeDuration, List<Predicate<RaftLogSummary>>> snapshotArbitrators =
      new HashMap<>();

  private final ScheduledExecutorService workerThread =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.RATIS_BG_DISK_GUARDIAN.getName());
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private final long daemonIntervalMs;

  DiskGuardian(Supplier<RatisConsensus> server, RatisConfig config) {
    this.serverRef = MemoizedSupplier.valueOf(server);
    this.daemonIntervalMs = config.getImpl().getCheckAndTakeSnapshotInterval();
  }

  void start() {
    // first schedule the snapshot daemon
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        workerThread, this::snapshotDaemon, daemonIntervalMs, daemonIntervalMs, TimeUnit.SECONDS);

    // then schedule all checker daemons
    snapshotArbitrators.forEach(
        (interval, checkers) ->
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                workerThread,
                () -> checkerDaemon(checkers),
                daemonIntervalMs,
                interval.toLong(TimeUnit.SECONDS),
                TimeUnit.SECONDS));
  }

  void stop() throws InterruptedException {
    if (isStopped.compareAndSet(true, false)) {
      workerThread.shutdown();
      workerThread.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /** Call this method to register checkers before {@link #start()} */
  void registerChecker(Predicate<RaftLogSummary> checker, TimeDuration interval) {
    final List<Predicate<RaftLogSummary>> checkers =
        snapshotArbitrators.computeIfAbsent(interval, i -> new CopyOnWriteArrayList<>());
    checkers.add(checker);
  }

  /** Periodically woken up. Will take a snapshot if a flag is set. */
  private void snapshotDaemon() {
    if (isStopped.get()) {
      return;
    }
    try {
      for (RaftGroupId groupId : serverRef.get().getServer().getGroupIds()) {
        if (getSnapshotFlag(groupId).get()) {
          try {
            serverRef
                .get()
                .triggerSnapshot(Utils.fromRaftGroupIdToConsensusGroupId(groupId), false);
            final boolean flagCleared = snapshotFlag.get(groupId).compareAndSet(true, false);
            if (!flagCleared) {
              logger.info(
                  "{}: clear snapshot flag failed for group {}, please check the related implementation",
                  this,
                  groupId);
            }
          } catch (ConsensusException e) {
            logger.info(
                "{} take snapshot failed for group {} due to {}. Disk file status {}",
                this,
                groupId,
                e,
                getLatestSummary(groupId).orElse(null));
          }
        }
      }
    } catch (IOException ignore) {
    }
  }

  /** Periodically woken up. Will check the snapshot condition and set the snapshot flag. */
  private void checkerDaemon(List<Predicate<RaftLogSummary>> checkerList) {
    if (isStopped.get()) {
      return;
    }
    try {
      for (RaftGroupId groupId : serverRef.get().getServer().getGroupIds()) {
        final Optional<RaftLogSummary> summary = getLatestSummary(groupId);
        if (summary.isPresent()) {
          final Optional<Boolean> anyCheckerPositive =
              checkerList.stream()
                  .map(checker -> checker.test(summary.get()))
                  .filter(Boolean::booleanValue)
                  .findAny();
          if (anyCheckerPositive.isPresent()) {
            getSnapshotFlag(groupId).set(true);
          }
        }
      }
    } catch (IOException ignore) {
    }
  }

  private AtomicBoolean getSnapshotFlag(RaftGroupId groupId) {
    return snapshotFlag.computeIfAbsent(groupId, id -> new AtomicBoolean(false));
  }

  private Optional<RaftLogSummary> getLatestSummary(RaftGroupId groupId) {
    // initialize the RaftLog Summary for the first time
    final RaftLogSummary summary =
        bookkeeper.computeIfAbsent(
            groupId,
            gid -> {
              final File root;
              try {
                root =
                    serverRef
                        .get()
                        .getServer()
                        .getDivision(groupId)
                        .getRaftStorage()
                        .getStorageDir()
                        .getCurrentDir();
                return new RaftLogSummary(gid, root);
              } catch (IOException e) {
                return null;
              }
            });

    if (summary != null) {
      summary.updateNow();
    }
    return Optional.ofNullable(summary);
  }
}
