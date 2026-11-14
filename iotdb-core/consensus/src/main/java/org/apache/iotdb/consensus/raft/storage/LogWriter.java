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

package org.apache.iotdb.consensus.raft.storage;

import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Pipelined group commit, with a writer and a forcer thread per group. The writer drains Persist /
 * PersistMeta actions in order and writes log records at once (a run of Persist actions with one
 * {@link LogStore#writeAll} call), without waiting for a force in progress. The forcer forces the
 * log once the written entries are due: {@code maxBatch} entries are unforced, or the oldest has
 * waited {@code maxDelayMs} (0: as soon as the previous force ends), or a metadata write waits. It
 * then hands the Durable / MetaDurable events back in action order. A metadata write waits until
 * every log record written before it is forced and is then written atomically and synced on its
 * own, so a durable metadata write still implies that every earlier log write is durable (a power
 * loss can never keep e.g. a raised term or sync number and lose an earlier truncation or entry),
 * and no log record after it is written before it is durable. The kernel only registers acks,
 * replies or votes after these events, so "respond after fsync" is guaranteed by construction.
 */
public final class LogWriter implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogWriter.class);

  /** At most this many queued actions are written per writer round. */
  private static final int MAX_DRAIN = 4096;

  private static final long NONE = Long.MIN_VALUE;

  private final LogStore log;
  private final MetaStore meta;
  private final Consumer<Event> sink;
  private final int maxBatch;
  private final long maxDelayNanos;
  private final LinkedBlockingQueue<Action> queue = new LinkedBlockingQueue<>();
  private final Thread writer;
  private final Thread forcer;
  private volatile boolean running = true;
  private volatile Throwable failure;

  // Written actions not yet handed back, in action order, and the force bookkeeping: guarded by
  // this. Sequence numbers count written actions.
  private final ArrayDeque<Written> written = new ArrayDeque<>();
  private long writtenSeq;
  private long lastLogSeq; // the last written Persist action
  private long forcedSeq; // every Persist action at or below it is covered by a finished force
  private long unforcedEntries; // entries written since the last force started
  private long unforcedSince = NONE; // when the first of them was written
  private boolean urgent; // a metadata write waits for the log to be forced
  private boolean writerDone;

  private static final class Written {
    final long seq;
    final Action action;

    Written(long seq, Action action) {
      this.seq = seq;
      this.action = action;
    }
  }

  /**
   * @param maxBatch a force is due once this many written entries are unforced
   * @param maxDelayMs ... or once the oldest of them has waited this long (0: at once)
   */
  public LogWriter(
      String name,
      LogStore log,
      MetaStore meta,
      Consumer<Event> sink,
      int maxBatch,
      long maxDelayMs) {
    this.log = log;
    this.meta = meta;
    this.sink = sink;
    this.maxBatch = Math.max(1, maxBatch);
    this.maxDelayNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, maxDelayMs));
    this.writer = new Thread(this::writeLoop, name);
    this.writer.setDaemon(true);
    this.forcer = new Thread(this::forceLoop, name + "-force");
    this.forcer.setDaemon(true);
  }

  public void start() {
    writer.start();
    forcer.start();
  }

  public void enqueue(Action a) {
    if (!(a instanceof Action.Persist) && !(a instanceof Action.PersistMeta)) {
      throw new IllegalArgumentException("not a persistence action: " + a);
    }
    if (failure != null) {
      throw new IllegalStateException("log writer failed", failure);
    }
    queue.add(a);
  }

  public Throwable failure() {
    return failure;
  }

  // ------------------------------------------------------------------- writer

  private void writeLoop() {
    List<Action> batch = new ArrayList<>();
    try {
      while ((running || !queue.isEmpty()) && failure == null) {
        batch.clear();
        Action first = queue.poll(100, TimeUnit.MILLISECONDS);
        if (first == null) {
          continue;
        }
        batch.add(first);
        queue.drainTo(batch, MAX_DRAIN - 1);
        measure(batch);
        write(batch);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      fail(t);
    } finally {
      synchronized (this) {
        writerDone = true;
        notifyAll();
      }
    }
  }

  private static void measure(List<Action> batch) {
    ResearchMetrics.add("raft.log_batch_count", 1);
    ResearchMetrics.add("raft.log_batch_action_count", batch.size());
    long entries = 0, payloadBytes = 0, truncations = 0, metadata = 0;
    for (Action action : batch) {
      if (action instanceof Action.Persist) {
        Action.Persist persist = (Action.Persist) action;
        entries += persist.entries.size();
        truncations += persist.truncateFrom >= 0 ? 1 : 0;
        for (LogEntry entry : persist.entries) {
          payloadBytes += entry.size();
        }
      } else {
        metadata++;
      }
    }
    ResearchMetrics.add("raft.log_batch_entry_count", entries);
    ResearchMetrics.add("raft.log_batch_payload_bytes", payloadBytes);
    ResearchMetrics.add("raft.log_batch_truncate_count", truncations);
    ResearchMetrics.add("raft.log_batch_metadata_count", metadata);
  }

  /** Writes a batch in action order and publishes each action to the forcer once written. */
  private void write(List<Action> batch) throws IOException, InterruptedException {
    List<Action> run = new ArrayList<>();
    List<LogEntry> entries = new ArrayList<>();
    for (Action a : batch) {
      if (a instanceof Action.Persist) {
        Action.Persist p = (Action.Persist) a;
        if (p.truncateFrom >= 0) {
          writeAll(entries);
          log.deleteFrom(p.truncateFrom);
        }
        entries.addAll(p.entries);
        run.add(p);
      } else {
        writeAll(entries);
        publish(run);
        awaitLogForced(); // the log records before this metadata write are durable first
        Action.PersistMeta m = (Action.PersistMeta) a;
        meta.persist(m.term, m.votedFor, m.profileMeta); // atomic file + fsync
        publish(new ArrayList<>(Collections.singletonList(a)));
      }
    }
    writeAll(entries);
    publish(run);
  }

  private void writeAll(List<LogEntry> entries) throws IOException {
    if (!entries.isEmpty()) {
      log.writeAll(entries);
      entries.clear();
    }
  }

  /** Hands written actions to the forcer, in order, and clears the list. */
  private synchronized void publish(List<Action> actions) {
    if (actions.isEmpty()) {
      return;
    }
    for (Action a : actions) {
      written.addLast(new Written(++writtenSeq, a));
      if (a instanceof Action.Persist) {
        lastLogSeq = writtenSeq;
        if (unforcedSince == NONE) {
          unforcedSince = System.nanoTime();
        }
        unforcedEntries += Math.max(1, ((Action.Persist) a).entries.size());
      }
    }
    actions.clear();
    notifyAll();
  }

  /** Blocks until a finished force covers every log record written so far. */
  private synchronized void awaitLogForced() throws InterruptedException, IOException {
    if (forcedSeq >= lastLogSeq) {
      return;
    }
    urgent = true;
    notifyAll();
    while (forcedSeq < lastLogSeq) {
      if (failure != null) {
        throw new IOException("log writer failed", failure);
      }
      wait(100);
    }
  }

  // ------------------------------------------------------------------- forcer

  private void forceLoop() {
    List<Written> release = new ArrayList<>();
    try {
      while (true) {
        long target;
        boolean sync;
        synchronized (this) {
          while (failure == null && !due()) {
            if (writerDone && written.isEmpty()) {
              return;
            }
            wait(waitMillis());
          }
          if (failure != null) {
            return; // the group stops: nothing more is handed back
          }
          target = writtenSeq;
          sync = lastLogSeq > forcedSeq;
          if (sync) {
            ResearchMetrics.add("raft.log_force_due_requested", urgent || !running ? 1 : 0);
            ResearchMetrics.add(
                "raft.log_force_due_entries",
                !urgent && running && unforcedEntries >= maxBatch ? 1 : 0);
            ResearchMetrics.add(
                "raft.log_force_due_interval",
                !urgent && running && unforcedEntries < maxBatch ? 1 : 0);
            ResearchMetrics.add("raft.log_force_entries", unforcedEntries);
          }
          urgent = false;
          unforcedEntries = 0;
          unforcedSince = NONE;
        }
        if (sync) {
          log.sync(); // covers every record written before target was published
        }
        synchronized (this) {
          forcedSeq = Math.max(forcedSeq, target);
          while (!written.isEmpty() && written.peekFirst().seq <= target) {
            release.add(written.pollFirst());
          }
          notifyAll();
        }
        for (Written w : release) {
          if (w.action instanceof Action.Persist) {
            sink.accept(new Event.Durable(((Action.Persist) w.action).token));
          } else {
            sink.accept(new Event.MetaDurable(((Action.PersistMeta) w.action).token));
          }
        }
        release.clear();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      fail(t);
    }
  }

  /** Holds this. */
  private boolean due() {
    if (written.isEmpty()) {
      return false;
    }
    if (urgent || !running || lastLogSeq <= forcedSeq) {
      return true; // a metadata write waits, closing, or only metadata actions to hand back
    }
    return unforcedEntries >= maxBatch || System.nanoTime() - unforcedSince >= maxDelayNanos;
  }

  /** Holds this: how long the forcer may sleep before the oldest unforced entry is due. */
  private long waitMillis() {
    if (written.isEmpty() || unforcedSince == NONE) {
      return 100;
    }
    long left = maxDelayNanos - (System.nanoTime() - unforcedSince);
    return Math.max(1, (left + 999_999) / 1_000_000);
  }

  private void fail(Throwable t) {
    LOGGER.error("log writer {} failed; the group must stop", writer.getName(), t);
    synchronized (this) {
      if (failure == null) {
        failure = t;
      }
      running = false;
      notifyAll();
    }
  }

  /**
   * The I/O of one batch, in order: log records (truncations, entries) are written and synced once
   * for the whole batch (group commit), and each metadata write is atomic and synced on its own.
   * Durability follows the order of the actions: a metadata write that follows log records of the
   * same batch syncs the log first, so a durable metadata write implies that every log write before
   * it is durable too (a power loss can never keep the metadata, e.g. a raised term or sync number,
   * and lose an earlier truncation or entry). Run by the writer thread; public so that the
   * simulation's asynchronous writer executes exactly the same steps.
   */
  public static void writeBatch(List<Action> batch, LogStore log, MetaStore meta)
      throws IOException {
    boolean logDirty = false;
    for (Action a : batch) {
      if (a instanceof Action.Persist) {
        Action.Persist p = (Action.Persist) a;
        if (p.truncateFrom >= 0) {
          log.deleteFrom(p.truncateFrom);
        }
        for (LogEntry e : p.entries) {
          log.write(e);
        }
        logDirty = true;
      } else {
        if (logDirty) {
          log.sync(); // the log records before this metadata write are durable first
          logDirty = false;
        }
        Action.PersistMeta m = (Action.PersistMeta) a;
        meta.persist(m.term, m.votedFor, m.profileMeta); // atomic file + fsync
      }
    }
    if (logDirty) {
      log.sync();
    }
  }

  /** Stop after writing the queued actions, forcing them and handing their events back. */
  @Override
  public void close() {
    synchronized (this) {
      running = false;
      notifyAll();
    }
    try {
      writer.join(TimeUnit.SECONDS.toMillis(30));
      forcer.join(TimeUnit.SECONDS.toMillis(30));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
