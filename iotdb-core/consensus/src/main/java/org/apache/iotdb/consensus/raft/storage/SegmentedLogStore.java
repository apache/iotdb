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

import org.apache.iotdb.consensus.common.research.ResearchIo;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Arrival-order segmented log: records are appended to the current segment in the order they are
 * written (TRaft hole filling appends small indexes late); TRUNCATE records make suffix repair
 * O(1); recovery replays the segments in order ("replay is the state") and cuts an unsynced /
 * incomplete tail off the last segment. Complete corrupt records fail recovery. Writing an index
 * again (ParallelRaft dated overwrite) appends a new record; the later record wins in the cache,
 * the index map and replay.
 *
 * <p>{@link #sync} forces outside the store's lock, so readers (the applier, the sender) and the
 * log writer do not wait for a force in progress; cache hits take no store lock at all. A segment
 * that rolls over is forced first (recovery cuts an incomplete tail off the last segment only) and
 * its channel is closed by the next sync, never under a force that may still use it. Lock order:
 * {@code forceLock}, then the store.
 */
public final class SegmentedLogStore implements LogStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentedLogStore.class);
  private static final String SEG_PREFIX = "seg_";
  private static final String SEG_SUFFIX = ".log";

  private static final class Segment {
    final long seq;
    final File file;
    FileChannel channel; // write channel of the active segment, read channel otherwise
    long size;
    boolean dirty;

    /** Holds a TRUNCATE record (unknown = true until replayed). */
    boolean truncates;

    Segment(long seq, File file) {
      this.seq = seq;
      this.file = file;
    }
  }

  private static final class Loc {
    final long seq;
    final long offset;
    final int length;

    Loc(long seq, long offset, int length) {
      this.seq = seq;
      this.offset = offset;
      this.length = length;
    }
  }

  private final File dir;
  private final EntryMetaCodec codec;
  private final long segmentBytes;
  private final EntryCache cache;
  private final LogMeta meta;
  private final TreeMap<Long, Segment> segments = new TreeMap<>();
  private final TreeMap<Long, Loc> locs = new TreeMap<>();
  private Segment active;
  private long loads;

  /** Serializes forces and the closing of channels they may use; taken before the store lock. */
  private final Object forceLock = new Object();

  /** Write channels of rolled-over segments, closed by the next sync (guarded by the store). */
  private final List<FileChannel> retired = new ArrayList<>();

  /** Tests: runs before each force of {@link #sync}, outside the store lock. */
  private volatile Runnable beforeForce = () -> {};

  public SegmentedLogStore(File logDir, EntryMetaCodec codec, long segmentBytes, long cacheBytes)
      throws IOException {
    this.dir = logDir;
    this.codec = codec;
    this.segmentBytes = segmentBytes;
    this.cache = new EntryCache(cacheBytes);
    AtomicFiles.createDirectories(dir);
    this.meta = new LogMeta(dir).load();
    File[] files =
        dir.listFiles((d, name) -> name.startsWith(SEG_PREFIX) && name.endsWith(SEG_SUFFIX));
    if (files != null) {
      for (File f : files) {
        long seq =
            Long.parseLong(
                f.getName()
                    .substring(SEG_PREFIX.length(), f.getName().length() - SEG_SUFFIX.length()));
        Segment s = new Segment(seq, f);
        s.size = f.length();
        s.truncates = true; // conservative until recover() has replayed it
        segments.put(seq, s);
      }
    }
  }

  // ------------------------------------------------------------------ recover

  @Override
  public synchronized LogIndex recover() throws IOException {
    locs.clear();
    Map<Long, LogRecordCodec.Record> heads = new TreeMap<>();
    List<Long> seqs = new ArrayList<>(segments.keySet());
    for (int i = 0; i < seqs.size(); i++) {
      Segment s = segments.get(seqs.get(i));
      boolean last = i == seqs.size() - 1;
      ByteBuffer buf =
          ByteBuffer.wrap(ResearchIo.readAllBytes(s.file.toPath(), ResearchIo.RAFT_LOG));
      long good = 0;
      s.truncates = false;
      while (buf.hasRemaining()) {
        int start = buf.position();
        LogRecordCodec.Record r = LogRecordCodec.decode(buf, codec);
        if (r == null) {
          boolean incomplete =
              buf.remaining() < LogRecordCodec.MIN_RECORD
                  || (buf.getInt(start) >= LogRecordCodec.MIN_RECORD - 8
                      && (long) buf.getInt(start) + 8 > buf.remaining());
          if (!last || !incomplete) {
            throw new IOException("Corrupt Raft record in " + s.file + " at " + start);
          }
          break;
        }
        good = buf.position();
        if (r.isTruncate()) {
          s.truncates = true;
          locs.tailMap(r.index, true).clear();
          heads.keySet().removeIf(k -> k >= r.index);
        } else if (r.index >= meta.firstIndex()) {
          locs.put(r.index, new Loc(s.seq, start, r.length));
          heads.put(r.index, r);
        }
      }
      if (good < s.size) {
        if (last) {
          LOGGER.warn("{}: truncating incomplete tail at {} (size {})", s.file, good, s.size);
          try (FileChannel ch = FileChannel.open(s.file.toPath(), StandardOpenOption.WRITE)) {
            ch.truncate(good);
            ResearchIo.force(ch, true, ResearchIo.RAFT_LOG);
          }
          s.size = good;
        }
      }
    }
    LogIndex index = new LogIndex();
    if (meta.firstIndex() > 0) {
      index.purgeBefore(meta.firstIndex(), meta.snapshotTerm());
    }
    for (Map.Entry<Long, LogRecordCodec.Record> en : heads.entrySet()) {
      LogRecordCodec.Record r = en.getValue();
      index.put(
          r.index,
          r.term,
          org.apache.iotdb.consensus.raft.core.log.EntryType.fromCode(r.type),
          r.meta,
          r.payload.remaining(),
          true);
    }
    if (!segments.isEmpty()) {
      active = segments.lastEntry().getValue();
      active.channel =
          FileChannel.open(
              active.file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }
    return index;
  }

  // -------------------------------------------------------------------- write

  private Segment ensureActive() throws IOException {
    if (active == null || active.size >= segmentBytes) {
      if (active != null && active.channel != null) {
        if (active.dirty) {
          ResearchIo.force(active.channel, false, ResearchIo.RAFT_LOG);
          active.dirty = false;
        }
        retired.add(active.channel); // a sync running now may still force it
        active.channel = null;
      }
      long seq = segments.isEmpty() ? 1 : segments.lastKey() + 1;
      Segment s =
          new Segment(seq, new File(dir, String.format("%s%020d%s", SEG_PREFIX, seq, SEG_SUFFIX)));
      s.channel =
          FileChannel.open(
              s.file.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      AtomicFiles.forceDirectory(dir);
      segments.put(seq, s);
      active = s;
    }
    return active;
  }

  private void append(ByteBuffer record, long index, boolean isEntry) throws IOException {
    Segment s = ensureActive();
    long offset = s.size;
    int len = record.remaining();
    while (record.hasRemaining()) {
      ResearchIo.write(s.channel, record, ResearchIo.RAFT_LOG);
    }
    s.size += len;
    s.dirty = true;
    if (isEntry) {
      locs.put(index, new Loc(s.seq, offset, len));
    } else {
      s.truncates = true;
    }
  }

  @Override
  public synchronized void write(LogEntry e) throws IOException {
    append(LogRecordCodec.encodeEntry(e, codec), e.index, true);
    cache.put(e);
  }

  /** One write call for the whole list; the records are those {@link #write} would append. */
  @Override
  public synchronized void writeAll(List<LogEntry> entries) throws IOException {
    if (entries.size() == 1) {
      write(entries.get(0));
      return;
    }
    if (entries.isEmpty()) {
      return;
    }
    ByteBuffer[] records = new ByteBuffer[entries.size()];
    int total = 0;
    for (int i = 0; i < records.length; i++) {
      records[i] = LogRecordCodec.encodeEntry(entries.get(i), codec);
      total += records[i].remaining();
    }
    ByteBuffer all = ByteBuffer.allocate(total);
    for (ByteBuffer record : records) {
      all.put(record.duplicate());
    }
    all.flip();
    Segment s = ensureActive();
    long offset = s.size;
    while (all.hasRemaining()) {
      ResearchIo.write(s.channel, all, ResearchIo.RAFT_LOG);
    }
    s.size += total;
    s.dirty = true;
    for (int i = 0; i < records.length; i++) {
      LogEntry e = entries.get(i);
      int length = records[i].remaining();
      locs.put(e.index, new Loc(s.seq, offset, length));
      offset += length;
      cache.put(e);
    }
  }

  @Override
  public synchronized void deleteFrom(long from) throws IOException {
    append(LogRecordCodec.encodeTruncate(from), from, false);
    locs.tailMap(from, true).clear();
    cache.removeFrom(from);
  }

  @Override
  public void sync() throws IOException {
    synchronized (forceLock) {
      List<Segment> dirty = new ArrayList<>();
      List<FileChannel> channels = new ArrayList<>();
      List<FileChannel> rolled;
      synchronized (this) {
        for (Segment s : segments.values()) {
          if (s.dirty && s.channel != null) {
            dirty.add(s);
            channels.add(s.channel); // a rollover may retire it meanwhile; it stays open
            s.dirty = false; // written again from here on: dirty again, forced by the next sync
          }
        }
        rolled = new ArrayList<>(retired);
        retired.clear();
      }
      try {
        for (FileChannel ch : channels) {
          beforeForce.run();
          ResearchIo.force(ch, false, ResearchIo.RAFT_LOG);
        }
      } catch (IOException | RuntimeException e) {
        synchronized (this) {
          for (Segment s : dirty) {
            s.dirty = s.channel != null; // a retired segment was forced when it rolled over
          }
          retired.addAll(rolled);
        }
        throw e;
      }
      for (FileChannel ch : rolled) {
        ch.close(); // forced when it rolled over; no force can be using it any more
      }
    }
  }

  /** Tests only: a hook that runs before each force of {@link #sync}, outside the store lock. */
  void setBeforeForce(Runnable hook) {
    beforeForce = hook;
  }

  // --------------------------------------------------------------------- read

  @Override
  public LogEntry read(long index) throws IOException {
    ResearchMetrics.add("raft.payload_read_requests", 1);
    ResearchMetrics.add("raft.payload_cache_hits", 0);
    ResearchMetrics.add("raft.payload_read_bytes_returned", 0);
    LogEntry cached = cache.get(index); // the cache is thread-safe: no store lock for a hit
    if (cached != null) {
      ResearchMetrics.add("raft.payload_cache_hits", 1);
      ResearchMetrics.add("raft.payload_read_bytes_returned", cached.size());
      return cached;
    }
    return load(index);
  }

  private synchronized LogEntry load(long index) throws IOException {
    Loc loc = locs.get(index);
    if (loc == null) {
      return null;
    }
    Segment s = segments.get(loc.seq);
    ByteBuffer buf = ByteBuffer.allocate(loc.length);
    try (FileChannel ch = FileChannel.open(s.file.toPath(), StandardOpenOption.READ)) {
      long pos = loc.offset;
      while (buf.hasRemaining()) {
        int n = ResearchIo.read(ch, buf, pos, ResearchIo.RAFT_LOG);
        if (n < 0) {
          throw new IOException("short read of e" + index + " in " + s.file);
        }
        pos += n;
      }
    }
    buf.flip();
    LogRecordCodec.Record r = LogRecordCodec.decode(buf, codec);
    if (r == null || r.index != index) {
      throw new IOException("corrupt record for e" + index + " in " + s.file);
    }
    loads++;
    LogEntry e = r.toEntry();
    cache.put(e);
    ResearchMetrics.add("raft.payload_read_bytes_returned", e.size());
    return e;
  }

  @Override
  public void purgeBefore(long index) throws IOException {
    synchronized (forceLock) {
      purgeBeforeLocked(index);
    }
  }

  private synchronized void purgeBeforeLocked(long index) throws IOException {
    for (Loc l : locs.headMap(index, false).values()) {
      // keep the map lean: indexes below the new first index are gone
    }
    locs.headMap(index, false).clear();
    cache.removeBefore(index);
    List<Long> removable = new ArrayList<>();
    boolean olderKept = false;
    for (Segment s : segments.values()) { // ascending seq: replay order
      if (s == active) {
        continue;
      }
      boolean referenced = false;
      for (Loc l : locs.values()) {
        if (l.seq == s.seq) {
          referenced = true;
          break;
        }
      }
      // A TRUNCATE record cancels stale records of older segments on replay, so a segment that
      // holds one must stay while any older segment stays (an overwrite can leave such a segment
      // without live entries while an older one still holds both live and truncated records).
      if (referenced || (s.truncates && olderKept)) {
        olderKept = true;
      } else {
        removable.add(s.seq);
      }
    }
    for (long seq : removable) {
      Segment s = segments.remove(seq);
      if (s.channel != null) {
        s.channel.close();
      }
      Files.deleteIfExists(s.file.toPath());
    }
  }

  @Override
  public LogMeta meta() {
    return meta;
  }

  @Override
  public synchronized long loadCount() {
    return loads;
  }

  public EntryCache cache() {
    return cache;
  }

  public synchronized int segmentCount() {
    return segments.size();
  }

  @Override
  public void close() throws IOException {
    synchronized (forceLock) {
      sync();
      synchronized (this) {
        for (Segment s : segments.values()) {
          if (s.channel != null) {
            s.channel.close();
            s.channel = null;
          }
        }
        active = null;
      }
    }
  }
}
