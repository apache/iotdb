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

package org.apache.iotdb.consensus.traft.paper.runtime;

import org.apache.iotdb.consensus.common.research.ResearchIo;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;
import org.apache.iotdb.consensus.traft.paper.core.TRaftCodec;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableStore;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.PayloadSource;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32;

/**
 * Append-only protocol journal. Incomplete tails are discarded; checksum corruption fails closed.
 * The bytes of compacted partitions stay on disk; a COMPACT record only drops their payload
 * locations from the in-memory index, so that index stays bounded by the uncompacted suffix.
 */
public final class TRaftFileStore implements DurableStore, Closeable {
  private static final int MAGIC = 0x54525031;
  private static final int MAX_RECORD = 64 << 20;
  private static final byte[] EMPTY_PAYLOAD = new byte[0];
  private static final String[] RECORD_COUNTERS = recordCounters();
  // Held counters for the per-record and per-read paths: no key lookup per update. Getting them
  // here also lists every key from the start, as the former zero additions did.
  private static final LongAdder[] RECORD_ADDERS = recordAdders();
  private static final LongAdder RECORDS = ResearchMetrics.counter("traft.log_record_count");
  private static final LongAdder FRAMES = ResearchMetrics.counter("traft.log_frame_count");
  private static final LongAdder PAYLOAD_READS =
      ResearchMetrics.counter("traft.payload_read_requests");
  private static final LongAdder PAYLOAD_HITS = ResearchMetrics.counter("traft.payload_cache_hits");
  private static final LongAdder PAYLOAD_BYTES =
      ResearchMetrics.counter("traft.payload_read_bytes_returned");
  // Leave room for the request envelope and journal/proposal metadata.
  static final int MAX_CLIENT_PAYLOAD = MAX_RECORD - (64 << 10);
  private final FileChannel channel;
  private long forcedRecords;
  private long writtenBytes;
  private long forceNanos;
  private final long cacheLimit;
  private final Map<EntryId, EntryLocation> entryRecords = new HashMap<>();
  // The first RETAIN_PROPOSAL record of each proposal, by its identity, pruned like entryRecords,
  // unless it shares its first entry's first frame (EntryLocation.retains).
  private final Map<EntryId, EntryLocation> retainRecords = new HashMap<>();
  // Resident entries load through these, by journal offset.
  private final PayloadSource payloads = (id, offset) -> loadPayload(id, offset, false);
  private final PayloadSource emptyPayloads = (id, offset) -> loadPayload(id, offset, true);
  private final LinkedHashMap<EntryId, byte[]> cache = new LinkedHashMap<>(256, .75f, true);
  private long cacheBytes;
  private long payloadLoads;
  // Journal end after the last write, and the end the last force covered. Bytes between them
  // (a deferred marker, or any record of an asynchronous or pipelined journal) are not yet durable.
  private long writtenEnd;
  private long forcedEnd;
  // Negative: every append is forced before it returns. Otherwise see setAsyncForceInterval.
  private long asyncForceIntervalNanos = -1;
  private long lastForceNanoTime = System.nanoTime();
  // See setPipelined.
  private boolean pipelined;
  // Buffered forcing (setForcePolicy): records carrying entry data written so far, the journal end
  // and data record count the last force covers or is covering, and when the first byte after that
  // end was written. Zero interval: no buffering.
  private long writtenDataRecords;
  private long targetedEnd;
  private long targetedDataRecords;
  private long firstUntargetedWriteNanos;
  private long forceIntervalNanos;
  private long forceDataRecords = 1;
  private boolean forceRequested;
  private boolean closed;

  private static LongAdder[] recordAdders() {
    LongAdder[] adders = new LongAdder[RECORD_COUNTERS.length];
    for (int i = 0; i < adders.length; i++) {
      adders[i] = ResearchMetrics.counter(RECORD_COUNTERS[i]);
    }
    return adders;
  }

  private static String[] recordCounters() {
    RecordType[] types = RecordType.values();
    String[] counters = new String[types.length];
    for (RecordType type : types) {
      counters[type.ordinal()] =
          "traft.log_record_" + type.name().toLowerCase(java.util.Locale.ROOT) + "_count";
    }
    return counters;
  }

  /** Immutable metadata validates repeated identities without rereading their payloads. */
  private static final class EntryLocation {
    final long offset;
    final long partition;
    final long timestamp;
    final int prevSize;
    final int payloadLength;
    final Kind kind;
    // This frame also holds the first RETAIN_PROPOSAL record of the proposal the entry opens.
    boolean retains;

    EntryLocation(long offset, Entry entry) {
      this.offset = offset;
      this.partition = entry.partition;
      this.timestamp = entry.timestamp;
      this.prevSize = entry.prevSize;
      this.payloadLength = entry.payloadSize();
      this.kind = entry.kind;
    }

    /** Entry.equals for an entry of the same identity. */
    boolean matches(Entry entry) {
      return partition == entry.partition
          && timestamp == entry.timestamp
          && prevSize == entry.prevSize
          && payloadLength == entry.payloadSize()
          && kind == entry.kind;
    }
  }

  public TRaftFileStore(File directory) throws IOException {
    this(directory, 32L << 20);
  }

  public TRaftFileStore(File directory, long cacheLimit) throws IOException {
    if (cacheLimit < 0) {
      throw new IllegalArgumentException("Negative payload cache budget");
    }
    this.cacheLimit = cacheLimit;
    AtomicFiles.createDirectories(directory);
    File file = new File(directory, "paper-journal-v1");
    boolean created = !file.exists();
    channel =
        FileChannel.open(
            file.toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
    if (created) {
      ResearchIo.force(channel, true, ResearchIo.TRAFT_LOG);
      AtomicFiles.forceDirectory(directory);
    }
  }

  /**
   * Validates every frame, truncates an incomplete tail and indexes payload locations, pruning them
   * at each COMPACT record. The returned list keeps no per-record state: its iterator streams the
   * journal again and re-indexes each record before returning it, so a replaying engine can still
   * make resident handles of entries that a later COMPACT record prunes; get() reads through an
   * offset array built on first use.
   */
  @Override
  public synchronized List<DurableRecord> load() throws IOException {
    int count = 0;
    entryRecords.clear();
    retainRecords.clear();
    cache.clear();
    cacheBytes = 0;
    long size = channel.size();
    long position = 0;
    while (position < size) {
      if (size - position < 12) {
        truncateTail(position);
        break;
      }
      ByteBuffer header = ByteBuffer.allocate(12);
      readFully(header, position);
      header.flip();
      int magic = header.getInt();
      int length = header.getInt();
      int expectedCrc = header.getInt();
      if (magic != MAGIC || length < 0 || length > MAX_RECORD) {
        throw new IOException("Invalid TRaft journal frame at " + position);
      }
      if (size - position - 12 < length) {
        truncateTail(position);
        break;
      }
      ByteBuffer payload = ByteBuffer.allocate(length);
      readFully(payload, position + 12);
      CRC32 crc = new CRC32();
      crc.update(payload.array());
      if ((int) crc.getValue() != expectedCrc) {
        throw new IOException("TRaft journal checksum mismatch at " + position);
      }
      DurableRecord record = TRaftCodec.decodeRecord(payload.array());
      if (packed(record)) {
        index(entryHalf(record), position);
        index(retainHalf(record), position);
        count += 2;
      } else {
        index(record, position);
        count++;
      }
      position += 12L + length;
    }
    channel.position(channel.size());
    writtenEnd = forcedEnd = targetedEnd = channel.size();
    return new LoadedJournal(count);
  }

  /** The records of one load(), read back from the journal on demand. */
  private final class LoadedJournal extends AbstractList<DurableRecord> {
    private final int count;
    // Frame offset shifted left by one; the low bit selects the RETAIN half of a packed frame.
    private long[] offsets;

    LoadedJournal(int count) {
      this.count = count;
    }

    @Override
    public int size() {
      return count;
    }

    @Override
    public DurableRecord get(int index) {
      if (index < 0 || index >= count) {
        throw new IndexOutOfBoundsException("record " + index + " of " + count);
      }
      try {
        long located = offsets()[index];
        DurableRecord record = readRecord(located >>> 1);
        if (!packed(record)) {
          return record;
        }
        return (located & 1) == 0 ? entryHalf(record) : retainHalf(record);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private long[] offsets() throws IOException {
      synchronized (TRaftFileStore.this) {
        if (offsets == null) {
          long[] built = new long[count];
          long position = 0;
          for (int i = 0; i < count; i++) {
            built[i] = position << 1;
            if (packedFrame(position)) {
              built[++i] = position << 1 | 1;
            }
            position = nextFrame(position);
          }
          offsets = built;
        }
        return offsets;
      }
    }

    @Override
    public Iterator<DurableRecord> iterator() {
      return new Iterator<DurableRecord>() {
        private long position;
        private int next;
        // The RETAIN half of the packed frame whose ENTRY half was returned last.
        private DurableRecord retainHalf;

        @Override
        public boolean hasNext() {
          return next < count;
        }

        @Override
        public DurableRecord next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          try {
            synchronized (TRaftFileStore.this) {
              next++;
              if (retainHalf != null) {
                DurableRecord record = retainHalf;
                retainHalf = null;
                return record;
              }
              Frame frame = readFrame(position);
              // Indexing in step with the reader keeps every location the reader can still ask
              // for; COMPACT records prune again exactly where they did during validation.
              DurableRecord record = frame.record;
              if (packed(record)) {
                retainHalf = retainHalf(record);
                record = entryHalf(record);
                index(record, position);
                index(retainHalf, position);
              } else {
                index(record, position);
              }
              position = frame.next;
              return record;
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      };
    }
  }

  private void readFully(ByteBuffer target, long position) throws IOException {
    while (target.hasRemaining()) {
      int n = ResearchIo.read(channel, target, position, ResearchIo.TRAFT_LOG);
      if (n < 0) {
        throw new IOException("Unexpected journal EOF");
      }
      position += n;
    }
  }

  private void truncateTail(long position) throws IOException {
    channel.truncate(position);
    ResearchIo.force(channel, true, ResearchIo.TRAFT_LOG);
  }

  @Override
  public synchronized void append(DurableRecord record) throws IOException {
    rejectPacked(record);
    verifyEntry(record.entry);
    if (record.proposal != null) {
      verifyEntry(record.proposal.first);
    }
    long offset = channel.size();
    int length = writeRecord(record, offset);
    FRAMES.increment();
    noteWritten(offset + length, dataRecords(record));
    if (forcedOnAppend(record)) {
      force();
    }
    publishRecord(record, offset, length);
  }

  /**
   * Written in journal order but forced only by the next forced append or by close. The next force
   * covers every byte written before it, so a later forced record can never be durable without this
   * one.
   */
  @Override
  public synchronized void appendDeferred(DurableRecord record) throws IOException {
    rejectPacked(record);
    verifyEntry(record.entry);
    if (record.proposal != null) {
      verifyEntry(record.proposal.first);
    }
    long offset = channel.size();
    int length = writeRecord(record, offset);
    FRAMES.increment();
    noteWritten(offset + length, dataRecords(record));
    publishRecord(record, offset, length);
  }

  @Override
  public synchronized void appendBatch(List<DurableRecord> records) throws IOException {
    if (records.isEmpty()) {
      return;
    }
    if (records.size() == 1) {
      append(records.get(0));
      return;
    }
    List<DurableRecord> batch = new ArrayList<>(records);
    // Sized for the batch: growing it record by record rehashes it several times per batch.
    Map<EntryId, Entry> staged = new HashMap<>(Math.max(16, batch.size() * 2));
    List<byte[]> encoded = new ArrayList<>(batch.size());
    // The frame of each record; the two records of a packed pair share one.
    int[] frameOf = new int[batch.size()];
    boolean forced = false;
    for (int i = 0; i < batch.size(); i++) {
      DurableRecord record = batch.get(i);
      rejectPacked(record);
      forced |= forcedOnAppend(record);
      verifyBatchEntry(record.entry, staged);
      if (record.proposal != null) {
        verifyBatchEntry(record.proposal.first, staged);
      }
      frameOf[i] = encoded.size();
      if (i + 1 < batch.size() && packable(record, batch.get(i + 1))) {
        DurableRecord retain = batch.get(++i);
        forced |= forcedOnAppend(retain);
        verifyBatchEntry(retain.proposal.first, staged);
        frameOf[i] = frameOf[i - 1];
        encoded.add(encodeRecord(pack(record, retain)));
      } else {
        encoded.add(encodeRecord(record));
      }
    }
    long[] offsets = new long[encoded.size()];
    int[] lengths = new int[encoded.size()];
    long start = channel.size();
    long total = 0;
    for (byte[] payload : encoded) {
      total += 12L + payload.length;
    }
    if (total > Integer.MAX_VALUE) {
      throw new IOException("TRaft journal batch too large");
    }
    // One write call carries the whole batch; the frames on disk are those of one call per record.
    ByteBuffer frames = ByteBuffer.allocate((int) total);
    for (int i = 0; i < encoded.size(); i++) {
      offsets[i] = start + frames.position();
      lengths[i] = putFrame(frames, encoded.get(i));
    }
    frames.flip();
    channel.position(start);
    while (frames.hasRemaining()) {
      ResearchIo.write(channel, frames, ResearchIo.TRAFT_LOG);
    }
    long data = 0;
    for (DurableRecord record : batch) {
      data += dataRecords(record);
    }
    noteWritten(start + total, data);
    // No resident handle or caller ACK can observe the batch before this barrier.
    // A failed call may leave recoverable old-format frames; its engine is fenced.
    if (forced) {
      force();
    }
    FRAMES.add(encoded.size());
    for (int i = 0; i < batch.size(); i++) {
      int frame = frameOf[i];
      boolean opens = i == 0 || frameOf[i - 1] != frame;
      publishRecord(batch.get(i), offsets[frame], opens ? lengths[frame] : 0);
    }
  }

  /**
   * A boundary's ENTRY record and the RETAIN_PROPOSAL record right after it in one batch share a
   * frame: an ENTRY record that carries the proposal, whose first is that entry (TRaftCodec then
   * writes the proposal in a byte or two). The pair becomes durable together; separate frames could
   * also leave only the entry, which recovery treats as staged data. Readers get the two records
   * back (entryHalf, retainHalf).
   */
  private static boolean packable(DurableRecord entry, DurableRecord retain) {
    return entry.type == RecordType.ENTRY
        && entry.entry.hasPayload()
        && entry.reference == null
        && entry.identities.isEmpty()
        && retain.type == RecordType.RETAIN_PROPOSAL
        && retain.entry == null
        && retain.reference == null
        && retain.identities.isEmpty()
        && retain.term == entry.term
        && retain.vote == entry.vote
        && retain.frontier == entry.frontier
        && !retain.proposal.first.hasPayload()
        && retain.proposal.first.equals(entry.entry);
  }

  private static DurableRecord pack(DurableRecord entry, DurableRecord retain) {
    return new DurableRecord(
        RecordType.ENTRY,
        entry.term,
        entry.vote,
        entry.entry,
        retain.proposal,
        null,
        entry.frontier,
        null);
  }

  private static boolean packed(DurableRecord record) {
    return record.type == RecordType.ENTRY && record.proposal != null;
  }

  private static DurableRecord entryHalf(DurableRecord packed) {
    return new DurableRecord(
        RecordType.ENTRY,
        packed.term,
        packed.vote,
        packed.entry,
        null,
        null,
        packed.frontier,
        null);
  }

  private static DurableRecord retainHalf(DurableRecord packed) {
    return new DurableRecord(
        RecordType.RETAIN_PROPOSAL,
        packed.term,
        packed.vote,
        null,
        packed.proposal,
        null,
        packed.frontier,
        null);
  }

  private static void rejectPacked(DurableRecord record) throws IOException {
    if (packed(record)) {
      throw new IOException("An ENTRY record carries no proposal");
    }
  }

  private void verifyBatchEntry(Entry entry, Map<EntryId, Entry> staged) throws IOException {
    if (entry == null) {
      return;
    }
    EntryLocation durable = entryRecords.get(entry.id);
    Entry previous = staged.get(entry.id);
    if (durable == null && previous == null) {
      if (!entry.hasPayload()) {
        throw new IOException("Metadata precedes its payload in durable batch: " + entry.id);
      }
      // Compared by metadata only (Entry.equals), so the entry itself serves.
      staged.put(entry.id, entry);
    } else if (durable != null ? !durable.matches(entry) : !previous.equals(entry)) {
      throw new IOException("Durable entry identity changed in batch: " + entry.id);
    }
  }

  private static long dataRecords(DurableRecord record) {
    return record.entry != null && record.entry.hasPayload() ? 1 : 0;
  }

  private void noteWritten(long end, long data) {
    if (writtenEnd <= targetedEnd) {
      firstUntargetedWriteNanos = System.nanoTime();
    }
    writtenEnd = end;
    writtenDataRecords += data;
    if (forceIntervalNanos > 0 && writtenDataRecords - targetedDataRecords >= forceDataRecords) {
      notifyAll();
    }
  }

  private int writeRecord(DurableRecord record, long offset) throws IOException {
    return writeEncodedRecord(encodeRecord(record), offset);
  }

  private static byte[] encodeRecord(DurableRecord record) throws IOException {
    byte[] payload = TRaftCodec.encodeRecord(record);
    if (payload.length > MAX_RECORD) {
      throw new IOException("TRaft journal record too large");
    }
    return payload;
  }

  private int writeEncodedRecord(byte[] payload, long offset) throws IOException {
    ByteBuffer frame = ByteBuffer.allocate(12 + payload.length);
    int length = putFrame(frame, payload);
    frame.flip();
    channel.position(offset);
    while (frame.hasRemaining()) {
      ResearchIo.write(channel, frame, ResearchIo.TRAFT_LOG);
    }
    return length;
  }

  private static int putFrame(ByteBuffer target, byte[] payload) {
    CRC32 crc = new CRC32();
    crc.update(payload);
    target.putInt(MAGIC).putInt(payload.length).putInt((int) crc.getValue()).put(payload);
    return 12 + payload.length;
  }

  private void publishRecord(DurableRecord record, long offset, int length) throws IOException {
    forcedRecords++;
    RECORDS.increment();
    RECORD_ADDERS[record.type.ordinal()].increment();
    writtenBytes += length;
    index(record, offset);
    cacheEntry(record.entry);
    if (record.proposal != null) {
      cacheEntry(record.proposal.first);
    }
  }

  private void cacheEntry(Entry entry) {
    // Payload arrays are immutable: the cache shares the entry's own bytes.
    if (entry != null
        && entry.hasPayload()
        && entry.payloadSize() > 0
        && entry.payloadSize() <= cacheLimit
        && cache.get(entry.id) == null) {
      cache(entry.id, entry.payloadView());
    }
  }

  private void index(DurableRecord record, long offset) throws IOException {
    verifyEntry(record.entry);
    if (record.proposal != null) {
      verifyEntry(record.proposal.first);
    }
    if (record.entry != null) {
      indexEntry(record.entry, offset);
    }
    if (record.proposal != null) {
      indexEntry(record.proposal.first, offset);
    }
    if (record.type == RecordType.RETAIN_PROPOSAL && record.proposal != null) {
      EntryId first = record.proposal.first.id;
      EntryLocation entry = entryRecords.get(first);
      if (entry != null && entry.offset == offset) {
        // A packed frame: the RETAIN half is durable with its entry.
        entry.retains = true;
      } else if ((entry == null || !entry.retains) && !retainRecords.containsKey(first)) {
        retainRecords.put(first, new EntryLocation(offset, record.proposal.first));
      }
    }
    if (record.type == RecordType.COMPACT) {
      prune(record.frontier);
    }
  }

  /**
   * Forget the payload locations of compacted partitions. Resident handles already made keep their
   * own offset; a later record naming a pruned identity as metadata fails closed in verifyEntry.
   */
  private void prune(long frontier) {
    Iterator<Map.Entry<EntryId, EntryLocation>> locations = entryRecords.entrySet().iterator();
    while (locations.hasNext()) {
      Map.Entry<EntryId, EntryLocation> location = locations.next();
      if (location.getValue().partition <= frontier) {
        locations.remove();
        byte[] cached = cache.remove(location.getKey());
        if (cached != null) {
          cacheBytes -= cached.length;
        }
      }
    }
    retainRecords.values().removeIf(location -> location.partition <= frontier);
  }

  /** Payload locations currently indexed; exposed for tests. */
  synchronized int indexedEntries() {
    return entryRecords.size();
  }

  private void indexEntry(Entry entry, long offset) {
    if (!entryRecords.containsKey(entry.id)) {
      entryRecords.put(entry.id, new EntryLocation(offset, entry));
    }
  }

  private void verifyEntry(Entry entry) throws IOException {
    if (entry == null) {
      return;
    }
    EntryLocation previous = entryRecords.get(entry.id);
    if (previous == null) {
      if (!entry.hasPayload()) {
        throw new IOException("Metadata references an entry without durable payload: " + entry.id);
      }
      return;
    }
    if (!previous.matches(entry)) {
      throw new IOException("Durable entry identity changed: " + entry.id);
    }
  }

  private static final class Frame {
    final DurableRecord record;
    final long next;

    Frame(DurableRecord record, long next) {
      this.record = record;
      this.next = next;
    }
  }

  private synchronized DurableRecord readRecord(long offset) throws IOException {
    return readFrame(offset).record;
  }

  /** Whether a frame that load() has already validated is packed; reads only its first bytes. */
  private synchronized boolean packedFrame(long offset) throws IOException {
    ByteBuffer head = ByteBuffer.allocate(15);
    readFully(head, offset);
    return TRaftCodec.isEntryRecordWithProposal(Arrays.copyOfRange(head.array(), 12, 15));
  }

  /** The position after a frame that load() has already validated; reads only its header. */
  private synchronized long nextFrame(long offset) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(12);
    readFully(header, offset);
    header.flip();
    header.getInt();
    return offset + 12L + header.getInt();
  }

  private synchronized Frame readFrame(long offset) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(12);
    readFully(header, offset);
    header.flip();
    if (header.getInt() != MAGIC) {
      throw new IOException("Invalid journal magic");
    }
    int length = header.getInt();
    int checksum = header.getInt();
    if (length < 0 || length > MAX_RECORD) {
      throw new IOException("Invalid journal length");
    }
    ByteBuffer body = ByteBuffer.allocate(length);
    readFully(body, offset + 12);
    CRC32 crc = new CRC32();
    crc.update(body.array());
    if ((int) crc.getValue() != checksum) {
      throw new IOException("Journal checksum mismatch");
    }
    return new Frame(TRaftCodec.decodeRecord(body.array()), offset + 12L + length);
  }

  /**
   * The handle captures its own journal offset: a COMPACT record may prune the identity from the
   * index while an in-flight message or a restart application still holds the handle.
   */
  @Override
  public synchronized Entry residentEntry(Entry entry) {
    EntryLocation location = entryRecords.get(entry.id);
    if (location == null) {
      throw new IllegalStateException("Entry is not durable: " + entry.id);
    }
    return Entry.resident(
        entry, location.payloadLength == 0 ? emptyPayloads : payloads, location.offset);
  }

  private byte[] loadPayload(EntryId id, long offset, boolean empty) {
    try {
      return readPayload(id, offset, empty);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private synchronized byte[] readPayload(EntryId id, long offset, boolean empty)
      throws IOException {
    PAYLOAD_READS.increment();
    byte[] bytes = cache.get(id);
    if (bytes != null) {
      PAYLOAD_HITS.increment();
      PAYLOAD_BYTES.add(bytes.length);
      return bytes;
    }
    if (empty) {
      // Every empty immutable payload is identical. Share one cached value without
      // retaining one zero-byte cache-map entry per historical no-op.
      PAYLOAD_HITS.increment();
      return EMPTY_PAYLOAD;
    }
    DurableRecord record = readRecord(offset);
    Entry entry =
        record.entry != null && record.entry.id.equals(id)
            ? record.entry
            : record.proposal != null && record.proposal.first.id.equals(id)
                ? record.proposal.first
                : null;
    if (entry == null) {
      throw new IOException("Entry location mismatch " + id);
    }
    bytes = entry.payload();
    payloadLoads++;
    cache(id, bytes);
    PAYLOAD_BYTES.add(bytes.length);
    return bytes;
  }

  private void cache(EntryId id, byte[] bytes) {
    byte[] old = cache.remove(id);
    if (old != null) {
      cacheBytes -= old.length;
    }
    if (bytes.length == 0 || bytes.length > cacheLimit) {
      return;
    }
    cache.put(id, bytes);
    cacheBytes += bytes.length;
    while (cacheBytes > cacheLimit && !cache.isEmpty()) {
      Map.Entry<EntryId, byte[]> eldest = cache.entrySet().iterator().next();
      cacheBytes -= eldest.getValue().length;
      cache.remove(eldest.getKey());
    }
  }

  public synchronized long payloadLoads() {
    return payloadLoads;
  }

  public synchronized long cacheBytes() {
    return cacheBytes;
  }

  public synchronized long forcedRecords() {
    return forcedRecords;
  }

  public synchronized long writtenBytes() {
    return writtenBytes;
  }

  public synchronized long forceNanos() {
    return forceNanos;
  }

  /** Written bytes no force has covered yet; exposed for tests. */
  public synchronized boolean hasPendingForce() {
    return writtenEnd > forcedEnd;
  }

  /**
   * Forces a pending deferred marker now. The runtime calls this from its ticker so an unforced
   * marker never sits at the journal tail longer than one tick when the group is idle; under load
   * the next forced append has usually covered it already and this is a no-op. With asynchronous
   * durability it forces only once the interval has passed since the last force.
   */
  public synchronized void forcePending() throws IOException {
    if (writtenEnd > forcedEnd
        && (asyncForceIntervalNanos < 0
            || System.nanoTime() - lastForceNanoTime >= asyncForceIntervalNanos)) {
      force();
    }
  }

  /**
   * Asynchronous durability, IoTDB's wal_mode=ASYNC applied to this journal: every append except a
   * TERM_VOTE record returns once its bytes are written to the operating system, and forcePending
   * forces them at most {@code intervalMillis} later. This deliberately breaks the DurableStore
   * contract: an acknowledgment survives a process crash, but losing the page cache of a majority
   * (power loss, kernel crash) can lose acknowledged entries and leave replicas divergent. Votes
   * stay forced, so a restarted node never votes twice in one term. Negative restores the default,
   * in which every append is forced before it returns.
   */
  public synchronized void setAsyncForceInterval(long intervalMillis) {
    asyncForceIntervalNanos =
        intervalMillis < 0 ? -1 : TimeUnit.MILLISECONDS.toNanos(intervalMillis);
  }

  /** Forces every written byte now, whatever the durability mode. */
  public synchronized void forceNow() throws IOException {
    if (writtenEnd > forcedEnd) {
      force();
    }
  }

  /**
   * Pipelined durability, for the default synchronous mode: appends other than TERM_VOTE records
   * only write, and the runtime forces them with {@link #forceWritten} on a separate thread. The
   * runtime then holds every frame and reply produced after a write until a force covers that
   * write, so no other node observes an acknowledgment before its record is durable. This keeps the
   * DurableStore contract as other nodes see it while the protocol thread never waits for the disk.
   */
  public synchronized void setPipelined(boolean enabled) {
    pipelined = enabled;
  }

  /**
   * Buffered forcing for the pipelined journal: {@link #awaitForceDue} lets a force start only once
   * the bytes no force covers yet hold {@code dataRecords} records carrying entry data, or the
   * first of those bytes was written {@code intervalMillis} ago. Every acknowledgment still waits
   * for its force, so this trades latency for fewer and larger forces; durability is unchanged.
   * Zero interval forces as soon as asked.
   */
  public synchronized void setForcePolicy(long intervalMillis, long dataRecords) {
    if (intervalMillis < 0 || dataRecords < 1) {
      throw new IllegalArgumentException("force interval >= 0 and data records >= 1 required");
    }
    forceIntervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
    forceDataRecords = dataRecords;
    notifyAll();
  }

  /**
   * Makes the bytes written so far due at once (see awaitForceDue): something other than a data
   * acknowledgment, such as a vote, a recovery report or a proposal acknowledgment, waits on them.
   */
  public synchronized void requestForce() {
    forceRequested = true;
    notifyAll();
  }

  /**
   * Blocks until the bytes no force covers yet are due under {@link #setForcePolicy}, or a force is
   * requested, or the store closes. Returns at once without a policy or when every written byte is
   * already targeted.
   */
  public synchronized void awaitForceDue() throws InterruptedException {
    while (!closed && forceIntervalNanos > 0 && writtenEnd > targetedEnd) {
      if (forceRequested) {
        ResearchMetrics.add("traft.force_due_requested", 1);
        return;
      }
      if (writtenDataRecords - targetedDataRecords >= forceDataRecords) {
        ResearchMetrics.add("traft.force_due_entries", 1);
        return;
      }
      long remaining = firstUntargetedWriteNanos + forceIntervalNanos - System.nanoTime();
      if (remaining <= 0) {
        ResearchMetrics.add("traft.force_due_interval", 1);
        return;
      }
      long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
      wait(millis, millis == 0 ? (int) remaining : 0);
    }
  }

  /**
   * The smallest forced journal end that makes this journal's first record holding {@code id}'s
   * payload durable, or -1 when no such record is indexed. Forces always end at a record boundary,
   * so a forced end past the record's offset covers the whole record.
   */
  public synchronized long payloadEnd(EntryId id) {
    EntryLocation location = entryRecords.get(id);
    return location == null ? -1 : location.offset + 1;
  }

  /**
   * The smallest forced journal end that makes the first RETAIN_PROPOSAL record of proposal {@code
   * id} durable, or -1 when none is indexed (see payloadEnd).
   */
  public synchronized long retainEnd(EntryId id) {
    EntryLocation entry = entryRecords.get(id);
    if (entry != null && entry.retains) {
      return entry.offset + 1;
    }
    EntryLocation location = retainRecords.get(id);
    return location == null ? -1 : location.offset + 1;
  }

  /** Journal end after the last write; a force covering it makes every written record durable. */
  public synchronized long writtenEnd() {
    return writtenEnd;
  }

  /** Journal end covered by the last completed force. */
  public synchronized long forcedEnd() {
    return forcedEnd;
  }

  /**
   * Forces every byte written so far without holding this store's lock, so appends continue while
   * the disk works, and returns the journal end now durable. Appends that complete during the call
   * may or may not be covered; a later call covers them.
   */
  public long forceWritten() throws IOException {
    long target;
    synchronized (this) {
      target = writtenEnd;
      if (target <= forcedEnd) {
        return forcedEnd;
      }
      targetedEnd = target;
      targetedDataRecords = writtenDataRecords;
      forceRequested = false;
    }
    long nanos = ResearchIo.force(channel, false, ResearchIo.TRAFT_LOG);
    synchronized (this) {
      forceNanos += nanos;
      lastForceNanoTime = System.nanoTime();
      forcedEnd = Math.max(forcedEnd, target);
      return forcedEnd;
    }
  }

  private boolean forcedOnAppend(DurableRecord record) {
    return record.type == RecordType.TERM_VOTE || (asyncForceIntervalNanos < 0 && !pipelined);
  }

  private void force() throws IOException {
    long target = writtenEnd;
    targetedEnd = target;
    targetedDataRecords = writtenDataRecords;
    forceRequested = false;
    forceNanos += ResearchIo.force(channel, false, ResearchIo.TRAFT_LOG);
    forcedEnd = Math.max(forcedEnd, target);
    lastForceNanoTime = System.nanoTime();
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
    notifyAll();
    try {
      if (writtenEnd > forcedEnd && channel.isOpen()) {
        force();
      }
    } finally {
      channel.close();
    }
  }
}
